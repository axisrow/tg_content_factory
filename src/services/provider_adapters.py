from __future__ import annotations

import asyncio
import base64
import binascii
import functools
import importlib.util
import logging
import os
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, Optional

import aiohttp

logger = logging.getLogger(__name__)

# Type alias for image generation adapters
ImageAdapter = Callable[[str, str], Awaitable[Optional[str]]]

# Default directory where binary / base64 image results are persisted.
# Must match where the web app serves generated images from — ``DATA_IMAGE_DIR``
# (``data/image``) mounted at ``/data/image`` in src/web/assembly.py. Writing
# elsewhere (the old ``data/images``) made saved files unreachable over HTTP, so
# gpt-image-1 / HuggingFace results rendered as broken images in a no-S3 deploy.
DEFAULT_IMAGE_OUTPUT_DIR = "data/image"


def _image_target_path(output_dir: str | None = None) -> Path:
    """Return a fresh ``<output_dir>/<uuid>.png`` path, creating the dir.

    Single source of the on-disk naming convention, shared by every image
    adapter — both those that already hold the bytes (:func:`save_image_bytes`)
    and the Codex adapter, which must hand the path to the engine *before* the
    file exists. ``output_dir`` defaults to :data:`DEFAULT_IMAGE_OUTPUT_DIR`,
    resolved at call time so the module-level default stays overridable.
    """
    out = Path(output_dir or DEFAULT_IMAGE_OUTPUT_DIR)
    out.mkdir(parents=True, exist_ok=True)
    return out / f"{uuid.uuid4().hex}.png"


async def save_image_bytes(image_bytes: bytes, output_dir: str | None = None) -> str:
    """Persist raw image *bytes* to *output_dir* and return the file path.

    Single place every image adapter uses to land binary results on disk, so
    naming/flush behaviour stays consistent.  Downstream (``ImageGenerationService``)
    uploads non-URL results to S3 when configured, so returning a path is enough.
    """
    filepath = _image_target_path(output_dir)
    await asyncio.to_thread(filepath.write_bytes, image_bytes)
    return str(filepath)


async def save_image_b64(b64_data: str, output_dir: str | None = None) -> str:
    """Decode a base64 image payload and persist it via :func:`save_image_bytes`."""
    try:
        image_bytes = base64.b64decode(b64_data, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise RuntimeError(f"image: invalid base64 payload: {exc}") from exc
    return await save_image_bytes(image_bytes, output_dir)


async def finalize_image_result(
    item: dict[str, Any], output_dir: str | None = None
) -> Optional[str]:
    """Normalise one provider result entry to a URL or a saved file path.

    Providers return either a hosted ``url`` (kept as-is) or an inline
    ``b64_json`` payload (decoded and saved to disk).  Centralising this keeps
    every adapter's output shape identical.
    """
    url = item.get("url")
    if url:
        return str(url)
    b64_data = item.get("b64_json")
    if b64_data:
        return await save_image_b64(str(b64_data), output_dir)
    return None

# Default network timeout for outbound HTTP calls to LLM / image providers.
# Without it aiohttp waits indefinitely and a stalled upstream hangs the calling
# coroutine forever (#633 bug #10).
_HTTP_TIMEOUT = aiohttp.ClientTimeout(total=120)


async def _parse_json_for_text(data: Any) -> str:
    # Try common response shapes
    if data is None:
        return ""
    if isinstance(data, str):
        return data
    if isinstance(data, dict):
        # OpenAI style
        if "choices" in data:
            try:
                c = data["choices"][0]
                if isinstance(c, dict):
                    if "message" in c:
                        # `content` can be JSON null (tool-call/refusal/filter);
                        # dict.get(default) keeps the stored None, breaking the
                        # -> str contract downstream (audit #836/8). Coerce to "".
                        return c["message"].get("content") or ""
                    return c.get("text") or ""
            except Exception:
                pass
        # Cohere style
        if "generations" in data:
            try:
                return data["generations"][0].get("text", "")
            except Exception:
                pass
        # HuggingFace / other style
        if "generated_text" in data:
            return data.get("generated_text", "")
        if "outputs" in data:
            try:
                out = data["outputs"][0]
                if isinstance(out, dict):
                    # values can be JSON null; coerce to "" to keep the -> str contract
                    return out.get("content") or out.get("text") or ""
                return str(out)
            except Exception:
                pass
        if "result" in data:
            r = data["result"]
            if isinstance(r, str):
                return r
            if isinstance(r, dict):
                for k in ("text", "content", "generated_text"):
                    if k in r:
                        v = r[k]
                        # the matched value may be JSON null or a nested object;
                        # keep the -> str contract
                        return v if isinstance(v, str) else ("" if v is None else str(v))
                return str(r)
        # Ollama / local shapes
        if "results" in data:
            try:
                r0 = data["results"][0]
                if isinstance(r0, dict):
                    # nested content
                    if "content" in r0 and isinstance(r0["content"], dict):
                        return r0["content"].get("text", "")
                    return r0.get("text", "")
            except Exception:
                pass
        # fallback: try first stringy field
        for v in data.values():
            if isinstance(v, str):
                return v
        return str(data)
    if isinstance(data, list):
        # list of items
        try:
            first = data[0]
            return await _parse_json_for_text(first)
        except Exception:
            return str(data)
    return str(data)


def make_generic_http_adapter(
    base_url: str, api_key: Optional[str] = None, api_key_header: str = "Authorization"
) -> Callable[..., Awaitable[str]]:
    endpoint = base_url

    async def provider(
        prompt: str = "",
        model: Optional[str] = None,
        max_tokens: int = 256,
        temperature: float = 0.0,
        stream: bool = False,
        **kwargs: Any,
    ) -> str:
        payload: Dict[str, Any] = {"prompt": prompt}
        if model:
            payload["model"] = model
        headers = {"Content-Type": "application/json"}
        if api_key:
            headers[api_key_header] = f"Bearer {api_key}"
        try:
            async with aiohttp.ClientSession(timeout=_HTTP_TIMEOUT) as session:
                async with session.post(endpoint, json=payload, headers=headers) as resp:
                    if resp.status != 200:
                        text = await resp.text()
                        raise RuntimeError(f"Provider error {resp.status}: {text}")
                    data = await resp.json()
                    return await _parse_json_for_text(data)
        except Exception:
            raise

    return provider


def make_context7_adapter(
    api_key: str, base_url: Optional[str] = None
) -> Callable[..., Awaitable[str]]:
    base = base_url or os.environ.get("CONTEXT7_API_BASE", "https://api.context7.com/v1/generate")
    return make_generic_http_adapter(base, api_key)


# ── Image generation adapters ──────────────────────────────────────────


def make_together_image_adapter(api_key: str) -> ImageAdapter:
    """Together AI image generation via FLUX models."""

    async def adapter(prompt: str, model: str = "") -> Optional[str]:
        url = "https://api.together.xyz/v1/images/generations"
        headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
        payload: Dict[str, Any] = {
            "model": model or "black-forest-labs/FLUX.1-schnell",
            "prompt": prompt,
            "n": 1,
            "steps": 4,
        }
        async with aiohttp.ClientSession(timeout=_HTTP_TIMEOUT) as session:
            async with session.post(url, json=payload, headers=headers) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    raise RuntimeError(f"Together image error {resp.status}: {text}")
                data = await resp.json()
                items = data.get("data")
                if not items:
                    raise RuntimeError(f"Together image: empty 'data' in response: {data}")
                return await finalize_image_result(items[0])

    return adapter


def make_huggingface_image_adapter(
    api_token: str, output_dir: str = DEFAULT_IMAGE_OUTPUT_DIR
) -> ImageAdapter:
    """HuggingFace Inference API — returns binary image, saved to local file."""

    async def adapter(prompt: str, model: str = "") -> Optional[str]:
        default_model = "stabilityai/stable-diffusion-xl-base-1.0"
        if model and "/" not in model:
            logger.warning("HuggingFace: model %r lacks '/' separator, falling back to %s", model, default_model)
        model_id = model if model and "/" in model else default_model
        url = f"https://router.huggingface.co/hf-inference/models/{model_id}"
        headers = {"Authorization": f"Bearer {api_token}"}
        payload = {"inputs": prompt}
        async with aiohttp.ClientSession(timeout=_HTTP_TIMEOUT) as session:
            async with session.post(url, json=payload, headers=headers) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    raise RuntimeError(f"HuggingFace image error {resp.status}: {text}")
                content_type = resp.content_type or ""
                if not content_type.startswith("image/"):
                    body_preview = (await resp.text())[:200]
                    raise RuntimeError(
                        f"HuggingFace image: expected image/* content-type, got {content_type}: {body_preview}"
                    )
                image_bytes = await resp.read()
                return await save_image_bytes(image_bytes, output_dir)

    return adapter


OPENAI_DEFAULT_IMAGE_MODEL = "gpt-image-1"


def _build_openai_image_payload(prompt: str, model_id: str) -> Dict[str, Any]:
    """Assemble the images/generations payload, varying params by model family.

    ``gpt-image-1*`` rejects DALL·E-only fields (``response_format``), so only
    the parameters each family accepts are sent.  ``gpt-image-1`` always returns
    ``b64_json``; legacy ``dall-e-*`` returns a hosted ``url``.
    """
    payload: Dict[str, Any] = {"model": model_id, "prompt": prompt, "n": 1}
    if model_id.startswith("gpt-image"):
        payload["size"] = "auto"
        payload["quality"] = "auto"
    else:  # legacy dall-e-* family
        payload["size"] = "1024x1024"
    return payload


def make_openai_image_adapter(api_key: str) -> ImageAdapter:
    """OpenAI image generation (gpt-image-1, with legacy DALL-E support)."""

    async def adapter(prompt: str, model: str = "") -> Optional[str]:
        base = os.environ.get("OPENAI_API_BASE", "https://api.openai.com/v1")
        url = f"{base.rstrip('/')}/images/generations"
        headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
        payload = _build_openai_image_payload(prompt, model or OPENAI_DEFAULT_IMAGE_MODEL)
        async with aiohttp.ClientSession(timeout=_HTTP_TIMEOUT) as session:
            async with session.post(url, json=payload, headers=headers) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    raise RuntimeError(f"OpenAI image error {resp.status}: {text}")
                data = await resp.json()
                items = data.get("data")
                if not items:
                    raise RuntimeError(f"OpenAI image: empty 'data' in response: {data}")
                return await finalize_image_result(items[0])

    return adapter


def make_replicate_image_adapter(api_token: str, timeout: float = 60.0) -> ImageAdapter:
    """Replicate async prediction API with polling."""

    async def adapter(prompt: str, model: str = "") -> Optional[str]:
        default_model = "black-forest-labs/flux-schnell"
        if model and "/" not in model:
            logger.warning("Replicate: model %r lacks '/' separator, falling back to %s", model, default_model)
        model_id = model if model and "/" in model else default_model
        # Use the model route: POST /v1/models/{owner}/{name}/predictions
        url = f"https://api.replicate.com/v1/models/{model_id}/predictions"
        headers = {"Authorization": f"Bearer {api_token}", "Content-Type": "application/json"}
        payload: Dict[str, Any] = {
            "input": {"prompt": prompt},
        }
        async with aiohttp.ClientSession(timeout=_HTTP_TIMEOUT) as session:
            # Create prediction
            async with session.post(url, json=payload, headers=headers) as resp:
                if resp.status not in (200, 201):
                    text = await resp.text()
                    raise RuntimeError(f"Replicate create error {resp.status}: {text}")
                prediction = await resp.json()

            # Poll for completion
            poll_url = prediction.get("urls", {}).get("get")
            if not poll_url:
                raise RuntimeError(f"Replicate: missing poll URL in response: {prediction}")
            elapsed = 0.0
            while elapsed < timeout:
                await asyncio.sleep(1.0)
                elapsed += 1.0
                async with session.get(poll_url, headers=headers) as resp:
                    if resp.status != 200:
                        continue
                    result = await resp.json()
                    status = result.get("status")
                    if status == "succeeded":
                        output = result.get("output")
                        if isinstance(output, list) and output:
                            return output[0]
                        if isinstance(output, str):
                            return output
                        return None
                    if status in ("failed", "canceled"):
                        error = result.get("error", "unknown error")
                        raise RuntimeError(f"Replicate prediction failed: {error}")
            raise RuntimeError(f"Replicate prediction timed out after {timeout}s")

    return adapter


# ── Codex SDK image generation ──
#
# Unlike the HTTP providers above, Codex does not use an API key: the
# ``openai_codex`` SDK drives a local Codex engine that reuses the existing
# Codex CLI authentication (``~/.codex/auth.json``).  The image is produced by
# the Codex agent itself via the ``$imagegen`` tool, which writes a PNG to a
# path we hand it — so this adapter, like HuggingFace, returns a local file path.
CODEX_DEFAULT_IMAGE_MODEL = "gpt-5.4"
# Deadline for one Codex image turn. On timeout the adapter kills the Codex
# subprocess (Codex.close() → terminate()/kill()), which unblocks the stuck
# worker thread, so the executor slot is freed shortly after — not leaked for
# the process lifetime.
CODEX_IMAGE_TIMEOUT_SECONDS = 180.0
CODEX_IMAGE_CLOSE_TIMEOUT_SECONDS = 15.0


def _build_codex_image_prompt(prompt: str, output_path: str) -> str:
    """Build the ``$imagegen`` instruction that tells Codex to save a PNG.

    Kept as a pure function so the prompt shape is unit-testable without the SDK.
    """
    return (
        "$imagegen\n\n"
        "Generate one high-quality image from this prompt:\n\n"
        f"{prompt}\n\n"
        "Save the generated PNG to this exact local path:\n\n"
        f"{output_path}\n\n"
        "After generating it, reply briefly with what was created and the saved file path."
    )


def _codex_saved_path_from_result(result: Any) -> Optional[str]:
    """Extract the saved image path from a Codex ``TurnResult``.

    Walks ``result.items`` for an ``imageGeneration`` item (``saved_path``) or an
    ``imageView`` item (``path``).  Mirrors the result shape of openai_codex's
    generated thread-item models.  Returns ``None`` when no image item is present.
    """
    for wrapped in getattr(result, "items", None) or []:
        item = wrapped.root if hasattr(wrapped, "root") else wrapped
        item_type = getattr(item, "type", None)
        if item_type == "imageGeneration":
            saved = getattr(item, "saved_path", None)
            if saved:
                return str(saved)
        elif item_type == "imageView":
            path = getattr(item, "path", None)
            if path:
                return str(path)
    return None


def _codex_sdk_installed() -> bool:
    """True when the ``openai_codex`` SDK importable in this environment."""
    return importlib.util.find_spec("openai_codex") is not None


def _codex_home_path() -> Path:
    """Return the Codex runtime home used by the CLI/SDK."""
    configured = os.environ.get("CODEX_HOME", "").strip()
    if configured:
        return Path(configured).expanduser()
    return Path.home() / ".codex"


def _codex_path_writable(path: Path) -> bool:
    """True when *path* can be written by this process."""
    try:
        return os.access(path, os.W_OK)
    except OSError:
        return False


def _codex_runtime_state_writable(codex_home: Path) -> bool:
    """True when Codex can initialize/update runtime state under *codex_home*."""
    try:
        if not codex_home.is_dir() or not _codex_path_writable(codex_home):
            return False
        for path in codex_home.glob("state*.sqlite*"):
            if not _codex_path_writable(path):
                return False
    except OSError:
        return False
    return True


@functools.lru_cache(maxsize=1)
def codex_available() -> bool:
    """True when the Codex SDK is installed and its CLI runtime is usable.

    Single source of truth for "is the keyless codex provider usable", shared by
    both registration paths (``ImageGenerationService._register_from_env`` and
    ``ImageProviderService.build_adapters``). Cached because the inputs — SDK
    install state, ``$CODEX_HOME``/``~/.codex`` auth, and runtime-state
    writability — are static for the process lifetime, and the check otherwise
    runs import-machinery and filesystem probes on every image request.
    """
    if not _codex_sdk_installed():
        return False
    codex_home = _codex_home_path()
    return (codex_home / "auth.json").exists() and _codex_runtime_state_writable(codex_home)



# Dedicated thread pools for the Codex image path, kept OFF the default asyncio
# loop executor (the one `asyncio.to_thread` / `run_in_executor(None, ...)` use).
# Two separate pools, created once at import time (not per call):
#
#   * ``_CODEX_RUN_EXECUTOR`` runs the blocking ``thread.run`` turn. Keeping it
#     off the default pool means a hung Codex turn cannot starve the default
#     pool's slots, which S3 upload (s3_store.py), the debug log-tail
#     (routes/debug.py) and AI search also schedule onto.
#   * ``_CODEX_CLOSE_EXECUTOR`` runs only ``codex.close()``. It MUST be a
#     separate pool: when every ``_CODEX_RUN_EXECUTOR`` slot is occupied by hung
#     turns, the timeout handler still needs a free slot to submit close() —
#     and close() is what terminates the subprocess and unwinds the parked run
#     thread (the SDK reader thread fail_all()s the blocked queue.get()).
#     Submitting close() onto the same saturated run pool would queue it behind
#     the very hangs it is meant to clear, so the kill never happens.
#
# Lifecycle note: ThreadPoolExecutor worker threads are non-daemon, so a thread
# still parked inside a hung Codex turn at interpreter shutdown would block exit
# via the executor's atexit join. In practice the timeout handler calls close()
# which unblocks the parked run thread, draining the slot; the pools are
# intentionally small so at most a few threads can ever be parked at once.
_CODEX_RUN_EXECUTOR = ThreadPoolExecutor(max_workers=4, thread_name_prefix="codex-image-run")
_CODEX_CLOSE_EXECUTOR = ThreadPoolExecutor(max_workers=2, thread_name_prefix="codex-image-close")


async def _close_codex_image_subprocess(
    loop: asyncio.AbstractEventLoop,
    codex: Any,
    *,
    reason: str,
    image_timeout: float,
) -> None:
    if reason == "timeout":
        logger.warning("Codex image generation timed out after %.0fs; closing Codex subprocess", image_timeout)
    elif reason == "cancelled":
        logger.warning("Codex image generation was cancelled by caller; closing Codex subprocess")
    try:
        await asyncio.wait_for(
            loop.run_in_executor(_CODEX_CLOSE_EXECUTOR, codex.close),
            timeout=CODEX_IMAGE_CLOSE_TIMEOUT_SECONDS,
        )
    except Exception:
        logger.warning("Codex image: failed to close stalled subprocess", exc_info=True)


def make_codex_image_adapter(
    output_dir: str = DEFAULT_IMAGE_OUTPUT_DIR,
    image_timeout: float = CODEX_IMAGE_TIMEOUT_SECONDS,
) -> ImageAdapter:
    """Codex SDK image generation — drives the local Codex engine, saves a file.

    No API key: authentication comes from the Codex CLI (``~/.codex/auth.json``).
    The ``openai_codex`` import is lazy so this module loads without the SDK
    installed (the adapter is only registered when the SDK is actually present).
    The blocking ``thread.run`` call runs in a worker thread (so the event loop
    is not stalled) under an ``image_timeout`` deadline, so a hung Codex turn
    surfaces a ``TimeoutError`` to the caller instead of blocking indefinitely.
    """

    async def adapter(prompt: str, model: str = "") -> Optional[str]:
        target_path = _image_target_path(output_dir)
        out = target_path.parent
        target = target_path.resolve()
        model_id = model or CODEX_DEFAULT_IMAGE_MODEL
        instruction = _build_codex_image_prompt(prompt, str(target))

        # Share the live Codex handle with the event loop so a timeout can kill
        # the subprocess. thread.run() blocks the worker with no cancellation
        # token; wait_for cancels only the awaiting coroutine, so without an
        # out-of-band close() the worker thread would stay parked in the SDK's
        # blocking queue.get() and never free its executor slot.
        codex_box: dict = {}

        def _run_codex() -> Optional[str]:
            from openai_codex import Codex, Sandbox

            codex = Codex()
            codex_box["codex"] = codex
            with codex:
                thread = codex.thread_start(
                    cwd=str(out.resolve()),
                    model=model_id,
                    sandbox=Sandbox.workspace_write,
                )
                result = thread.run(instruction)
            status = getattr(getattr(result, "status", None), "value", None)
            if status != "completed":
                raise RuntimeError(f"Codex image: thread did not complete (status={status})")
            return _codex_saved_path_from_result(result)

        loop = asyncio.get_running_loop()
        try:
            # Run on the dedicated codex-run pool, NOT the default loop executor,
            # so a hung turn can't starve the default pool shared by S3 upload /
            # log-tail / AI search.
            saved = await asyncio.wait_for(
                loop.run_in_executor(_CODEX_RUN_EXECUTOR, _run_codex), timeout=image_timeout
            )
        except TimeoutError:
            codex = codex_box.get("codex")
            if codex is not None:
                # close() → terminate()/kill(); the SDK's reader thread then
                # fail_all()s the blocked queue.get(), unwinding the worker.
                # Submit on the SEPARATE close pool: when the run pool is
                # saturated by hangs, close() still gets a free slot — it's what
                # kills the subprocess and frees the parked run slot.
                await _close_codex_image_subprocess(loop, codex, reason="timeout", image_timeout=image_timeout)
            raise
        except asyncio.CancelledError:
            codex = codex_box.get("codex")
            if codex is not None:
                await asyncio.shield(
                    _close_codex_image_subprocess(loop, codex, reason="cancelled", image_timeout=image_timeout)
                )
            raise
        # Prefer the path Codex reported (it may pick its own filename inside our
        # output dir), but confine it to the requested directory: the prompt is
        # user/pipeline-controlled, so a reported path must not redirect the
        # returned/uploaded file outside `out`. Fall back to the requested target
        # if Codex wrote there without echoing the path back.
        if saved:
            saved_path = Path(saved).resolve()
            if saved_path.exists() and saved_path.parent == target.parent:
                return str(saved_path)
        if target.exists():
            return str(target)
        raise RuntimeError("Codex image: no image file produced")

    return adapter
