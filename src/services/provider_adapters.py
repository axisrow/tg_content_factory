from __future__ import annotations

import asyncio
import logging
import os
import uuid
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, Optional

import aiohttp

logger = logging.getLogger(__name__)

# Type alias for image generation adapters
ImageAdapter = Callable[[str, str], Awaitable[Optional[str]]]


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
                        return c["message"].get("content", "")
                    return c.get("text", "")
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
                    return out.get("content", out.get("text", ""))
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
                        return r[k]
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


def make_cohere_adapter(
    api_key: str, base_url: Optional[str] = None
) -> Callable[..., Awaitable[str]]:
    base = base_url or os.environ.get("COHERE_API_BASE", "https://api.cohere.ai/v1/generate")

    async def provider(
        prompt: str = "",
        model: Optional[str] = None,
        max_tokens: int = 256,
        temperature: float = 0.0,
        stream: bool = False,
        **kwargs: Any,
    ) -> str:
        url = base
        headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
        payload = {"prompt": prompt, "max_tokens": int(max_tokens or 256)}
        if model:
            payload["model"] = model
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload, headers=headers) as resp:
                    text = await resp.text()
                    if resp.status != 200:
                        raise RuntimeError(f"Cohere error {resp.status}: {text}")
                    data = await resp.json()
                    return await _parse_json_for_text(data)
        except Exception:
            raise

    return provider


def make_ollama_adapter(
    base_url: Optional[str] = None, api_key: Optional[str] = None
) -> Callable[..., Awaitable[str]]:
    base = base_url or os.environ.get("OLLAMA_BASE", "http://localhost:11434")
    endpoint = base.rstrip("/") + "/api/generate"

    async def provider(
        prompt: str = "",
        model: Optional[str] = None,
        max_tokens: int = 256,
        temperature: float = 0.0,
        stream: bool = False,
        **kwargs: Any,
    ) -> str:
        payload: Dict[str, Any] = {
            "model": model or os.environ.get("OLLAMA_DEFAULT_MODEL", "llama3.2"),
            "prompt": prompt,
        }
        headers = {"Content-Type": "application/json"}
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(endpoint, json=payload, headers=headers) as resp:
                    text = await resp.text()
                    if resp.status != 200:
                        raise RuntimeError(f"Ollama error {resp.status}: {text}")
                    data = await resp.json()
                    return await _parse_json_for_text(data)
        except Exception:
            raise

    return provider


def make_huggingface_adapter(
    api_token: str, base_url: Optional[str] = None
) -> Callable[..., Awaitable[str]]:
    # Use inference endpoint when model is provided in model arg; otherwise use base_url
    base = base_url or os.environ.get(
        "HUGGINGFACE_API_BASE", "https://api-inference.huggingface.co/models"
    )

    async def provider(
        prompt: str = "",
        model: Optional[str] = None,
        max_tokens: int = 256,
        temperature: float = 0.0,
        stream: bool = False,
        **kwargs: Any,
    ) -> str:
        if model:
            url = f"{base.rstrip('/')}/{model}"
        else:
            # fallback generic route
            url = base
        headers = {"Authorization": f"Bearer {api_token}"}
        payload = {"inputs": prompt, "parameters": {"max_new_tokens": int(max_tokens or 256)}}
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload, headers=headers) as resp:
                    text = await resp.text()
                    if resp.status != 200:
                        raise RuntimeError(f"HuggingFace error {resp.status}: {text}")
                    data = await resp.json()
                    return await _parse_json_for_text(data)
        except Exception:
            raise

    return provider


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
            async with aiohttp.ClientSession() as session:
                async with session.post(endpoint, json=payload, headers=headers) as resp:
                    text = await resp.text()
                    if resp.status != 200:
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


# Convenience shims
def make_cohere(api_key: str) -> Callable[..., Awaitable[str]]:
    return make_cohere_adapter(api_key)


def make_ollama(
    base_url: Optional[str] = None, api_key: Optional[str] = None
) -> Callable[..., Awaitable[str]]:
    return make_ollama_adapter(base_url, api_key)


def make_huggingface(api_token: str) -> Callable[..., Awaitable[str]]:
    return make_huggingface_adapter(api_token)


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
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, headers=headers) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    raise RuntimeError(f"Together image error {resp.status}: {text}")
                data = await resp.json()
                items = data.get("data")
                if not items:
                    raise RuntimeError(f"Together image: empty 'data' in response: {data}")
                return items[0].get("url") or items[0].get("b64_json")

    return adapter


def make_huggingface_image_adapter(api_token: str, output_dir: str = "data/images") -> ImageAdapter:
    """HuggingFace Inference API — returns binary image, saved to local file."""

    async def adapter(prompt: str, model: str = "") -> Optional[str]:
        default_model = "stabilityai/stable-diffusion-xl-base-1.0"
        if model and "/" not in model:
            logger.warning("HuggingFace: model %r lacks '/' separator, falling back to %s", model, default_model)
        model_id = model if model and "/" in model else default_model
        url = f"https://api-inference.huggingface.co/models/{model_id}"
        headers = {"Authorization": f"Bearer {api_token}"}
        payload = {"inputs": prompt}
        async with aiohttp.ClientSession() as session:
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
                out = Path(output_dir)
                out.mkdir(parents=True, exist_ok=True)
                filename = f"{uuid.uuid4().hex}.png"
                filepath = out / filename
                filepath.write_bytes(image_bytes)
                return str(filepath)

    return adapter


def make_openai_image_adapter(api_key: str) -> ImageAdapter:
    """OpenAI DALL-E image generation."""

    async def adapter(prompt: str, model: str = "") -> Optional[str]:
        base = os.environ.get("OPENAI_API_BASE", "https://api.openai.com/v1")
        url = f"{base.rstrip('/')}/images/generations"
        headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
        payload: Dict[str, Any] = {
            "model": model or "dall-e-3",
            "prompt": prompt,
            "n": 1,
            "size": "1024x1024",
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, headers=headers) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    raise RuntimeError(f"OpenAI image error {resp.status}: {text}")
                data = await resp.json()
                items = data.get("data")
                if not items:
                    raise RuntimeError(f"OpenAI image: empty 'data' in response: {data}")
                return items[0].get("url") or items[0].get("b64_json")

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
        async with aiohttp.ClientSession() as session:
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
