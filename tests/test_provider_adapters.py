"""Tests for provider adapters."""

import pytest

from src.services.provider_adapters import (
    _parse_json_for_text,
    make_context7_adapter,
    make_generic_http_adapter,
)


class FakeResp:
    def __init__(self, status=200, json_data=None, text_data=None):
        self.status = status
        self._json = json_data or {}
        self._text = text_data or ""

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def text(self):
        return self._text or ""

    async def json(self):
        return self._json


class FakeSession:
    def __init__(self, resp: FakeResp):
        self._resp = resp

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    def post(self, *args, **kwargs):
        return self._resp


def fake_client_session_factory(resp):
    class _Factory:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return FakeSession(resp)

        async def __aexit__(self, exc_type, exc, tb):
            return False

    return _Factory


# === SDK mocks for the OpenAI/Together/Replicate image adapters ===
#
# These adapters drive the official ``openai`` / ``replicate`` SDKs (issue #958)
# rather than raw aiohttp, so the tests mock the SDK client instead of
# ``aiohttp.ClientSession``.


class _FakeImage:
    """Mirrors the OpenAI SDK ``Image`` model: a ``url`` or ``b64_json`` field."""

    def __init__(self, url=None, b64_json=None):
        self.url = url
        self.b64_json = b64_json


class _FakeImagesResponse:
    def __init__(self, data):
        self.data = data


def patch_async_openai(monkeypatch, *, response=None, error=None, capture=None):
    """Patch ``openai.AsyncOpenAI`` so ``images.generate`` returns *response*
    (an iterable of ``_FakeImage``) or raises *error*.  When *capture* is a dict,
    the ``generate`` kwargs and client init kwargs are recorded for assertions.
    """

    class _FakeImages:
        async def generate(self, **kwargs):
            if capture is not None:
                capture["generate_kwargs"] = kwargs
            if error is not None:
                raise error
            return _FakeImagesResponse(list(response or []))

    class _FakeAsyncOpenAI:
        def __init__(self, **kwargs):
            if capture is not None:
                capture["client_kwargs"] = kwargs
            self.images = _FakeImages()

    import openai

    monkeypatch.setattr(openai, "AsyncOpenAI", _FakeAsyncOpenAI)
    return capture


class _FakeFileOutput:
    """Mirrors the Replicate SDK ``FileOutput`` — exposes a ``url`` attribute."""

    def __init__(self, url):
        self.url = url


def patch_replicate_client(monkeypatch, *, output=None, error=None, hang=False, capture=None):
    """Patch ``replicate.client.Client`` so ``async_run`` returns *output*,
    raises *error*, or hangs forever (to exercise the timeout path)."""

    class _FakeClient:
        def __init__(self, **kwargs):
            if capture is not None:
                capture["client_kwargs"] = kwargs

        async def async_run(self, ref, input=None, **params):
            if capture is not None:
                capture["ref"] = ref
                capture["input"] = input
            if hang:
                import asyncio

                await asyncio.Event().wait()  # never resolves
            if error is not None:
                raise error
            return output

    import replicate.client

    monkeypatch.setattr(replicate.client, "Client", _FakeClient)
    return capture


# === _parse_json_for_text tests ===


@pytest.mark.anyio
async def test_parse_json_none_returns_empty():
    assert await _parse_json_for_text(None) == ""


@pytest.mark.anyio
async def test_parse_json_string_returns_as_is():
    assert await _parse_json_for_text("hello world") == "hello world"


@pytest.mark.anyio
async def test_parse_json_openai_choices_with_message():
    data = {"choices": [{"message": {"content": "OpenAI reply"}}]}
    assert await _parse_json_for_text(data) == "OpenAI reply"


@pytest.mark.anyio
async def test_parse_json_openai_choices_with_text():
    data = {"choices": [{"text": "OpenAI text"}]}
    assert await _parse_json_for_text(data) == "OpenAI text"


@pytest.mark.anyio
async def test_parse_json_cohere_generations():
    data = {"generations": [{"text": "Cohere response"}]}
    assert await _parse_json_for_text(data) == "Cohere response"


@pytest.mark.anyio
async def test_parse_json_huggingface_generated_text():
    data = {"generated_text": "HF output"}
    assert await _parse_json_for_text(data) == "HF output"


@pytest.mark.anyio
async def test_parse_json_outputs_with_content():
    data = {"outputs": [{"content": "Output content"}]}
    assert await _parse_json_for_text(data) == "Output content"


@pytest.mark.anyio
async def test_parse_json_outputs_with_text():
    data = {"outputs": [{"text": "Output text"}]}
    assert await _parse_json_for_text(data) == "Output text"


@pytest.mark.anyio
async def test_parse_json_outputs_string():
    data = {"outputs": ["string output"]}
    assert "string output" in await _parse_json_for_text(data)


@pytest.mark.anyio
async def test_outputs_null_content_keeps_str_contract():
    # Regression #971: JSON null in outputs[0] must coerce to "" (-> str contract).
    data = {"outputs": [{"content": None, "text": None}]}
    result = await _parse_json_for_text(data)
    assert isinstance(result, str)
    assert result == ""


@pytest.mark.anyio
async def test_parse_json_result_string():
    data = {"result": "Result string"}
    assert await _parse_json_for_text(data) == "Result string"


@pytest.mark.anyio
async def test_result_dict_value_is_coerced_to_str():
    # Regression #971: a nested dict / null under result[k] must not leak out as
    # a non-str value, violating the -> str annotation.
    nested = await _parse_json_for_text({"result": {"text": {"nested": 1}}})
    assert isinstance(nested, str)
    null_val = await _parse_json_for_text({"result": {"text": None}})
    assert null_val == ""


@pytest.mark.anyio
async def test_parse_json_result_dict_with_text():
    data = {"result": {"text": "Result text"}}
    assert await _parse_json_for_text(data) == "Result text"


@pytest.mark.anyio
async def test_parse_json_result_dict_with_content():
    data = {"result": {"content": "Result content"}}
    assert await _parse_json_for_text(data) == "Result content"


@pytest.mark.anyio
async def test_parse_json_result_dict_with_generated_text():
    data = {"result": {"generated_text": "Generated"}}
    assert await _parse_json_for_text(data) == "Generated"


@pytest.mark.anyio
async def test_parse_json_result_dict_fallback():
    data = {"result": {"unknown_key": "value"}}
    assert "unknown_key" in await _parse_json_for_text(data)


@pytest.mark.anyio
async def test_parse_json_ollama_results_nested():
    data = {"results": [{"content": {"text": "Ollama nested"}}]}
    assert await _parse_json_for_text(data) == "Ollama nested"


@pytest.mark.anyio
async def test_parse_json_ollama_results_flat():
    data = {"results": [{"text": "Ollama flat"}]}
    assert await _parse_json_for_text(data) == "Ollama flat"


@pytest.mark.anyio
async def test_parse_json_fallback_first_string():
    data = {"key1": 123, "key2": "first string", "key3": "second"}
    assert await _parse_json_for_text(data) == "first string"


@pytest.mark.anyio
async def test_parse_json_fallback_str():
    data = {"key1": 123, "key2": [1, 2]}
    result = await _parse_json_for_text(data)
    assert "key1" in result


@pytest.mark.anyio
async def test_parse_json_list():
    data = [{"text": "list item"}]
    assert await _parse_json_for_text(data) == "list item"


@pytest.mark.anyio
async def test_parse_json_unknown_type():
    result = await _parse_json_for_text(42)
    assert result == "42"


# === Generic HTTP adapter tests ===


@pytest.mark.anyio
async def test_generic_http_adapter_success(monkeypatch):
    resp = FakeResp(status=200, json_data={"text": "Generic response"})
    monkeypatch.setattr("aiohttp.ClientSession", fake_client_session_factory(resp))
    adapter = make_generic_http_adapter("https://api.example.com/generate")
    out = await adapter("test prompt")
    assert "Generic response" in out


@pytest.mark.anyio
async def test_generic_http_adapter_with_model(monkeypatch):
    resp = FakeResp(status=200, json_data={"result": "Model output"})
    monkeypatch.setattr("aiohttp.ClientSession", fake_client_session_factory(resp))
    adapter = make_generic_http_adapter("https://api.example.com/generate")
    out = await adapter("prompt", model="custom-model")
    assert "Model output" in out


@pytest.mark.anyio
async def test_generic_http_adapter_with_api_key(monkeypatch):
    resp = FakeResp(status=200, json_data={"text": "Authenticated"})
    monkeypatch.setattr("aiohttp.ClientSession", fake_client_session_factory(resp))
    adapter = make_generic_http_adapter("https://api.example.com/generate", api_key="secret")
    out = await adapter("prompt")
    assert "Authenticated" in out


@pytest.mark.anyio
async def test_generic_http_adapter_custom_header(monkeypatch):
    resp = FakeResp(status=200, json_data={"text": "Custom header"})
    monkeypatch.setattr("aiohttp.ClientSession", fake_client_session_factory(resp))
    adapter = make_generic_http_adapter(
        "https://api.example.com/generate",
        api_key="secret",
        api_key_header="X-API-Key",
    )
    out = await adapter("prompt")
    assert "Custom header" in out


@pytest.mark.anyio
async def test_generic_http_adapter_error(monkeypatch):
    resp = FakeResp(status=502, text_data="Bad gateway")
    monkeypatch.setattr("aiohttp.ClientSession", fake_client_session_factory(resp))
    adapter = make_generic_http_adapter("https://api.example.com/generate")
    with pytest.raises(RuntimeError) as exc_info:
        await adapter("prompt")
    assert "502" in str(exc_info.value)


# === Context7 adapter tests ===


@pytest.mark.anyio
async def test_context7_adapter_success(monkeypatch):
    resp = FakeResp(status=200, json_data={"text": "Context7 response"})
    monkeypatch.setattr("aiohttp.ClientSession", fake_client_session_factory(resp))
    adapter = make_context7_adapter("test_key")
    out = await adapter("test prompt")
    assert "Context7 response" in out


@pytest.mark.anyio
async def test_context7_adapter_custom_base_url(monkeypatch):
    resp = FakeResp(status=200, json_data={"text": "Custom base"})
    monkeypatch.setattr("aiohttp.ClientSession", fake_client_session_factory(resp))
    adapter = make_context7_adapter("test_key", base_url="https://custom.context7.api")
    out = await adapter("prompt")
    assert "Custom base" in out


# === _parse_json_for_text edge cases (edge-case lines) ===


@pytest.mark.anyio
async def test_parse_json_openai_choices_empty_list():
    """Empty choices list falls through to str(data) fallback."""
    data = {"choices": []}
    result = await _parse_json_for_text(data)
    assert "choices" in result


@pytest.mark.anyio
async def test_parse_json_cohere_generations_empty():
    """Empty generations list falls through."""
    data = {"generations": []}
    result = await _parse_json_for_text(data)
    assert "generations" in result


@pytest.mark.anyio
async def test_parse_json_outputs_non_dict_item():
    """outputs with non-dict items falls to str(out)."""
    data = {"outputs": [42]}
    result = await _parse_json_for_text(data)
    assert "42" in result


@pytest.mark.anyio
async def test_parse_json_result_dict_nested():
    """result dict with unknown keys falls to str(r)."""
    data = {"result": {"random_key": 123}}
    result = await _parse_json_for_text(data)
    assert "random_key" in result


@pytest.mark.anyio
async def test_parse_json_ollama_results_non_dict():
    """results with non-dict items raises IndexError, falls to str(data)."""
    data = {"results": []}
    result = await _parse_json_for_text(data)
    assert "results" in result


@pytest.mark.anyio
async def test_parse_json_list_empty():
    """Empty list falls to str(data)."""
    result = await _parse_json_for_text([])
    assert result == "[]"


@pytest.mark.anyio
async def test_parse_json_result_non_dict_non_string():
    """result that is neither str nor dict falls to str(r)."""
    data = {"result": [1, 2, 3]}
    result = await _parse_json_for_text(data)
    # Falls through result dict checks to first-string-value fallback,
    # which returns str(data) since no string values found
    assert "[1, 2, 3]" in result


# === Image adapter tests ===


@pytest.mark.anyio
async def test_together_image_adapter_success(monkeypatch):
    from src.services.provider_adapters import make_together_image_adapter

    cap = patch_async_openai(
        monkeypatch, response=[_FakeImage(url="https://img.example.com/1.png")], capture={}
    )
    adapter = make_together_image_adapter("fake-key")
    result = await adapter("a cat", "black-forest-labs/FLUX.1-schnell")
    assert result == "https://img.example.com/1.png"
    # Together drives the OpenAI SDK against Together's base_url, carrying the
    # provider-specific ``steps`` knob through ``extra_body``.
    assert cap["client_kwargs"]["base_url"] == "https://api.together.xyz/v1"
    assert cap["generate_kwargs"]["model"] == "black-forest-labs/FLUX.1-schnell"
    assert cap["generate_kwargs"]["extra_body"] == {"steps": 4}


@pytest.mark.anyio
async def test_together_image_adapter_default_model(monkeypatch):
    from src.services.provider_adapters import make_together_image_adapter

    cap = patch_async_openai(
        monkeypatch, response=[_FakeImage(url="https://img.example.com/1.png")], capture={}
    )
    adapter = make_together_image_adapter("fake-key")
    await adapter("a cat", "")  # empty model → provider default
    assert cap["generate_kwargs"]["model"] == "black-forest-labs/FLUX.1-schnell"


@pytest.mark.anyio
async def test_together_image_adapter_error_status(monkeypatch):
    from src.services.provider_adapters import make_together_image_adapter

    patch_async_openai(monkeypatch, error=RuntimeError("500 Server error"))
    adapter = make_together_image_adapter("fake-key")
    with pytest.raises(RuntimeError, match="Together image error"):
        await adapter("a cat")


@pytest.mark.anyio
async def test_together_image_adapter_empty_data(monkeypatch):
    from src.services.provider_adapters import make_together_image_adapter

    patch_async_openai(monkeypatch, response=[])
    adapter = make_together_image_adapter("fake-key")
    with pytest.raises(RuntimeError, match="empty"):
        await adapter("a cat")


@pytest.mark.anyio
async def test_huggingface_image_adapter_warns_no_slash(monkeypatch, caplog):
    import logging

    from src.services.provider_adapters import make_huggingface_image_adapter

    # Provide an image-like response with content_type
    class ImageResp(FakeResp):
        content_type = "image/png"

        def __init__(self):
            super().__init__(status=200)
            self._image_bytes = b"\x89PNG\r\n\x1a\n"

        async def read(self):
            return self._image_bytes

    resp = ImageResp()
    monkeypatch.setattr("aiohttp.ClientSession", fake_client_session_factory(resp))

    import tempfile

    with tempfile.TemporaryDirectory() as tmpdir:
        adapter = make_huggingface_image_adapter("fake-token", output_dir=tmpdir)
        with caplog.at_level(logging.WARNING):
            result = await adapter("a cat", "noprovider")
        # Should warn about missing '/' separator
        assert any("lacks '/' separator" in r.message for r in caplog.records)
        assert result is not None
        assert result.endswith(".png")


@pytest.mark.anyio
async def test_huggingface_image_adapter_error_status(monkeypatch):
    from src.services.provider_adapters import make_huggingface_image_adapter

    resp = FakeResp(status=403, text_data="Forbidden")
    monkeypatch.setattr("aiohttp.ClientSession", fake_client_session_factory(resp))

    import tempfile

    with tempfile.TemporaryDirectory() as tmpdir:
        adapter = make_huggingface_image_adapter("fake-token", output_dir=tmpdir)
        with pytest.raises(RuntimeError, match="403"):
            await adapter("a cat")


@pytest.mark.anyio
async def test_huggingface_image_adapter_non_image_content_type(monkeypatch):
    from src.services.provider_adapters import make_huggingface_image_adapter

    class JsonResp(FakeResp):
        content_type = "application/json"

    resp = JsonResp(status=200, json_data={"error": "not an image"})
    monkeypatch.setattr("aiohttp.ClientSession", fake_client_session_factory(resp))

    import tempfile

    with tempfile.TemporaryDirectory() as tmpdir:
        adapter = make_huggingface_image_adapter("fake-token", output_dir=tmpdir)
        with pytest.raises(RuntimeError, match="expected image"):
            await adapter("a cat")


@pytest.mark.anyio
async def test_openai_image_adapter_success(monkeypatch):
    from src.services.provider_adapters import make_openai_image_adapter

    cap = patch_async_openai(
        monkeypatch, response=[_FakeImage(url="https://img.example.com/dalle.png")], capture={}
    )
    adapter = make_openai_image_adapter("fake-key")
    result = await adapter("a sunset", "dall-e-3")
    assert result == "https://img.example.com/dalle.png"
    assert cap["client_kwargs"]["api_key"] == "fake-key"
    assert cap["client_kwargs"]["base_url"] == "https://api.openai.com/v1"


@pytest.mark.anyio
async def test_openai_image_adapter_honours_api_base_env(monkeypatch):
    from src.services.provider_adapters import make_openai_image_adapter

    monkeypatch.setenv("OPENAI_API_BASE", "https://gateway.internal/v1/")
    cap = patch_async_openai(monkeypatch, response=[_FakeImage(url="https://x/y.png")], capture={})
    adapter = make_openai_image_adapter("fake-key")
    await adapter("a sunset", "dall-e-3")
    # Trailing slash is stripped so the SDK's URL joining stays correct.
    assert cap["client_kwargs"]["base_url"] == "https://gateway.internal/v1"


@pytest.mark.anyio
async def test_openai_image_adapter_error_status(monkeypatch):
    from src.services.provider_adapters import make_openai_image_adapter

    patch_async_openai(monkeypatch, error=RuntimeError("401 Unauthorized"))
    adapter = make_openai_image_adapter("fake-key")
    with pytest.raises(RuntimeError, match="OpenAI image error"):
        await adapter("a sunset")


@pytest.mark.anyio
async def test_openai_image_adapter_empty_data(monkeypatch):
    from src.services.provider_adapters import make_openai_image_adapter

    patch_async_openai(monkeypatch, response=[])
    adapter = make_openai_image_adapter("fake-key")
    with pytest.raises(RuntimeError, match="empty"):
        await adapter("a sunset")


@pytest.mark.anyio
async def test_openai_image_adapter_gpt_image_saves_b64(monkeypatch, tmp_path):
    """gpt-image-1 returns b64_json — adapter must decode and persist it to a file."""
    import base64

    from src.services import provider_adapters
    from src.services.provider_adapters import make_openai_image_adapter

    fake_png = b"\x89PNG\r\n\x1a\n" + b"\x00" * 32
    b64 = base64.b64encode(fake_png).decode()
    cap = patch_async_openai(monkeypatch, response=[_FakeImage(b64_json=b64)], capture={})
    monkeypatch.setattr(provider_adapters, "DEFAULT_IMAGE_OUTPUT_DIR", str(tmp_path))

    adapter = make_openai_image_adapter("fake-key")
    result = await adapter("a sunset", "")  # empty model → default gpt-image-1

    from pathlib import Path

    assert result is not None
    assert result.startswith(str(tmp_path))
    assert result.endswith(".png")
    assert Path(result).read_bytes() == fake_png
    # Empty model resolved to the gpt-image-1 default with its family params.
    assert cap["generate_kwargs"]["model"] == "gpt-image-1"
    assert cap["generate_kwargs"]["size"] == "auto"
    assert cap["generate_kwargs"]["quality"] == "auto"


def test_openai_params_gpt_image_omits_dalle_only_params():
    from src.services.provider_adapters import _openai_image_params

    params = _openai_image_params("p", "gpt-image-1")
    assert params["model"] == "gpt-image-1"
    assert params["size"] == "auto"
    assert params["quality"] == "auto"
    assert "response_format" not in params
    assert "style" not in params


def test_openai_params_legacy_dalle_uses_fixed_size():
    from src.services.provider_adapters import _openai_image_params

    params = _openai_image_params("p", "dall-e-3")
    assert params["model"] == "dall-e-3"
    assert params["size"] == "1024x1024"
    assert "quality" not in params


@pytest.mark.anyio
async def test_finalize_image_result_prefers_url():
    from src.services.provider_adapters import finalize_image_result

    result = await finalize_image_result({"url": "https://x/y.png", "b64_json": "ignored"})
    assert result == "https://x/y.png"


@pytest.mark.anyio
async def test_finalize_image_result_none_when_empty():
    from src.services.provider_adapters import finalize_image_result

    assert await finalize_image_result({}) is None


@pytest.mark.anyio
async def test_finalize_image_result_accepts_sdk_image_object():
    """finalize_image_result also accepts an SDK Image (url/b64_json attributes)."""
    from src.services.provider_adapters import finalize_image_result

    result = await finalize_image_result(_FakeImage(url="https://x/sdk.png"))
    assert result == "https://x/sdk.png"


@pytest.mark.anyio
async def test_save_image_b64_rejects_invalid_payload(tmp_path):
    from src.services.provider_adapters import save_image_b64

    with pytest.raises(RuntimeError, match="invalid base64"):
        await save_image_b64("not-valid-base64!!!", output_dir=str(tmp_path))


@pytest.mark.anyio
async def test_replicate_image_adapter_success(monkeypatch):
    from src.services.provider_adapters import make_replicate_image_adapter

    cap = patch_replicate_client(
        monkeypatch, output=_FakeFileOutput("https://img.example.com/replicate.png"), capture={}
    )
    adapter = make_replicate_image_adapter("fake-token", timeout=10.0)
    result = await adapter("a cat", "black-forest-labs/flux-schnell")
    assert result == "https://img.example.com/replicate.png"
    assert cap["client_kwargs"]["api_token"] == "fake-token"
    assert cap["ref"] == "black-forest-labs/flux-schnell"
    assert cap["input"] == {"prompt": "a cat"}


@pytest.mark.anyio
async def test_replicate_image_adapter_string_output(monkeypatch):
    """A bare URL-string output is returned as-is."""
    from src.services.provider_adapters import make_replicate_image_adapter

    patch_replicate_client(monkeypatch, output="https://img.example.com/str.png")
    adapter = make_replicate_image_adapter("fake-token", timeout=10.0)
    result = await adapter("a cat", "black-forest-labs/flux-schnell")
    assert result == "https://img.example.com/str.png"


@pytest.mark.anyio
async def test_replicate_image_adapter_error(monkeypatch):
    from src.services.provider_adapters import make_replicate_image_adapter

    patch_replicate_client(monkeypatch, error=RuntimeError("model load error"))
    adapter = make_replicate_image_adapter("fake-token")
    with pytest.raises(RuntimeError, match="model load error"):
        await adapter("a cat")


@pytest.mark.anyio
async def test_replicate_image_adapter_warns_no_slash(monkeypatch, caplog):
    import logging

    from src.services.provider_adapters import make_replicate_image_adapter

    cap = patch_replicate_client(
        monkeypatch, output=_FakeFileOutput("https://img.example.com/img.png"), capture={}
    )
    adapter = make_replicate_image_adapter("fake-token", timeout=10.0)

    with caplog.at_level(logging.WARNING):
        result = await adapter("a cat", "noprovider")
    assert any("lacks '/' separator" in r.message for r in caplog.records)
    assert result == "https://img.example.com/img.png"
    # Bad model id fell back to the provider default, not the raw "noprovider".
    assert cap["ref"] == "black-forest-labs/flux-schnell"


@pytest.mark.anyio
async def test_replicate_image_adapter_list_output(monkeypatch):
    """Replicate adapter returns first item when output is a list of FileOutputs."""
    from src.services.provider_adapters import make_replicate_image_adapter

    patch_replicate_client(
        monkeypatch,
        output=[
            _FakeFileOutput("https://img.example.com/1.png"),
            _FakeFileOutput("https://img.example.com/2.png"),
        ],
    )
    adapter = make_replicate_image_adapter("fake-token", timeout=10.0)
    result = await adapter("a cat")
    assert result == "https://img.example.com/1.png"


@pytest.mark.anyio
async def test_replicate_image_adapter_timeout(monkeypatch):
    """Replicate adapter raises RuntimeError when the prediction never resolves."""
    from src.services.provider_adapters import make_replicate_image_adapter

    patch_replicate_client(monkeypatch, hang=True)
    adapter = make_replicate_image_adapter("fake-token", timeout=0.05)
    with pytest.raises(RuntimeError, match="timed out"):
        await adapter("a cat")


# === Codex SDK image adapter ===


def test_build_codex_image_prompt_has_imagegen_and_path():
    from src.services.provider_adapters import _build_codex_image_prompt

    prompt = _build_codex_image_prompt("a sunset", "/tmp/out/abc.png")
    assert "$imagegen" in prompt
    assert "a sunset" in prompt
    assert "/tmp/out/abc.png" in prompt


def test_codex_saved_path_from_image_generation_item():
    from types import SimpleNamespace

    from src.services.provider_adapters import _codex_saved_path_from_result

    result = SimpleNamespace(
        items=[SimpleNamespace(type="imageGeneration", saved_path="/tmp/img.png")]
    )
    assert _codex_saved_path_from_result(result) == "/tmp/img.png"


def test_codex_saved_path_from_image_view_item():
    from types import SimpleNamespace

    from src.services.provider_adapters import _codex_saved_path_from_result

    result = SimpleNamespace(items=[SimpleNamespace(type="imageView", path="/tmp/view.png")])
    assert _codex_saved_path_from_result(result) == "/tmp/view.png"


def test_codex_saved_path_none_when_no_image_item():
    from types import SimpleNamespace

    from src.services.provider_adapters import _codex_saved_path_from_result

    result = SimpleNamespace(items=[SimpleNamespace(type="agentMessage")])
    assert _codex_saved_path_from_result(result) is None


def _install_fake_codex(
    monkeypatch,
    *,
    status="completed",
    write_target=True,
    item_path=True,
    rogue_saved_path=None,
    hang_seconds=0.0,
):
    """Install a fake ``openai_codex`` module that simulates one image turn.

    When ``write_target`` is True the fake writes a PNG to the cwd-resolved path
    Codex was asked to save to (parsed out of the instruction), mimicking the
    real engine writing the file. ``item_path`` controls whether the returned
    result echoes the saved path back in its items. ``rogue_saved_path`` echoes a
    *different* path in the items (simulating a prompt-injected turn reporting a
    file outside the requested output dir); the file there is created so it
    exists, to prove the adapter rejects it on directory grounds, not existence.
    """
    import sys
    from types import ModuleType, SimpleNamespace

    captured = {}

    import threading

    closed_event = threading.Event()
    started_event = threading.Event()

    class FakeThread:
        def run(self, instruction):
            started_event.set()
            if hang_seconds:
                # Block as the real SDK does inside a blocking queue.get(); the
                # only way out is close() firing closed_event (mirrors the SDK's
                # reader thread fail_all()ing the queue on subprocess kill).
                if closed_event.wait(timeout=hang_seconds):
                    raise RuntimeError("Codex image: subprocess closed")
            # The instruction embeds the absolute save path on its own line.
            save_path = None
            for line in instruction.splitlines():
                line = line.strip()
                if line.endswith(".png"):
                    save_path = line
            captured["save_path"] = save_path
            if write_target and save_path:
                with open(save_path, "wb") as fh:
                    fh.write(b"\x89PNG\r\n\x1a\n" + b"\x00" * 16)
            items = []
            if rogue_saved_path is not None:
                with open(rogue_saved_path, "wb") as fh:
                    fh.write(b"\x89PNG\r\n\x1a\n" + b"\x00" * 16)
                items.append(SimpleNamespace(type="imageGeneration", saved_path=str(rogue_saved_path)))
            elif item_path and save_path:
                items.append(SimpleNamespace(type="imageGeneration", saved_path=save_path))
            return SimpleNamespace(status=SimpleNamespace(value=status), items=items, final_response="done")

    class FakeCodex:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def thread_start(self, **kwargs):
            captured["start_kwargs"] = kwargs
            return FakeThread()

        def close(self):
            # Real Codex.close() terminate()/kill()s the subprocess; here it
            # releases the blocked run() and records that it was called.
            captured["closed"] = True
            closed_event.set()

    fake_mod = ModuleType("openai_codex")
    fake_mod.Codex = FakeCodex
    fake_mod.Sandbox = SimpleNamespace(workspace_write="workspace-write")
    monkeypatch.setitem(sys.modules, "openai_codex", fake_mod)
    captured["started_event"] = started_event
    return captured


@pytest.mark.anyio
async def test_codex_image_adapter_returns_saved_path(monkeypatch, tmp_path):
    from pathlib import Path

    from src.services.provider_adapters import make_codex_image_adapter

    captured = _install_fake_codex(monkeypatch)
    adapter = make_codex_image_adapter(output_dir=str(tmp_path))
    result = await adapter("a robot painting", "gpt-5.4")

    assert result is not None
    assert result.endswith(".png")
    assert Path(result).exists()
    assert Path(result).read_bytes().startswith(b"\x89PNG")
    # default cwd is the resolved output dir; requested model is threaded through
    assert captured["start_kwargs"]["model"] == "gpt-5.4"
    assert captured["start_kwargs"]["sandbox"] == "workspace-write"


@pytest.mark.anyio
async def test_codex_image_adapter_default_model(monkeypatch, tmp_path):
    from src.services.provider_adapters import CODEX_DEFAULT_IMAGE_MODEL, make_codex_image_adapter

    captured = _install_fake_codex(monkeypatch)
    adapter = make_codex_image_adapter(output_dir=str(tmp_path))
    await adapter("a cat", "")  # empty model → default

    assert captured["start_kwargs"]["model"] == CODEX_DEFAULT_IMAGE_MODEL


@pytest.mark.anyio
async def test_codex_image_adapter_falls_back_to_target_when_no_item(monkeypatch, tmp_path):
    """Codex wrote the file but did not echo the path in items → use the target."""
    from pathlib import Path

    from src.services.provider_adapters import make_codex_image_adapter

    _install_fake_codex(monkeypatch, item_path=False)
    adapter = make_codex_image_adapter(output_dir=str(tmp_path))
    result = await adapter("a dog", "gpt-5.4")

    assert result is not None
    assert Path(result).exists()


@pytest.mark.anyio
async def test_codex_image_adapter_times_out_and_kills_subprocess(monkeypatch, tmp_path):
    """A stalled Codex turn times out AND closes the subprocess to free the thread."""
    import asyncio

    from src.services.provider_adapters import make_codex_image_adapter

    captured = _install_fake_codex(monkeypatch, hang_seconds=5.0)
    adapter = make_codex_image_adapter(output_dir=str(tmp_path), image_timeout=0.05)
    with pytest.raises((asyncio.TimeoutError, TimeoutError)):
        await adapter("x", "gpt-5.4")
    # close() was called on timeout — without it the worker thread would leak.
    assert captured.get("closed") is True


@pytest.mark.anyio
async def test_codex_image_adapter_cancel_closes_subprocess(monkeypatch, tmp_path):
    """Caller cancellation also closes the Codex subprocess."""
    import asyncio

    from src.services.provider_adapters import make_codex_image_adapter

    captured = _install_fake_codex(monkeypatch, hang_seconds=5.0)
    adapter = make_codex_image_adapter(output_dir=str(tmp_path), image_timeout=5.0)

    task = asyncio.create_task(adapter("x", "gpt-5.4"))
    assert await asyncio.to_thread(captured["started_event"].wait, 1.0)

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert captured.get("closed") is True


@pytest.mark.anyio
async def test_codex_image_adapter_rejects_path_outside_output_dir(monkeypatch, tmp_path):
    """A reported saved_path outside the requested output dir is not returned.

    The prompt is user/pipeline-controlled, so a turn that echoes a path outside
    `output_dir` must not redirect the returned file. Here Codex reports a file
    in a sibling dir and writes no file to the target → the adapter rejects the
    rogue path and, with no target file, raises rather than leaking it.
    """
    from src.services.provider_adapters import make_codex_image_adapter

    rogue = tmp_path.parent / "rogue.png"
    out = tmp_path / "out"
    out.mkdir()
    _install_fake_codex(monkeypatch, write_target=False, rogue_saved_path=rogue)
    adapter = make_codex_image_adapter(output_dir=str(out))
    try:
        with pytest.raises(RuntimeError, match="no image file"):
            await adapter("x", "gpt-5.4")
    finally:
        if rogue.exists():
            rogue.unlink()


@pytest.mark.anyio
async def test_codex_image_adapter_raises_on_failed_status(monkeypatch, tmp_path):
    from src.services.provider_adapters import make_codex_image_adapter

    _install_fake_codex(monkeypatch, status="failed", write_target=False)
    adapter = make_codex_image_adapter(output_dir=str(tmp_path))
    with pytest.raises(RuntimeError, match="did not complete"):
        await adapter("x", "gpt-5.4")


@pytest.mark.anyio
async def test_codex_image_adapter_raises_when_no_file(monkeypatch, tmp_path):
    """Status completed but no file written and no item path → error, not silent None."""
    from src.services.provider_adapters import make_codex_image_adapter

    _install_fake_codex(monkeypatch, write_target=False, item_path=False)
    adapter = make_codex_image_adapter(output_dir=str(tmp_path))
    with pytest.raises(RuntimeError, match="no image file"):
        await adapter("x", "gpt-5.4")


@pytest.mark.anyio
async def test_parse_json_openai_content_null_returns_empty():
    """OpenAI content can be JSON null (tool-call/refusal) — coerce to "" not None (#836/8)."""
    data = {"choices": [{"message": {"content": None}}]}
    assert await _parse_json_for_text(data) == ""
