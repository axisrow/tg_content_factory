"""Tests for image generation adapter factories in provider_adapters.py.

The OpenAI/Together/Replicate adapters drive the official ``openai`` / ``replicate``
SDKs (issue #958), so those tests mock the SDK client. HuggingFace stays on raw
aiohttp (its SDK returns a PIL.Image), so its test still mocks ``aiohttp.ClientSession``.
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest

from src.services.provider_adapters import (
    make_huggingface_image_adapter,
    make_openai_image_adapter,
    make_replicate_image_adapter,
    make_together_image_adapter,
)

# ── aiohttp mocks (HuggingFace only) ──


def _mock_response(
    *, status: int = 200, json_data: dict | None = None, content: bytes = b"", content_type: str = ""
):
    """Build a mock aiohttp response."""
    resp = AsyncMock()
    resp.status = status
    resp.content_type = content_type
    resp.text = AsyncMock(return_value=json.dumps(json_data) if json_data else "")
    resp.json = AsyncMock(return_value=json_data)
    resp.read = AsyncMock(return_value=content)
    resp.__aenter__ = AsyncMock(return_value=resp)
    resp.__aexit__ = AsyncMock(return_value=False)
    return resp


def _mock_session(responses: list):
    """Build a mock aiohttp.ClientSession that returns *responses* in order."""
    session = MagicMock()
    call_idx = {"i": 0}

    def _next_response(*args, **kwargs):
        idx = call_idx["i"]
        call_idx["i"] += 1
        return responses[idx]

    session.post = MagicMock(side_effect=_next_response)
    session.get = MagicMock(side_effect=_next_response)
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=False)
    return session


# ── SDK mocks (OpenAI / Together / Replicate) ──


class _SDKImage:
    """Mirrors the OpenAI SDK ``Image`` model."""

    def __init__(self, url=None, b64_json=None):
        self.url = url
        self.b64_json = b64_json


def _patch_async_openai(*, images: list):
    """Return a context manager patching ``openai.AsyncOpenAI`` so its
    ``images.generate`` returns an object with ``.data == images``.

    The fake is an async context manager — the adapters use ``async with``."""

    class _FakeImages:
        async def generate(self, **kwargs):
            return MagicMock(data=list(images))

    class _FakeAsyncOpenAI:
        def __init__(self, **kwargs):
            self.images = _FakeImages()

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

    import openai

    return patch.object(openai, "AsyncOpenAI", _FakeAsyncOpenAI)


class _FakeFileOutput:
    """Mirrors the Replicate SDK ``FileOutput``."""

    def __init__(self, url):
        self.url = url


class _FakeReplicateHttpx:
    async def aclose(self):
        return None


def _patch_replicate(*, output):
    class _FakeClient:
        def __init__(self, **kwargs):
            self._async_client = _FakeReplicateHttpx()

        async def async_run(self, ref, input=None, **params):
            return output

    import replicate.client

    return patch.object(replicate.client, "Client", _FakeClient)


# ── Together AI (OpenAI-compatible SDK) ──


@pytest.mark.anyio
async def test_together_image_adapter_success():
    adapter = make_together_image_adapter("test-key")
    with _patch_async_openai(images=[_SDKImage(url="https://img.together.xyz/abc.png")]):
        url = await adapter("A cat", "black-forest-labs/FLUX.1-schnell")
    assert url == "https://img.together.xyz/abc.png"


@pytest.mark.anyio
async def test_together_image_adapter_error():
    adapter = make_together_image_adapter("test-key")

    class _FakeImages:
        async def generate(self, **kwargs):
            raise RuntimeError("429 rate limited")

    class _FakeAsyncOpenAI:
        def __init__(self, **kwargs):
            self.images = _FakeImages()

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

    import openai

    with patch.object(openai, "AsyncOpenAI", _FakeAsyncOpenAI):
        with pytest.raises(RuntimeError, match="Together image error"):
            await adapter("A cat", "")


# ── HuggingFace (raw aiohttp) ──


@pytest.mark.anyio
async def test_huggingface_image_adapter_saves_binary(tmp_path):
    adapter = make_huggingface_image_adapter("test-token", output_dir=str(tmp_path))
    fake_png = b"\x89PNG\r\n\x1a\n" + b"\x00" * 100
    resp = _mock_response(status=200, content=fake_png, content_type="image/png")
    session = _mock_session([resp])

    with patch.object(aiohttp, "ClientSession", return_value=session):
        path = await adapter("A dog", "stabilityai/sdxl")

    assert path is not None
    assert path.startswith(str(tmp_path))
    assert path.endswith(".png")
    # Verify file was written
    from pathlib import Path

    assert Path(path).read_bytes() == fake_png


# ── OpenAI (official SDK) ──


@pytest.mark.anyio
async def test_openai_image_adapter_success():
    adapter = make_openai_image_adapter("test-key")
    with _patch_async_openai(images=[_SDKImage(url="https://oaidalleapi.blob/img.png")]):
        url = await adapter("A sunset", "dall-e-3")
    assert url == "https://oaidalleapi.blob/img.png"


# ── Replicate (official SDK) ──


@pytest.mark.anyio
async def test_replicate_image_adapter_returns_url():
    adapter = make_replicate_image_adapter("test-token", timeout=5.0)
    with _patch_replicate(output=_FakeFileOutput("https://replicate.delivery/img.png")):
        url = await adapter("A mountain", "black-forest-labs/flux-schnell")
    assert url == "https://replicate.delivery/img.png"


@pytest.mark.anyio
async def test_replicate_image_adapter_failed_prediction():
    adapter = make_replicate_image_adapter("test-token", timeout=5.0)

    class _FakeClient:
        def __init__(self, **kwargs):
            pass

        async def async_run(self, ref, input=None, **params):
            raise RuntimeError("model crashed")

    import replicate.client

    with patch.object(replicate.client, "Client", _FakeClient):
        with pytest.raises(RuntimeError, match="model crashed"):
            await adapter("A test", "some/model")
