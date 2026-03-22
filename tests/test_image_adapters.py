"""Tests for image generation adapter factories in provider_adapters.py."""

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


# ── Together AI ──


@pytest.mark.asyncio
async def test_together_image_adapter_success():
    adapter = make_together_image_adapter("test-key")
    resp = _mock_response(json_data={"data": [{"url": "https://img.together.xyz/abc.png"}]})
    session = _mock_session([resp])

    with patch.object(aiohttp, "ClientSession", return_value=session):
        url = await adapter("A cat", "black-forest-labs/FLUX.1-schnell")

    assert url == "https://img.together.xyz/abc.png"


@pytest.mark.asyncio
async def test_together_image_adapter_error():
    adapter = make_together_image_adapter("test-key")
    resp = _mock_response(status=429, json_data={})
    session = _mock_session([resp])

    with patch.object(aiohttp, "ClientSession", return_value=session):
        with pytest.raises(RuntimeError, match="Together image error 429"):
            await adapter("A cat", "")


# ── HuggingFace ──


@pytest.mark.asyncio
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


# ── OpenAI ──


@pytest.mark.asyncio
async def test_openai_image_adapter_success():
    adapter = make_openai_image_adapter("test-key")
    resp = _mock_response(json_data={"data": [{"url": "https://oaidalleapi.blob/img.png"}]})
    session = _mock_session([resp])

    with patch.object(aiohttp, "ClientSession", return_value=session):
        url = await adapter("A sunset", "dall-e-3")

    assert url == "https://oaidalleapi.blob/img.png"


# ── Replicate ──


@pytest.mark.asyncio
async def test_replicate_image_adapter_polls_until_complete():
    adapter = make_replicate_image_adapter("test-token", timeout=5.0)

    create_resp = _mock_response(
        status=201,
        json_data={"urls": {"get": "https://api.replicate.com/v1/predictions/abc123"}},
    )
    poll_processing = _mock_response(
        status=200,
        json_data={"status": "processing", "output": None},
    )
    poll_succeeded = _mock_response(
        status=200,
        json_data={"status": "succeeded", "output": ["https://replicate.delivery/img.png"]},
    )

    session = _mock_session([create_resp, poll_processing, poll_succeeded])

    with patch.object(aiohttp, "ClientSession", return_value=session), patch(
        "src.services.provider_adapters.asyncio.sleep", new_callable=AsyncMock
    ):
        url = await adapter("A mountain", "flux-schnell")

    assert url == "https://replicate.delivery/img.png"


@pytest.mark.asyncio
async def test_replicate_image_adapter_failed_prediction():
    adapter = make_replicate_image_adapter("test-token", timeout=5.0)

    create_resp = _mock_response(
        status=201,
        json_data={"urls": {"get": "https://api.replicate.com/v1/predictions/abc"}},
    )
    poll_failed = _mock_response(
        status=200,
        json_data={"status": "failed", "error": "model crashed"},
    )

    session = _mock_session([create_resp, poll_failed])

    with patch.object(aiohttp, "ClientSession", return_value=session), patch(
        "src.services.provider_adapters.asyncio.sleep", new_callable=AsyncMock
    ):
        with pytest.raises(RuntimeError, match="model crashed"):
            await adapter("A test", "some-model")
