"""Live end-to-end test: real Codex SDK image generation through the project adapter.

Opt-in via ``RUN_CODEX_IMAGE_LIVE=1`` (see conftest). Uses the same
``make_codex_image_adapter`` the ``codex`` image provider registers, so this
exercises the real code path — Codex engine + ``$imagegen`` writing a PNG —
not a reimplementation. Verifies the agreed contract: a file is created and it
is a valid, non-empty PNG.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

pytestmark = pytest.mark.codex_image_live

_PNG_MAGIC = b"\x89PNG\r\n\x1a\n"


# The openai_codex SDK leaks an unclosed file handle when its client is torn
# down (api.py close()), surfacing as a PytestUnraisableExceptionWarning under
# the project's filterwarnings=["error"] policy. It is upstream-only and
# unrelated to the image result, so suppress that one warning locally rather
# than weakening the global policy.
@pytest.mark.filterwarnings("ignore::ResourceWarning")
@pytest.mark.filterwarnings("ignore::pytest.PytestUnraisableExceptionWarning")
@pytest.mark.timeout(300)
async def test_codex_adapter_generates_valid_png(tmp_path):
    from src.services.provider_adapters import codex_available, make_codex_image_adapter

    if not codex_available():
        pytest.skip("Codex SDK not installed or Codex CLI not authenticated")

    adapter = make_codex_image_adapter(output_dir=str(tmp_path))
    result = await adapter("a small friendly robot painting a sunset, square", "gpt-5.4")

    assert result is not None, "adapter returned no path"
    path = Path(result)
    assert await asyncio.to_thread(path.exists), f"image file not created: {result}"
    data = await asyncio.to_thread(path.read_bytes)
    assert len(data) > 0, "image file is empty"
    assert data.startswith(_PNG_MAGIC), "file is not a valid PNG"
