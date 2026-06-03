"""ENV-gate for bounded Telegram-visible mutation CLI tests.

These tests intentionally mutate visible Telegram state, but only for an
auto-discovered live DB target with bounded scope and cleanup where the
operation is reversible.

Two env vars are required to run these tests:
- RUN_CLI_REAL_TG_LIVE=1 — required by the live CLI fixture.
- RUN_REAL_TELEGRAM_MUTATION_SAFE=1 — required by the root conftest's
  `real_tg_mutation_safe` marker policy.
"""
from __future__ import annotations

import os
import zlib
from pathlib import Path

import pytest

from tests.cli_real_tg_integration._live_readiness import _gate_enabled

GATE_ENV = "RUN_REAL_TELEGRAM_MUTATION_SAFE"


def make_minimal_png(path: Path) -> None:
    """Write a valid 1x1 white pixel PNG to *path* for use in photo-loader tests."""
    import struct

    def _chunk(chunk_type: bytes, data: bytes) -> bytes:
        c = chunk_type + data
        return struct.pack(">I", len(data)) + c + struct.pack(">I", zlib.crc32(c) & 0xFFFFFFFF)

    header = b"\x89PNG\r\n\x1a\n"
    ihdr = _chunk(b"IHDR", struct.pack(">IIBBBBB", 1, 1, 8, 2, 0, 0, 0))
    raw_pixel = b"\x00\xFF\xFF\xFF"
    idat = _chunk(b"IDAT", zlib.compress(raw_pixel))
    iend = _chunk(b"IEND", b"")
    path.write_bytes(header + ihdr + idat + iend)


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    if _gate_enabled(GATE_ENV):
        return

    skip_marker = pytest.mark.skip(
        reason=f"mutation-safe Telegram CLI tests disabled; set {GATE_ENV}=1 to force on, =0 to force off"
    )
    here = os.path.dirname(os.path.abspath(__file__))
    for item in items:
        if os.path.abspath(str(item.fspath)).startswith(here):
            item.add_marker(skip_marker)
