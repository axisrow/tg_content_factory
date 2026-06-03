"""ENV-gate for mutating CLI tests — by analogy with heavy/conftest.py.

These tests touch user-visible data (channels/messages/pipelines/settings) on
the real project DB without a tidy rollback path.

**Two env vars are required** to run these tests:
- RUN_CLI_MUTATING=1 — this folder-level gate (set below).
- RUN_REAL_TELEGRAM_SAFE=1 — required by the root conftest's
  `real_tg_safe` marker policy (every test in this folder inherits the
  marker via its own pytestmark assignment).

Setting only RUN_CLI_MUTATING=1 will still skip with the root conftest's
"real Telegram safe tests are disabled" message — both vars are needed.
"""
from __future__ import annotations

import os

import pytest

from tests.cli_real_tg_integration._live_readiness import _gate_enabled

GATE_ENV = "RUN_CLI_MUTATING"


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    if _gate_enabled(GATE_ENV):
        return

    skip_marker = pytest.mark.skip(
        reason=(
            f"mutating CLI tests disabled; set {GATE_ENV}=1 (or =0 to force off) "
            "and RUN_REAL_TELEGRAM_SAFE — auto-enabled on a live-ready project"
        )
    )
    here = os.path.dirname(os.path.abspath(__file__))
    for item in items:
        if os.path.abspath(str(item.fspath)).startswith(here):
            item.add_marker(skip_marker)
