"""ENV-gate for mutating CLI tests — by analogy with heavy/conftest.py.

These tests touch user-visible data (channels/messages/pipelines/settings) on
the real project DB. They are opt-in only and never auto-enable — they act on a
real account and must be run deliberately, by hand.

**Three env vars are required** to run these tests:
- RUN_CLI_MUTATING=1 — this folder-level gate (set below).
- RUN_REAL_TELEGRAM_SAFE=1 — required by the root conftest's
  `real_tg_safe` marker policy (every test in this folder inherits the
  marker via its own pytestmark assignment).
- RUN_CLI_REAL_TG_LIVE=1 — required by the live CLI fixture (`cli_real_cli_env`).

Setting only some of them will still skip — all three are needed.
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
            f"mutating CLI tests disabled; set {GATE_ENV}=1 (and RUN_REAL_TELEGRAM_SAFE=1) "
            "to run — opt-in only, never auto-enabled"
        )
    )
    here = os.path.dirname(os.path.abspath(__file__))
    for item in items:
        if os.path.abspath(str(item.fspath)).startswith(here):
            item.add_marker(skip_marker)
