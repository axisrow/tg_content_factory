"""ENV-gate for destructive CLI tests — by analogy with heavy/conftest.py.

These tests launch real long-running processes (web server, worker, scheduler
daemon) or stop/restart them. They are inherently disruptive to anything
running on the local machine and are skipped unless the operator opts in.

**Three env vars are required** to run these tests:
- RUN_CLI_REAL_TG_LIVE=1 — required by the live CLI fixture.
- RUN_CLI_DESTRUCTIVE=1 — this folder-level gate (set below).
- RUN_REAL_TELEGRAM_MANUAL=1 — required by the root conftest's
  `real_tg_manual` marker policy (every test in this folder inherits the
  marker via its own pytestmark assignment).

Setting only RUN_CLI_DESTRUCTIVE=1 will still skip with the root conftest's
"real Telegram manual tests are disabled" message — all vars are needed.
"""
from __future__ import annotations

import os

import pytest

GATE_ENV = "RUN_CLI_DESTRUCTIVE"


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    if os.environ.get(GATE_ENV) == "1":
        return

    skip_marker = pytest.mark.skip(
        reason=(
            f"destructive CLI tests disabled; set {GATE_ENV}=1 "
            "AND RUN_REAL_TELEGRAM_MANUAL=1 to run them"
        )
    )
    here = os.path.dirname(os.path.abspath(__file__))
    for item in items:
        if os.path.abspath(str(item.fspath)).startswith(here):
            item.add_marker(skip_marker)
