from __future__ import annotations

import os

import pytest

from tests.cli_real_tg_integration._live_readiness import _gate_enabled

HEAVY_GATE_ENV = "RUN_CLI_REAL_TG_HEAVY"


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    if _gate_enabled(HEAVY_GATE_ENV):
        return

    skip_marker = pytest.mark.skip(
        reason=f"heavy CLI tests disabled; set {HEAVY_GATE_ENV}=1 to run — opt-in only, never auto-enabled"
    )
    here = os.path.dirname(os.path.abspath(__file__))
    for item in items:
        if os.path.abspath(str(item.fspath)).startswith(here):
            item.add_marker(skip_marker)
