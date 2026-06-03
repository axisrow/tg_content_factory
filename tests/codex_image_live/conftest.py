"""Gate for the live Codex SDK image-generation test.

Opt-in only: the test runs a real Codex generation (drives the local Codex
engine, takes tens of seconds) and is skipped unless ``RUN_CODEX_IMAGE_LIVE`` is
explicitly truthy. Mirrors the opt-in gate style of the live CLI suite — never
auto-enabled, so CI stays green and silent.
"""

from __future__ import annotations

import os

import pytest

from tests.cli_real_tg_integration._live_readiness import _gate_enabled

GATE_ENV = "RUN_CODEX_IMAGE_LIVE"


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    if _gate_enabled(GATE_ENV):
        return
    skip_marker = pytest.mark.skip(
        reason=f"live Codex image test disabled; set {GATE_ENV}=1 to run — opt-in only, never auto-enabled"
    )
    here = os.path.dirname(os.path.abspath(__file__))
    for item in items:
        if os.path.abspath(str(item.fspath)).startswith(here):
            item.add_marker(skip_marker)
