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

import pytest

GATE_ENV = "RUN_REAL_TELEGRAM_MUTATION_SAFE"


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    if os.environ.get(GATE_ENV) == "1":
        return

    skip_marker = pytest.mark.skip(
        reason=f"mutation-safe Telegram CLI tests disabled; set {GATE_ENV}=1 to run them"
    )
    here = os.path.dirname(os.path.abspath(__file__))
    for item in items:
        if os.path.abspath(str(item.fspath)).startswith(here):
            item.add_marker(skip_marker)
