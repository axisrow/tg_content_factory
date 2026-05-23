from __future__ import annotations

import os

import pytest

MANUAL_GATE_ENV = "RUN_REAL_TELEGRAM_MANUAL"


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    if os.environ.get(MANUAL_GATE_ENV) == "1":
        return

    skip_marker = pytest.mark.skip(
        reason=f"manual Telegram-visible CLI tests disabled; set {MANUAL_GATE_ENV}=1 to run them"
    )
    here = os.path.dirname(os.path.abspath(__file__))
    for item in items:
        if os.path.abspath(str(item.fspath)).startswith(here):
            item.add_marker(skip_marker)
