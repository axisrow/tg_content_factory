"""ENV-gate for mutating CLI tests — by analogy with heavy/conftest.py.

These tests touch user-visible data (channels/messages/pipelines/settings) on
the real project DB without a tidy rollback path. They are skipped unless the
operator explicitly opts in by setting RUN_CLI_MUTATING=1.
"""
from __future__ import annotations

import os

import pytest

GATE_ENV = "RUN_CLI_MUTATING"


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    if os.environ.get(GATE_ENV) == "1":
        return

    skip_marker = pytest.mark.skip(
        reason=f"mutating CLI tests disabled; set {GATE_ENV}=1 to run them"
    )
    here = os.path.dirname(os.path.abspath(__file__))
    for item in items:
        if os.path.abspath(str(item.fspath)).startswith(here):
            item.add_marker(skip_marker)
