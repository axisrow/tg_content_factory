"""ENV-gate for destructive CLI tests — by analogy with heavy/conftest.py.

These tests launch real long-running processes (web server, worker, scheduler
daemon) or stop/restart them. They are inherently disruptive to anything
running on the local machine and are skipped unless the operator explicitly
opts in by setting RUN_CLI_DESTRUCTIVE=1.
"""
from __future__ import annotations

import os

import pytest

GATE_ENV = "RUN_CLI_DESTRUCTIVE"


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    if os.environ.get(GATE_ENV) == "1":
        return

    skip_marker = pytest.mark.skip(
        reason=f"destructive CLI tests disabled; set {GATE_ENV}=1 to run them"
    )
    here = os.path.dirname(os.path.abspath(__file__))
    for item in items:
        if os.path.abspath(str(item.fspath)).startswith(here):
            item.add_marker(skip_marker)
