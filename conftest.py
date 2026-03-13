from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path

import pytest
from joblib import cpu_count

_REAL_TG_PARALLEL_GATES = ("RUN_REAL_TELEGRAM_SAFE", "RUN_REAL_TELEGRAM_MANUAL")
_AIOSQLITE_SERIAL_FIXTURES = {"cli_db"}
_AIOSQLITE_SERIAL_TOKENS = ("import aiosqlite",)


def _should_force_single_worker(args: list[str]) -> bool:
    if any(os.environ.get(name) == "1" for name in _REAL_TG_PARALLEL_GATES):
        return True
    return any("::" in arg for arg in args)


def pytest_xdist_auto_num_workers(config) -> int:
    if _should_force_single_worker(list(config.args)):
        return 1
    return max(1, cpu_count() - 1)


@lru_cache(maxsize=None)
def _file_requires_aiosqlite_serial(path_str: str) -> bool:
    path = Path(path_str)
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return False
    return any(token in text for token in _AIOSQLITE_SERIAL_TOKENS)


def pytest_collection_modifyitems(items) -> None:
    for item in items:
        if _AIOSQLITE_SERIAL_FIXTURES.intersection(item.fixturenames):
            item.add_marker(pytest.mark.aiosqlite_serial)
            continue
        if _file_requires_aiosqlite_serial(str(item.path)):
            item.add_marker(pytest.mark.aiosqlite_serial)
