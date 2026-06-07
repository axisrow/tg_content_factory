from __future__ import annotations

import math
import os
import re
from functools import lru_cache
from pathlib import Path

import pytest

_REAL_TG_PARALLEL_GATES = (
    "RUN_REAL_TELEGRAM_SAFE",
    "RUN_REAL_TELEGRAM_MUTATION_SAFE",
    "RUN_REAL_TELEGRAM_MANUAL",
)
_AIOSQLITE_SERIAL_FIXTURES = {"cli_db"}
_AIOSQLITE_SERIAL_TOKENS = ("import aiosqlite",)
_DEFAULT_XDIST_AUTO_WORKERS = 4
_XDIST_WORKER_CAP_ENV = "TGCF_PYTEST_XDIST_WORKERS"


def _should_force_single_worker(args: list[str]) -> bool:
    if any(os.environ.get(name) == "1" for name in _REAL_TG_PARALLEL_GATES):
        return True
    return any("::" in arg for arg in args)


def _xdist_auto_worker_cap() -> int:
    raw_value = os.environ.get(_XDIST_WORKER_CAP_ENV)
    if raw_value is None:
        return _DEFAULT_XDIST_AUTO_WORKERS
    try:
        parsed_value = int(raw_value)
    except ValueError:
        return _DEFAULT_XDIST_AUTO_WORKERS
    return max(1, parsed_value)


def _xdist_available_workers_for_load(cpu_count: int) -> int:
    try:
        current_load = os.getloadavg()[0]
    except (AttributeError, OSError):
        current_load = 0.0
    busy_cores = max(0, math.ceil(current_load))
    return max(1, cpu_count - busy_cores - 1)


def pytest_xdist_auto_num_workers(config) -> int:
    if _should_force_single_worker(list(config.args)):
        return 1
    cpu_count = os.cpu_count() or 1
    available_workers = _xdist_available_workers_for_load(cpu_count)
    return min(available_workers, _xdist_auto_worker_cap())


@lru_cache(maxsize=None)
def _file_requires_aiosqlite_serial(path_str: str) -> bool:
    path = Path(path_str)
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return False
    return any(
        bool(re.search(r"^" + re.escape(token), text, re.MULTILINE))
        for token in _AIOSQLITE_SERIAL_TOKENS
    )


def pytest_collection_modifyitems(items) -> None:
    for item in items:
        needs_serial = _AIOSQLITE_SERIAL_FIXTURES.intersection(
            item.fixturenames
        ) or _file_requires_aiosqlite_serial(str(item.path))
        if needs_serial:
            item.add_marker(pytest.mark.aiosqlite_serial)
            item.add_marker(pytest.mark.xdist_group(name="aiosqlite_serial"))
