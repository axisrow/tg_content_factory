from __future__ import annotations

import importlib.util
from pathlib import Path
from types import SimpleNamespace


def _load_root_conftest():
    path = Path(__file__).resolve().parents[1] / "conftest.py"
    spec = importlib.util.spec_from_file_location("root_conftest_for_tests", path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


root_conftest = _load_root_conftest()


def _config(args: list[str]) -> SimpleNamespace:
    return SimpleNamespace(args=args)


def _set_cpu_state(monkeypatch, *, cpu_count: int, load_average: float) -> None:
    monkeypatch.setattr(root_conftest.os, "cpu_count", lambda: cpu_count)
    monkeypatch.setattr(
        root_conftest.os,
        "getloadavg",
        lambda: (load_average, load_average, load_average),
        raising=False,
    )


def test_xdist_auto_workers_are_capped_by_default(monkeypatch) -> None:
    monkeypatch.delenv("TGCF_PYTEST_XDIST_WORKERS", raising=False)
    _set_cpu_state(monkeypatch, cpu_count=16, load_average=0.0)

    assert root_conftest.pytest_xdist_auto_num_workers(_config(["tests"])) == 4


def test_xdist_auto_workers_reduce_worker_count_when_cpu_is_busy(monkeypatch) -> None:
    monkeypatch.setenv("TGCF_PYTEST_XDIST_WORKERS", "8")
    _set_cpu_state(monkeypatch, cpu_count=16, load_average=12.8)

    assert root_conftest.pytest_xdist_auto_num_workers(_config(["tests"])) == 2


def test_xdist_auto_workers_round_fractional_load_up(monkeypatch) -> None:
    _set_cpu_state(monkeypatch, cpu_count=4, load_average=0.95)

    assert root_conftest._xdist_available_workers_for_load(4) == 2


def test_xdist_auto_workers_keep_one_worker_when_cpu_is_saturated(monkeypatch) -> None:
    monkeypatch.setenv("TGCF_PYTEST_XDIST_WORKERS", "8")
    _set_cpu_state(monkeypatch, cpu_count=8, load_average=20.0)

    assert root_conftest.pytest_xdist_auto_num_workers(_config(["tests"])) == 1


def test_xdist_auto_workers_can_be_capped_by_env(monkeypatch) -> None:
    monkeypatch.setenv("TGCF_PYTEST_XDIST_WORKERS", "2")
    _set_cpu_state(monkeypatch, cpu_count=16, load_average=0.0)

    assert root_conftest.pytest_xdist_auto_num_workers(_config(["tests"])) == 2


def test_xdist_auto_workers_never_exceed_available_cpu(monkeypatch) -> None:
    monkeypatch.setenv("TGCF_PYTEST_XDIST_WORKERS", "8")
    _set_cpu_state(monkeypatch, cpu_count=2, load_average=0.0)

    assert root_conftest.pytest_xdist_auto_num_workers(_config(["tests"])) == 1


def test_xdist_auto_workers_use_default_cap_for_invalid_env(monkeypatch) -> None:
    monkeypatch.setenv("TGCF_PYTEST_XDIST_WORKERS", "invalid")
    _set_cpu_state(monkeypatch, cpu_count=16, load_average=0.0)

    assert root_conftest.pytest_xdist_auto_num_workers(_config(["tests"])) == 4


def test_xdist_auto_workers_force_single_worker_for_nodeid(monkeypatch) -> None:
    monkeypatch.setenv("TGCF_PYTEST_XDIST_WORKERS", "4")
    _set_cpu_state(monkeypatch, cpu_count=16, load_average=0.0)

    assert root_conftest.pytest_xdist_auto_num_workers(_config(["tests/test_cli.py::test_one"])) == 1


def test_xdist_auto_workers_force_single_worker_for_real_tg_gate(monkeypatch) -> None:
    monkeypatch.setenv("RUN_REAL_TELEGRAM_SAFE", "1")
    monkeypatch.setenv("TGCF_PYTEST_XDIST_WORKERS", "4")
    _set_cpu_state(monkeypatch, cpu_count=16, load_average=0.0)

    assert root_conftest.pytest_xdist_auto_num_workers(_config(["tests"])) == 1
