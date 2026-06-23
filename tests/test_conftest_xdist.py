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


def _session(args: list[str], *, maxfail: int = 0) -> SimpleNamespace:
    config = _config(list(args))
    # pytest_collection reads config.option.maxfail to skip under fail-fast.
    config.option = SimpleNamespace(maxfail=maxfail)
    return SimpleNamespace(config=config)


def _collect(args: list[str], *, maxfail: int = 0) -> list[str]:
    """Run pytest_collection over ``args`` and return the (possibly) regrouped args."""
    session = _session(args, maxfail=maxfail)
    root_conftest.pytest_collection(session)
    return list(session.config.args)


def _set_cpu_state(monkeypatch, *, cpu_count: int, load_average: float) -> None:
    # These cases exercise the dev-laptop load-aware path; ensure CI is unset so
    # the CI "use all cores" short-circuit (#944) does not mask the load logic.
    monkeypatch.delenv("CI", raising=False)
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


def test_xdist_ci_ignores_load_and_uses_all_cores(monkeypatch) -> None:
    # On CI the host is dedicated, so the load throttle is skipped and every core
    # is available — even under a high reported load (#944).
    _set_cpu_state(monkeypatch, cpu_count=4, load_average=8.0)
    monkeypatch.setenv("CI", "true")

    assert root_conftest._xdist_available_workers_for_load(4) == 4


def test_xdist_ci_reaches_full_cap(monkeypatch) -> None:
    # `-n auto` on a 4-vCPU runner reaches the default cap of 4 (vs ~2 with the
    # dev load throttle) — the headline CI speedup.
    monkeypatch.delenv("TGCF_PYTEST_XDIST_WORKERS", raising=False)
    _set_cpu_state(monkeypatch, cpu_count=4, load_average=8.0)
    monkeypatch.setenv("CI", "true")

    assert root_conftest.pytest_xdist_auto_num_workers(_config(["tests"])) == 4


def test_xdist_ci_still_respects_env_cap(monkeypatch) -> None:
    # The CI path uses all cores for "available" but the explicit worker cap still
    # applies (min of the two).
    _set_cpu_state(monkeypatch, cpu_count=16, load_average=0.0)
    monkeypatch.setenv("CI", "true")
    monkeypatch.setenv("TGCF_PYTEST_XDIST_WORKERS", "3")

    assert root_conftest.pytest_xdist_auto_num_workers(_config(["tests"])) == 3


# --- collection arg regrouping (#1005) -------------------------------------
# A cross-file fixture leak: when collection args from sibling directories are
# interleaved on the command line, pytest re-enters a package and drops that
# package's conftest fixtures, so every test in the re-entered file fails setup
# with ``fixture '<name>' not found``. pytest_collection regroups same-directory
# args so each package is collected once. The reorder must run before collection
# (it shapes the collection tree) — sorting items in modifyitems is too late.
# These pin that behaviour at the unit level; see
# test_collection_interleave_regression.py for the end-to-end subprocess repro.


def test_arg_dir_key_strips_node_id_and_filename() -> None:
    assert root_conftest._arg_dir_key("tests/routes/test_x.py::test_foo") == "tests/routes"
    assert root_conftest._arg_dir_key("tests/routes/test_x.py") == "tests/routes"
    assert root_conftest._arg_dir_key("tests/test_y.py") == "tests"
    # A bare directory or separator-less token maps to itself.
    assert root_conftest._arg_dir_key("tests") == "tests"
    assert root_conftest._arg_dir_key("tests/routes") == "tests/routes"


def test_collection_de_interleaves_sibling_directories() -> None:
    # The issue repro: a foreign-directory file splits two routes files.
    args = [
        "tests/routes/test_agent_lazyload.py",
        "tests/test_notifier_delivery_paths.py",
        "tests/routes/test_analytics_routes_channel_trends.py",
    ]
    assert _collect(args) == [
        "tests/routes/test_agent_lazyload.py",
        "tests/routes/test_analytics_routes_channel_trends.py",
        "tests/test_notifier_delivery_paths.py",
    ]


def test_collection_preserves_directory_first_appearance_order() -> None:
    # tests/ appears before tests/routes/, so its group must stay first — we
    # only de-interleave, we don't sort directories alphabetically.
    args = [
        "tests/test_b.py",
        "tests/routes/test_a.py",
        "tests/test_c.py",
        "tests/routes/test_d.py",
    ]
    assert _collect(args) == [
        "tests/test_b.py",
        "tests/test_c.py",
        "tests/routes/test_a.py",
        "tests/routes/test_d.py",
    ]


def test_collection_keeps_arg_order_within_a_directory() -> None:
    # Same directory throughout: nothing to de-interleave, order is untouched.
    args = ["tests/routes/test_z.py", "tests/routes/test_a.py", "tests/routes/test_m.py"]
    assert _collect(args) == args


def test_collection_noop_for_fewer_than_three_args() -> None:
    # Can't interleave (A, foreign, A again) with fewer than three args.
    assert _collect(["tests/routes/test_b.py", "tests/test_a.py"]) == [
        "tests/routes/test_b.py",
        "tests/test_a.py",
    ]


def test_collection_noop_for_default_testpaths() -> None:
    # The full-suite invocation passes a single ``tests`` arg — untouched.
    assert _collect(["tests"]) == ["tests"]


def test_collection_handles_node_id_args() -> None:
    args = [
        "tests/routes/test_a.py::test_one",
        "tests/test_b.py::test_two",
        "tests/routes/test_c.py::test_three",
    ]
    assert _collect(args) == [
        "tests/routes/test_a.py::test_one",
        "tests/routes/test_c.py::test_three",
        "tests/test_b.py::test_two",
    ]


def test_collection_is_skipped_under_fail_fast() -> None:
    # With -x / --maxfail, the order files run in decides which tests execute
    # before pytest stops, so we must NOT reorder — leave the user's exact
    # command untouched and behave like vanilla pytest (#1008).
    interleaved = [
        "tests/routes/test_a.py",
        "tests/test_b.py",
        "tests/routes/test_c.py",
    ]
    # maxfail=1 is what -x sets; any truthy maxfail disables the regroup.
    assert _collect(interleaved, maxfail=1) == interleaved
    assert _collect(interleaved, maxfail=3) == interleaved
    # maxfail=0 (no fail-fast) still regroups.
    assert _collect(interleaved, maxfail=0) == [
        "tests/routes/test_a.py",
        "tests/routes/test_c.py",
        "tests/test_b.py",
    ]
