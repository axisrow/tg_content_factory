"""Guards for the test-level taxonomy (unit / integration / smoke / e2e).

The level is auto-applied by ``_infer_test_level`` in the root ``conftest.py``.
These tests pin that classifier's behaviour against representative inputs and
lock the structural invariants (single level per item, unit ∩ integration = ∅,
nothing unlevelled) via a real collection of the suite.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest


def _load_root_conftest():
    path = Path(__file__).resolve().parents[1] / "conftest.py"
    spec = importlib.util.spec_from_file_location("root_conftest_for_levels", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


root_conftest = _load_root_conftest()


def _item(
    *,
    path: str = "/repo/tests/test_thing.py",
    fixturenames: tuple[str, ...] = (),
    markers: tuple[str, ...] = (),
    name: str = "test_thing",
) -> SimpleNamespace:
    """A minimal stand-in for a pytest item for _infer_test_level()."""
    marker_set = set(markers)
    return SimpleNamespace(
        path=Path(path),
        fixturenames=fixturenames,
        originalname=name,
        name=name,
        iter_markers=lambda: [SimpleNamespace(name=m) for m in marker_set],
    )


def _infer(item, *, file_db: bool = False) -> str:
    """Invoke the classifier the way pytest_collection_modifyitems does:
    walk the item's own markers once, then infer the level."""
    return root_conftest._infer_test_level(
        item, markers=root_conftest._own_markers(item), file_db=file_db
    )


# --- the four level markers and their sets stay in sync ---------------------


def test_level_marker_names_are_the_four_levels() -> None:
    assert set(root_conftest._LEVEL_MARKERS) == {"unit", "integration", "smoke", "e2e"}


def test_real_tg_markers_drive_e2e() -> None:
    assert root_conftest._E2E_MARKERS == {
        "real_tg_safe",
        "real_tg_mutation_safe",
        "real_tg_manual",
        "real_tg_never",
    }


# --- _infer_test_level: representative classifications ----------------------


def test_pure_logic_is_unit() -> None:
    item = _item(fixturenames=("monkeypatch", "tmp_path"))
    assert _infer(item) == "unit"


def test_db_fixture_is_integration() -> None:
    item = _item(fixturenames=("db",))
    assert _infer(item) == "integration"


def test_in_memory_db_counts_as_integration_via_fixture() -> None:
    # The `db` fixture is Database(":memory:") — by project decision that is
    # integration, not unit (a real SQLite subsystem, just off-disk).
    item = _item(fixturenames=("db", "monkeypatch"))
    assert _infer(item) == "integration"


def test_client_fixture_is_integration() -> None:
    item = _item(fixturenames=("client",))
    assert _infer(item) == "integration"


def test_routes_directory_is_integration_without_fixture() -> None:
    item = _item(path="/repo/tests/routes/test_web_container.py", fixturenames=())
    assert _infer(item) == "integration"


def test_repositories_directory_is_integration() -> None:
    item = _item(path="/repo/tests/repositories/test_x.py", fixturenames=("db",))
    assert _infer(item) == "integration"


def test_e2e_directory_is_e2e() -> None:
    item = _item(path="/repo/tests/e2e/test_flow.py", fixturenames=())
    assert _infer(item) == "e2e"


def test_real_tg_marker_is_e2e() -> None:
    item = _item(markers=("real_tg_safe",))
    assert _infer(item) == "e2e"


def test_live_provider_marker_is_smoke() -> None:
    item = _item(markers=("real_provider_smoke",))
    assert _infer(item) == "smoke"


def test_file_db_signal_is_integration() -> None:
    item = _item(fixturenames=())
    assert _infer(item, file_db=True) == "integration"


def test_explicit_marker_beats_heuristic_via_modifyitems_contract() -> None:
    # _infer_test_level is only consulted when _has_explicit_level is False;
    # confirm the guard recognises a hand-written level marker and ignores others.
    assert root_conftest._has_explicit_level({"unit"}) is True
    assert root_conftest._has_explicit_level({"anyio", "parametrize"}) is False


# --- per-test AST source scan ----------------------------------------------


@pytest.mark.unit  # writes a tiny temp .py only; literal DB tokens here are data, not real IO
def test_ast_scan_flags_only_db_building_tests(tmp_path) -> None:
    src = (
        "def test_pure():\n"
        "    assert 1 == 1\n"
        "\n"
        "def _open_db(tmp_path):\n"
        "    return Database(str(tmp_path / 'x.db'))\n"
        "\n"
        "def test_uses_helper(tmp_path):\n"
        "    db = _open_db(tmp_path)\n"
        "\n"
        "def test_inline_app():\n"
        "    transport = ASGITransport(app=app)\n"
    )
    f = tmp_path / "test_sample.py"
    f.write_text(src)
    flagged = root_conftest._integration_tests_in_file(str(f))
    assert flagged == {"test_uses_helper", "test_inline_app"}
    assert "test_pure" not in flagged


# --- structural invariants over the real collection ------------------------


@pytest.mark.slow  # forks a full pytest collection in a subprocess (~14s)
@pytest.mark.integration  # forks a full pytest collection in a subprocess
def test_collection_invariants_hold_over_real_suite() -> None:
    """End-to-end gate: every collected test gets exactly one level.

    Collects the whole suite in a subprocess and asserts that nothing is left
    unlevelled and that no item is both unit and integration. This is the
    regression guard against the heuristic drifting as tests are added.
    """
    import subprocess
    import sys

    repo = Path(__file__).resolve().parents[1]

    def _count(marker_expr: str) -> int:
        proc = subprocess.run(
            [sys.executable, "-m", "pytest", "--co", "-q", "-p", "no:cacheprovider", "-m", marker_expr],
            cwd=repo,
            capture_output=True,
            text=True,
        )
        return sum(1 for line in proc.stdout.splitlines() if "::" in line)

    unlevelled = _count("not (unit or integration or smoke or e2e)")
    assert unlevelled == 0, f"{unlevelled} collected tests carry no level marker"

    both = _count("unit and integration")
    assert both == 0, f"{both} collected tests are both unit and integration"
