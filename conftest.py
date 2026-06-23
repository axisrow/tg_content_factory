from __future__ import annotations

import ast
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

# --- Test-level taxonomy (unit / integration / smoke / e2e) ----------------
# A second classification axis layered on top of the existing real_tg_* /
# aiosqlite_serial markers. The level is inferred automatically at collection
# time so ~5000 tests need no manual annotation; an explicit level marker on a
# test/module always wins over the heuristic (see _has_explicit_level below).
_LEVEL_MARKERS = frozenset({"unit", "integration", "smoke", "e2e"})

# Markers that classify a test as e2e (long, full, real-surface): the real
# Telegram policy markers all drive live end-to-end CLI/API flows.
_E2E_MARKERS = frozenset(
    {"real_tg_safe", "real_tg_mutation_safe", "real_tg_manual", "real_tg_never"}
)
# Live API smoke markers — opt-in, gated, exercise a real provider/Codex path.
_SMOKE_MARKERS = frozenset({"real_provider_smoke", "codex_cli_live", "codex_image_live"})

# Explicit allow-list of integration-indicating fixtures. Kept strict on
# purpose: autouse plumbing (telethon_cli_spy, native_auth_spy, _enforce_cli_
# transport, vcr, block_network, tmp_path, …) shows up in every test's
# fixturenames and must NOT be treated as an integration signal. Only fixtures
# that stand up a real in-process subsystem belong here. Repository fixtures
# transitively depend on `db`, so listing `db` covers all of tests/repositories.
_INTEGRATION_FIXTURES = frozenset(
    {
        "db",
        "cli_db",
        "cli_env",
        "client",
        "web_client",
        "route_client",
        "web_mode_client",
        "base_app",
        "web_mode_app",
        "real_pool_harness_factory",
        "real_telegram_sandbox",
        "cli_real_cli_env",
        "pipeline_client",
    }
)
# Directory segments whose tests are integration by location regardless of the
# fixtures they happen to request (e.g. web-container tests that touch no db).
_INTEGRATION_PATH_SEGMENTS = ("/tests/routes/", "/tests/repositories/")
_E2E_PATH_SEGMENTS = ("/tests/e2e/",)
# Source tokens that unambiguously stand up a real in-process subsystem inside
# the test body (not via an allow-listed fixture). Kept narrow on purpose: every
# token names a concrete real-IO constructor/entrypoint that never appears in a
# pure-mock test (mocks patch these by string, they don't call them literally).
# A bare ``Database(`` is deliberately excluded — it matches in-memory
# (``:memory:``) unit tests; only the file-backed constructions are listed.
_INTEGRATION_SOURCE_TOKENS = (
    # FastAPI app / web container driven over ASGI
    "ASGITransport",
    "build_web_app",
    "build_web_container",
    # file-backed SQLite built inline (no cli_db / aiosqlite import to trip the
    # aiosqlite_serial signal)
    "Database(str(tmp_path",
    "Database(db_path",
    "Database(config.database",
    "DatabaseConfig(path=str(tmp_path",
    "sqlite3.connect",
    "aiosqlite.connect",
    "open_connection(",
    # real subprocess / session-file subsystems
    "StdioServerParameters",
    "SessionMaterializer(",
    # real project-config load from a written YAML file
    "load_config(",
)


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
    # On a dedicated CI runner (GitHub Actions always sets ``CI``) the host is ours
    # alone, so the load-aware throttle below — which reserves a core and subtracts
    # the rolling load average — only wastes parallelism (it caps a 4-vCPU runner to
    # ~2 workers). Use every core there; xdist workers here are asyncio/sqlite and
    # IO/await-bound, so oversubscription is cheap. Local dev stays load-aware so a
    # busy laptop isn't hogged (#944). Match a real CI flag, not just any non-empty
    # value, so a stray ``CI=false``/``CI=0`` in a dev shell doesn't disable the
    # throttle (review note, #974).
    if os.environ.get("CI", "").strip().lower() in ("1", "true", "yes"):
        return max(1, cpu_count)
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


def _arg_dir_key(arg: str) -> str:
    """Directory portion of a collection arg (path / ``file.py::node`` id).

    ``tests/routes/test_x.py::test_foo`` → ``tests/routes`` (drop the node id,
    then the filename). A bare directory (``tests/routes``) or separator-less
    token maps to itself — only a ``.py`` final segment is treated as a file.
    Used only to *group* args by directory, never to reorder within a directory.
    """
    head = arg.split("::", 1)[0]
    if not head.endswith(".py") or "/" not in head:
        return head
    return head.rsplit("/", 1)[0]


def pytest_collection(session) -> None:
    """Group command-line collection args by directory before collection (#1005).

    pytest builds one ``Package`` collector per directory in the order each first
    appears among the collection args. When args from sibling directories are
    *interleaved* — e.g. selecting ``tests/routes/a.py tests/b.py
    tests/routes/c.py`` — pytest enters ``tests/routes``, leaves it for
    ``tests``, then re-enters ``tests/routes``. On that second entry the
    duplicate ``Package``/``Directory`` node is built with a corrupted ``nodeid``
    (the module path instead of ``tests/routes``), so the package's
    ``conftest.py`` fixtures no longer match the re-entered file's items: every
    test there fails setup with ``fixture '<name>' not found`` even though the
    file passes in isolation.

    The reorder must happen here, before collection — it changes how the
    collection tree is built. Sorting the already-collected ``items`` in
    ``modifyitems`` is too late: the corrupted ``Package`` nodes already exist.
    CI never hits the bug (``--dist=loadfile`` shards whole files across workers,
    and a plain ``pytest tests/`` walks each directory contiguously); it is the
    ad-hoc local mixed-file run from the issue that interleaves.

    Make same-directory args contiguous so each package is collected exactly
    once. Stable on two axes — directories keep their first-appearance order and
    args keep their order within a directory — so an already-contiguous
    invocation is a no-op and the run order is otherwise untouched.
    """
    args = list(session.config.args)
    if len(args) < 3:
        # Fewer than three args cannot interleave (need A, foreign, A again),
        # so there is nothing to regroup.
        return

    order: dict[str, int] = {}
    for arg in args:
        order.setdefault(_arg_dir_key(arg), len(order))
    grouped = sorted(args, key=lambda arg: order[_arg_dir_key(arg)])
    if grouped != args:
        session.config.args = grouped


@lru_cache(maxsize=None)
def _read_test_source(path_str: str) -> str:
    """Cached read of a test file's source (shared by the collection scans).

    Returns "" if the file can't be read, so callers treat it as a no-signal
    file. Cached per path so each file is read at most once per session even
    though the collection hook runs per test item.
    """
    try:
        return Path(path_str).read_text(encoding="utf-8")
    except OSError:
        return ""


def _file_requires_aiosqlite_serial(path_str: str) -> bool:
    text = _read_test_source(path_str)
    return any(
        bool(re.search(r"^" + re.escape(token), text, re.MULTILINE))
        for token in _AIOSQLITE_SERIAL_TOKENS
    )


def _own_markers(item) -> set[str]:
    """Marker names declared on the item or its module (pre-heuristic)."""
    return {marker.name for marker in item.iter_markers()}


def _has_explicit_level(markers: set[str]) -> bool:
    """True if a hand-written level marker is present among ``markers``.

    Such a marker (e.g. an explicit @pytest.mark.unit) was authored on the test
    and must win over the heuristic. Takes the pre-walked marker-name set so the
    caller walks iter_markers() once for both this guard and inference.
    """
    return bool(markers & _LEVEL_MARKERS)


def _segment_is_integration(segment: str, db_helpers: frozenset[str]) -> bool:
    """True if a source segment builds a real subsystem directly or via helper."""
    if any(token in segment for token in _INTEGRATION_SOURCE_TOKENS):
        return True
    return any(f"{helper}(" in segment for helper in db_helpers)


@lru_cache(maxsize=None)
def _integration_tests_in_file(path_str: str) -> frozenset[str]:
    """Names of test functions that stand up a real subsystem in their own body.

    Per-test (not per-file): a heuristic for integration tests that construct an
    app/DB/subprocess directly in the test body — so no allow-listed fixture
    appears in fixturenames. A test counts when an integration token appears in
    its own source span, or it calls a module-level helper whose body builds one
    (the common ``_open_db(tmp_path)`` / ``_make_db(path)`` pattern). Pure-mock
    tests in the same file stay unit. File-backed SQLite via cli_db / raw
    aiosqlite is already handled by the aiosqlite_serial signal.
    """
    source = _read_test_source(path_str)
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return frozenset()
    lines = source.splitlines()

    def span(node: ast.FunctionDef | ast.AsyncFunctionDef) -> str:
        return "\n".join(lines[node.lineno - 1 : node.end_lineno or node.lineno])

    funcs: list[ast.FunctionDef | ast.AsyncFunctionDef] = [
        node
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    ]
    db_helpers = frozenset(
        node.name
        for node in funcs
        if not node.name.startswith("test")
        and any(token in span(node) for token in _INTEGRATION_SOURCE_TOKENS)
    )
    return frozenset(
        node.name
        for node in funcs
        if node.name.startswith("test") and _segment_is_integration(span(node), db_helpers)
    )


def _infer_test_level(item, *, markers: set[str], file_db: bool) -> str:
    """Classify an item as e2e / smoke / integration / unit.

    First match wins, in risk order: live end-to-end flows, then live API
    smoke, then in-process subsystem integration, else pure unit. ``markers``
    is the item's own marker-name set (computed once by the caller) and
    ``file_db`` is the already-computed aiosqlite_serial signal — both threaded
    in to avoid re-walking markers / re-reading the file.
    """
    path = item.path.as_posix()

    if any(seg in path for seg in _E2E_PATH_SEGMENTS) or markers & _E2E_MARKERS:
        return "e2e"
    if markers & _SMOKE_MARKERS:
        return "smoke"
    if any(seg in path for seg in _INTEGRATION_PATH_SEGMENTS):
        return "integration"
    if _INTEGRATION_FIXTURES.intersection(item.fixturenames):
        return "integration"
    if file_db:
        return "integration"
    # Per-test source heuristic: this test builds a real subsystem in its own
    # body (no fixture, no file DB import). originalname is the function name
    # without the parametrize suffix; fall back to name for safety.
    test_name = getattr(item, "originalname", None) or item.name
    if test_name in _integration_tests_in_file(str(item.path)):
        return "integration"
    return "unit"


def pytest_collection_modifyitems(items) -> None:
    for item in items:
        needs_serial = bool(
            _AIOSQLITE_SERIAL_FIXTURES.intersection(item.fixturenames)
            or _file_requires_aiosqlite_serial(str(item.path))
        )
        if needs_serial:
            item.add_marker(pytest.mark.aiosqlite_serial)
            item.add_marker(pytest.mark.xdist_group(name="aiosqlite_serial"))

        # Layer the test-level taxonomy on top, but never override an explicit
        # hand-written level marker (the curated smoke set + Phase-3 overrides).
        # markers is walked once here and reused for both the guard and inference.
        own_markers = _own_markers(item)
        if not _has_explicit_level(own_markers):
            level = _infer_test_level(item, markers=own_markers, file_db=needs_serial)
            item.add_marker(getattr(pytest.mark, level))
