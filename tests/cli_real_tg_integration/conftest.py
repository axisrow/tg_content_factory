from __future__ import annotations

import os
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

import pytest
import yaml

REPO_ROOT_ENV = "TG_CONTENT_FACTORY_ROOT"


@dataclass(frozen=True)
class CliEnv:
    repo_root: Path
    config_path: Path
    db_path: Path


def _detect_repo_root() -> Path:
    """Return the project root where config.yaml lives.

    Priority: TG_CONTENT_FACTORY_ROOT env var → pytest invocation cwd → walk up
    from this file looking for config.yaml outside any worktree.
    """
    env_root = os.environ.get(REPO_ROOT_ENV)
    if env_root:
        return Path(env_root).resolve()

    cwd = Path.cwd().resolve()
    if (cwd / "config.yaml").exists() and "worktrees" not in cwd.parts:
        return cwd

    here = Path(__file__).resolve()
    for parent in here.parents:
        if (parent / "config.yaml").exists() and "worktrees" not in parent.parts:
            return parent

    return cwd


@pytest.fixture(scope="session")
def cli_env() -> CliEnv:
    root = _detect_repo_root()
    config_path = root / "config.yaml"
    if not config_path.exists():
        pytest.skip(
            f"config.yaml not found under {root}. Set {REPO_ROOT_ENV} to the project root."
        )

    try:
        cfg = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError as exc:
        pytest.skip(f"failed to parse {config_path}: {exc}")

    db_rel = (cfg.get("database") or {}).get("path")
    if not db_rel:
        pytest.skip(f"{config_path} has no database.path entry")

    db_path = (root / db_rel).resolve()
    if not db_path.exists():
        pytest.skip(f"database file not found at {db_path}")
    if db_path.stat().st_size == 0:
        pytest.skip(f"database file at {db_path} is empty")

    return CliEnv(repo_root=root, config_path=config_path, db_path=db_path)


_WORKTREE_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def run_cli(cli_env: CliEnv):
    def _run(*args: str, timeout: int = 120) -> subprocess.CompletedProcess:
        # cwd = основной репо (там config.yaml + data/). PYTHONPATH ведёт на
        # текущий чекаут (worktree или main), а PYTHONSAFEPATH=1 убирает cwd из
        # sys.path → subprocess грузит `src/` из тестируемого кода, а не из
        # живого репозитория по cwd. Без этого worktree-правки в src/ невидимы.
        env = os.environ.copy()
        existing = env.get("PYTHONPATH", "")
        env["PYTHONPATH"] = (
            f"{_WORKTREE_ROOT}{os.pathsep}{existing}" if existing else str(_WORKTREE_ROOT)
        )
        env["PYTHONSAFEPATH"] = "1"
        return subprocess.run(
            [sys.executable, "-m", "src.main", *args],
            cwd=str(cli_env.repo_root),
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=timeout,
            env=env,
        )

    return _run


_FLOOD_WAIT_RE = re.compile(r"FloodWaitError|FLOOD_?WAIT", re.IGNORECASE)
_AUTH_RE = re.compile(
    r"AuthKeyError|AuthKeyUnregistered|session\s+expired|UnauthorizedError", re.IGNORECASE
)


@pytest.fixture
def assert_cli_ok():
    def _assert(result: subprocess.CompletedProcess) -> None:
        # FLOOD_WAIT и auth-проблемы — это поводы для skip только если CLI упал.
        # При успешном returncode упоминание «flood wait» в выводе (например, в
        # заголовке таблицы `account flood-status`) — это легитимный текст, не ошибка.
        if result.returncode != 0:
            combined = (result.stdout or "") + "\n" + (result.stderr or "")
            if _FLOOD_WAIT_RE.search(combined):
                pytest.skip("Telegram FLOOD_WAIT; retry later")
            if _AUTH_RE.search(combined):
                pytest.skip("Telegram session not authorized; re-auth account")
            pytest.fail(
                f"CLI exited with {result.returncode}\n"
                f"--- stdout ---\n{result.stdout}\n"
                f"--- stderr ---\n{result.stderr}",
                pytrace=False,
            )

    return _assert


_CHANNEL_LIST_ROW_RE = re.compile(r"^\s*(\d+)\s+(-?\d+)\s+", re.MULTILINE)


@pytest.fixture
def discover_first_channel(run_cli, assert_cli_ok):
    """Run `channel list` and return (pk, channel_id) for the first row, or skip."""

    def _discover() -> tuple[str, str]:
        result = run_cli("channel", "list")
        assert_cli_ok(result)
        match = _CHANNEL_LIST_ROW_RE.search(result.stdout)
        if not match:
            pytest.skip("no channels in data.db — `channel list` returned no rows")
        return match.group(1), match.group(2)

    return _discover


_DIALOG_USERNAME_RE = re.compile(r"@([A-Za-z0-9_]{4,})")


@pytest.fixture
def discover_first_dialog_username(run_cli, assert_cli_ok):
    """Run `dialogs list` and return the first @username, or skip."""

    def _discover() -> str:
        result = run_cli("dialogs", "list")
        assert_cli_ok(result)
        match = _DIALOG_USERNAME_RE.search(result.stdout)
        if not match:
            pytest.skip("no @username in `dialogs list` output")
        return "@" + match.group(1)

    return _discover


_ACCOUNT_LIST_ROW_RE = re.compile(
    r"^\s*\d+\s+(\+?\d{6,})\s",
    re.MULTILINE,
)


@pytest.fixture
def discover_first_phone(run_cli, assert_cli_ok):
    """Run `account list` and return the first phone, or skip."""

    def _discover() -> str:
        result = run_cli("account", "list")
        assert_cli_ok(result)
        match = _ACCOUNT_LIST_ROW_RE.search(result.stdout)
        if not match:
            pytest.skip("no accounts in data.db — `account list` returned no rows")
        return match.group(1)

    return _discover


# Таблицы pipeline/search-query/runs все печатают первой колонкой числовой ID:
# заголовки начинаются с букв ("ID"), а строки — с цифр; regex отсекает шапку.
_LEADING_INT_ROW_RE = re.compile(r"^\s*(\d+)\s+\S", re.MULTILINE)


@pytest.fixture
def discover_first_pipeline_id(run_cli, assert_cli_ok):
    """Run `pipeline list` and return the first pipeline id, or skip."""

    def _discover() -> str:
        result = run_cli("pipeline", "list")
        assert_cli_ok(result)
        match = _LEADING_INT_ROW_RE.search(result.stdout)
        if not match:
            pytest.skip("no pipelines — `pipeline list` returned no rows")
        return match.group(1)

    return _discover


@pytest.fixture
def discover_first_run_id(run_cli, assert_cli_ok, discover_first_pipeline_id):
    """Run `pipeline runs <pipeline_id>` and return the first run id, or skip."""

    def _discover() -> str:
        pipeline_id = discover_first_pipeline_id()
        result = run_cli("pipeline", "runs", pipeline_id)
        assert_cli_ok(result)
        match = _LEADING_INT_ROW_RE.search(result.stdout)
        if not match:
            pytest.skip(f"no runs for pipeline id={pipeline_id}")
        return match.group(1)

    return _discover


@pytest.fixture
def discover_first_search_query_id(run_cli, assert_cli_ok):
    """Run `search-query list` and return the first search query id, or skip."""

    def _discover() -> str:
        result = run_cli("search-query", "list")
        assert_cli_ok(result)
        match = _LEADING_INT_ROW_RE.search(result.stdout)
        if not match:
            pytest.skip("no search queries — `search-query list` returned no rows")
        return match.group(1)

    return _discover


# `agent threads` печатает строки вида `[<id>] <title>  (<created_at>)`.
_AGENT_THREAD_ROW_RE = re.compile(r"^\[(\d+)\]", re.MULTILINE)


@pytest.fixture
def discover_first_agent_thread_id(run_cli, assert_cli_ok):
    """Run `agent threads` and return the first thread id, or skip."""

    def _discover() -> str:
        result = run_cli("agent", "threads")
        assert_cli_ok(result)
        match = _AGENT_THREAD_ROW_RE.search(result.stdout)
        if not match:
            pytest.skip("no agent threads — `agent threads` returned no rows")
        return match.group(1)

    return _discover
