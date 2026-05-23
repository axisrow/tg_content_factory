from __future__ import annotations

import os
import re
import sqlite3
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

import pytest
import yaml

from src.database.schema import SCHEMA_SQL
from tests.conftest import _build_real_telegram_sandbox_config


@dataclass(frozen=True)
class CliRealTelegramSandbox:
    source_root: Path
    work_dir: Path
    config_path: Path
    db_path: Path
    session_cache_dir: Path
    api_id: int
    api_hash: str
    phone: str
    session_string: str
    read_channel_username: str | None
    read_channel_id: int | None

    @property
    def repo_root(self) -> Path:
        return self.work_dir

    @property
    def channel_pk(self) -> str | None:
        return "1" if self.read_channel_id is not None else None

    @property
    def channel_ref(self) -> str | None:
        if self.read_channel_username:
            return self.read_channel_username
        if self.read_channel_id is not None:
            return str(self.read_channel_id)
        return None


_SOURCE_ROOT = Path(__file__).resolve().parents[2]
CliEnv = CliRealTelegramSandbox


def _write_sandbox_config(sandbox_dir: Path, cfg) -> tuple[Path, Path, Path]:
    db_path = sandbox_dir / "data" / "tg_search.db"
    session_cache_dir = sandbox_dir / "telegram_sessions"
    config_path = sandbox_dir / "config.yaml"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    session_cache_dir.mkdir(parents=True, exist_ok=True)

    config = {
        "telegram": {"api_id": cfg.api_id, "api_hash": cfg.api_hash},
        "telegram_runtime": {
            "backend_mode": "native",
            "cli_transport": "in_process",
            "session_cache_dir": str(session_cache_dir),
        },
        "database": {"path": str(db_path)},
        "web": {"host": "127.0.0.1", "port": 18080, "password": "cli-real-tg-test"},
        "llm": {"enabled": False, "provider": "openai", "model": "gpt-4o-mini", "api_key": ""},
        "agent": {"model": "", "fallback_model": "", "fallback_api_key": ""},
        "security": {"session_encryption_key": ""},
    }
    config_path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")
    return config_path, db_path, session_cache_dir


def _seed_sandbox_db(db_path: Path, cfg) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.executescript(SCHEMA_SQL)
        conn.execute(
            """
            INSERT INTO accounts (id, phone, session_string, is_primary, is_active, is_premium)
            VALUES (1, ?, ?, 1, 1, 0)
            ON CONFLICT(phone) DO UPDATE SET
                session_string = excluded.session_string,
                is_primary = excluded.is_primary,
                is_active = excluded.is_active
            """,
            (cfg.phone, cfg.session_string),
        )
        if cfg.read_channel_id is not None:
            title = cfg.read_channel_username or f"sandbox channel {cfg.read_channel_id}"
            conn.execute(
                """
                INSERT INTO channels (id, channel_id, title, username, is_active, channel_type)
                VALUES (1, ?, ?, ?, 1, 'channel')
                ON CONFLICT(channel_id) DO UPDATE SET
                    title = excluded.title,
                    username = excluded.username,
                    is_active = excluded.is_active,
                    channel_type = excluded.channel_type
                """,
                (cfg.read_channel_id, title, cfg.read_channel_username),
            )
        conn.commit()


@pytest.fixture(scope="session")
def cli_real_telegram_sandbox(tmp_path_factory: pytest.TempPathFactory) -> CliRealTelegramSandbox:
    try:
        cfg = _build_real_telegram_sandbox_config(os.environ)
    except RuntimeError as exc:
        pytest.skip(str(exc))

    sandbox_dir = tmp_path_factory.mktemp("cli_real_telegram")
    config_path, db_path, session_cache_dir = _write_sandbox_config(sandbox_dir, cfg)
    _seed_sandbox_db(db_path, cfg)

    source_config = _SOURCE_ROOT / "config.yaml"
    source_db = _SOURCE_ROOT / "data" / "tg_search.db"
    assert config_path.resolve() != source_config.resolve()
    assert db_path.resolve() != source_db.resolve()

    return CliRealTelegramSandbox(
        source_root=_SOURCE_ROOT,
        work_dir=sandbox_dir,
        config_path=config_path,
        db_path=db_path,
        session_cache_dir=session_cache_dir,
        api_id=cfg.api_id,
        api_hash=cfg.api_hash,
        phone=cfg.phone,
        session_string=cfg.session_string,
        read_channel_username=cfg.read_channel_username,
        read_channel_id=cfg.read_channel_id,
    )


@pytest.fixture(scope="session")
def cli_env(cli_real_telegram_sandbox: CliRealTelegramSandbox) -> CliEnv:
    return cli_real_telegram_sandbox


def _cli_command(cli_env: CliEnv, args: tuple[str, ...]) -> list[str]:
    return [
        sys.executable,
        "-m",
        "src.main",
        "--config",
        str(cli_env.config_path),
        *args,
    ]


def cli_run_direct(cli_env: CliEnv, *args: str, timeout: float = 20.0) -> subprocess.CompletedProcess:
    """subprocess.run wrapper for cleanup code that must NOT call pytest.skip.

    The shared `run_cli` fixture converts subprocess.TimeoutExpired into
    pytest.skip(). That is correct for the test body, but a Skipped raised
    inside a `finally` block replaces any in-flight AssertionError, so
    cleanup code that uses run_cli silently masks real test failures (and
    leaks whatever resource the cleanup was supposed to release). Use this
    helper for cleanup invocations: TimeoutExpired bubbles up to the caller
    explicitly, no pytest.skip side effect.

    Module-level (not a fixture) so cleanup `finally` blocks can call it
    without depending on fixture resolution order.
    """
    return subprocess.run(  # noqa: S603 — controlled CLI module invocation
        _cli_command(cli_env, args),
        cwd=str(cli_env.repo_root),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=timeout,
        env=_build_cli_env(cli_env),
        check=False,
    )


def _build_cli_env(cli_env: CliEnv) -> dict[str, str]:
    """Compose the env dict for sandboxed CLI subprocesses."""
    env = os.environ.copy()
    existing = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = (
        f"{cli_env.source_root}{os.pathsep}{existing}" if existing else str(cli_env.source_root)
    )
    env["PYTHONSAFEPATH"] = "1"
    env["TG_API_ID"] = str(cli_env.api_id)
    env["TG_API_HASH"] = cli_env.api_hash
    env["TG_BACKEND_MODE"] = "native"
    env["TG_CLI_TRANSPORT"] = "in_process"
    env["TG_SESSION_CACHE_DIR"] = str(cli_env.session_cache_dir)
    return env


@pytest.fixture
def run_cli(cli_env: CliEnv):
    def _run(*args: str, timeout: int = 120) -> subprocess.CompletedProcess:
        try:
            return subprocess.run(
                _cli_command(cli_env, args),
                cwd=str(cli_env.repo_root),
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=timeout,
                env=_build_cli_env(cli_env),
            )
        except subprocess.TimeoutExpired:
            pytest.skip(f"CLI command timed out after {timeout}s: {' '.join(args)}")

    return _run


@pytest.fixture
def run_cli_popen(cli_env: CliEnv):
    """Spawn long-running CLI commands (serve/worker/scheduler start) as Popen.

    Returns a callable that spawns the process and records it for cleanup.
    On test teardown, every spawned process that is still alive is sent
    SIGTERM, then SIGKILL if it doesn't exit within 5s. Use this instead of
    `run_cli` when the command does not return on its own.

    stdout is routed to DEVNULL so a verbose server (uvicorn access logs)
    cannot fill the OS pipe buffer and stall the child mid-SIGTERM.
    stderr is captured so failing tests can surface the tail in their
    assertion messages — but it is drained via communicate() in teardown
    to keep wait() from blocking on a half-full pipe.
    """
    processes: list[subprocess.Popen] = []

    def _spawn(*args: str) -> subprocess.Popen:
        proc = subprocess.Popen(  # noqa: S603 — controlled CLI module invocation
            _cli_command(cli_env, args),
            cwd=str(cli_env.repo_root),
            env=_build_cli_env(cli_env),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        processes.append(proc)
        return proc

    yield _spawn

    for proc in processes:
        if proc.poll() is not None:
            continue
        proc.terminate()
        try:
            # communicate() drains stderr before waiting, so a stderr-noisy
            # process (logs) doesn't stall on pipe backpressure during SIGTERM.
            proc.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            try:
                proc.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                pass


def wait_for_http_200(url: str, *, timeout: float = 15.0, interval: float = 0.5) -> bool:
    """Poll an HTTP endpoint until it returns 200 or the timeout elapses.

    Returns True on success, False on timeout. Uses urllib (stdlib only) to
    avoid pulling httpx/requests into the test environment.
    """
    import urllib.error
    import urllib.request

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=2.0) as resp:
                if resp.status == 200:
                    return True
        except (urllib.error.URLError, ConnectionError, TimeoutError):
            pass
        time.sleep(interval)
    return False


def wait_for_db_row(
    db_path: Path,
    sql: str,
    params: tuple = (),
    *,
    timeout: float = 15.0,
    interval: float = 0.5,
) -> tuple | None:
    """Poll a sqlite DB for a row matching `sql`+`params`. Returns the row or None.

    Uses synchronous sqlite3 so we never collide with an aiosqlite-backed
    connection inside the CLI subprocess (WAL mode allows concurrent readers).
    """
    import sqlite3

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            conn = sqlite3.connect(str(db_path))
            try:
                row = conn.execute(sql, params).fetchone()
                if row is not None:
                    return row
            finally:
                conn.close()
        except sqlite3.Error:
            pass
        time.sleep(interval)
    return None


_FLOOD_WAIT_RE = re.compile(r"FloodWaitError|FLOOD_?WAIT", re.IGNORECASE)
_AUTH_RE = re.compile(
    r"AuthKeyError|AuthKeyUnregistered|session\s+expired|UnauthorizedError", re.IGNORECASE
)
_SILENT_FAILURE_RE = re.compile(
    r"Traceback|ModuleNotFoundError|No connected accounts|No accounts found|"
    r"Could not resolve channel|Error fetching broadcast stats|Failed to initialize|"
    r"Failed to load|RuntimeError",
    re.IGNORECASE,
)


@pytest.fixture
def assert_cli_ok():
    def _assert(result: subprocess.CompletedProcess, *, allow_error_text: bool = False) -> None:
        combined = (result.stdout or "") + "\n" + (result.stderr or "")
        if result.returncode != 0:
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
        if not allow_error_text and _SILENT_FAILURE_RE.search(combined):
            pytest.fail(
                "CLI returned zero but printed a failure-looking message\n"
                f"--- stdout ---\n{result.stdout}\n"
                f"--- stderr ---\n{result.stderr}",
                pytrace=False,
            )

    return _assert


@pytest.fixture
def sandbox_channel(cli_real_telegram_sandbox: CliRealTelegramSandbox) -> tuple[str, str]:
    if cli_real_telegram_sandbox.channel_pk is None or cli_real_telegram_sandbox.read_channel_id is None:
        pytest.skip("REAL_TG_READ_CHANNEL_ID is required for sandbox channel CLI tests")
    return cli_real_telegram_sandbox.channel_pk, str(cli_real_telegram_sandbox.read_channel_id)


@pytest.fixture
def sandbox_channel_username(cli_real_telegram_sandbox: CliRealTelegramSandbox) -> str:
    if not cli_real_telegram_sandbox.read_channel_username:
        pytest.skip("REAL_TG_READ_CHANNEL_USERNAME is required for sandbox username CLI tests")
    username = cli_real_telegram_sandbox.read_channel_username
    return username if username.startswith("@") else f"@{username}"


@pytest.fixture
def sandbox_phone(cli_real_telegram_sandbox: CliRealTelegramSandbox) -> str:
    return cli_real_telegram_sandbox.phone


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
        result = run_cli("pipeline", "runs", pipeline_id, "--limit", "1")
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
