from __future__ import annotations

import os
import re
import sqlite3
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import pytest

from src.config import load_config

CLI_REAL_TG_LIVE_GATE_ENV = "RUN_CLI_REAL_TG_LIVE"
CLI_REAL_TG_ROOT_ENV = "CLI_REAL_TG_ROOT"
CLI_REAL_TG_CONFIG_ENV = "CLI_REAL_TG_CONFIG"


@dataclass(frozen=True)
class CliRealCliEnv:
    source_root: Path
    live_root: Path
    config_path: Path
    db_path: Path
    web_port: int
    phones: tuple[str, ...]
    channel_pk: str | None
    channel_id: int | None
    channel_username: str | None

    @property
    def repo_root(self) -> Path:
        return self.live_root

    @property
    def primary_phone(self) -> str:
        if not self.phones:
            pytest.skip("live DB has no connected Telegram accounts")
        return self.phones[0]

    @property
    def channel_ref(self) -> str | None:
        if self.channel_username:
            return self.channel_username
        if self.channel_id is not None:
            return str(self.channel_id)
        return None

    @property
    def pid_path(self) -> Path:
        return self.db_path.with_suffix(".pid")


_SOURCE_ROOT = Path(__file__).resolve().parents[2]
CliEnv = CliRealCliEnv


def _resolve_live_root() -> Path:
    return Path(os.environ.get(CLI_REAL_TG_ROOT_ENV, _SOURCE_ROOT)).expanduser().resolve()


def _resolve_config_path(live_root: Path) -> Path:
    configured = os.environ.get(CLI_REAL_TG_CONFIG_ENV)
    if configured:
        path = Path(configured).expanduser()
        return path if path.is_absolute() else (live_root / path).resolve()
    return live_root / "config.yaml"


def _resolve_db_path(live_root: Path, db_path: str) -> Path:
    path = Path(db_path).expanduser()
    return path if path.is_absolute() else (live_root / path).resolve()


def _fetch_live_accounts(db_path: Path) -> tuple[str, ...]:
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT phone
            FROM accounts
            WHERE COALESCE(is_active, 1) = 1
              AND COALESCE(session_string, '') != ''
            ORDER BY COALESCE(is_primary, 0) DESC, id ASC
            """
        ).fetchall()
    return tuple(str(row[0]) for row in rows if row[0])


def _fetch_live_channel(db_path: Path) -> tuple[str | None, int | None, str | None]:
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT id, channel_id, username
            FROM channels
            WHERE COALESCE(is_active, 1) = 1
            ORDER BY id ASC
            LIMIT 1
            """
        ).fetchone()
    if row is None:
        return None, None, None
    pk = str(row[0]) if row[0] is not None else None
    channel_id = int(row[1]) if row[1] is not None else None
    username = str(row[2]) if row[2] else None
    return pk, channel_id, username


@pytest.fixture(scope="session")
def cli_real_cli_env() -> CliRealCliEnv:
    if os.environ.get(CLI_REAL_TG_LIVE_GATE_ENV) != "1":
        pytest.skip(f"live CLI tests disabled; set {CLI_REAL_TG_LIVE_GATE_ENV}=1 to run them")

    live_root = _resolve_live_root()
    config_path = _resolve_config_path(live_root)
    if not config_path.exists():
        pytest.skip(
            f"live CLI config not found at {config_path}; set {CLI_REAL_TG_CONFIG_ENV} or {CLI_REAL_TG_ROOT_ENV}"
        )

    config = load_config(config_path)
    if config.telegram.api_id == 0 or not config.telegram.api_hash:
        pytest.skip("live CLI config has no Telegram api_id/api_hash")

    db_path = _resolve_db_path(live_root, config.database.path)
    if not db_path.exists():
        pytest.skip(f"live CLI database not found at {db_path}")
    if db_path.stat().st_size == 0:
        pytest.skip(f"live CLI database at {db_path} is empty")

    try:
        phones = _fetch_live_accounts(db_path)
    except sqlite3.Error as exc:
        pytest.skip(f"failed to read live CLI accounts from {db_path}: {exc}")
    if not phones:
        pytest.skip("live CLI database has no active connected Telegram accounts")

    try:
        channel_pk, channel_id, channel_username = _fetch_live_channel(db_path)
    except sqlite3.Error:
        channel_pk, channel_id, channel_username = None, None, None

    return CliRealCliEnv(
        source_root=_SOURCE_ROOT,
        live_root=live_root,
        config_path=config_path,
        db_path=db_path,
        web_port=int(config.web.port),
        phones=phones,
        channel_pk=channel_pk,
        channel_id=channel_id,
        channel_username=channel_username,
    )


@pytest.fixture(scope="session")
def cli_env(cli_real_cli_env: CliRealCliEnv) -> CliEnv:
    return cli_real_cli_env


def _cli_command(cli_env: CliEnv, args: tuple[str, ...]) -> list[str]:
    return [
        sys.executable,
        "-m",
        "src.main",
        "--config",
        str(cli_env.config_path),
        *args,
    ]


def _build_cli_env(cli_env: CliEnv, extra_env: dict[str, str] | None = None) -> dict[str, str]:
    env = os.environ.copy()
    existing = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = f"{cli_env.source_root}{os.pathsep}{existing}" if existing else str(cli_env.source_root)
    env["PYTHONSAFEPATH"] = "1"
    if extra_env:
        env.update(extra_env)
    return env


def cli_run_direct(
    cli_env: CliEnv,
    *args: str,
    timeout: float = 20.0,
    extra_env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess:
    """subprocess.run wrapper for cleanup code that must not call pytest.skip."""
    return subprocess.run(  # noqa: S603 - controlled CLI module invocation
        _cli_command(cli_env, args),
        cwd=str(cli_env.repo_root),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=timeout,
        env=_build_cli_env(cli_env, extra_env=extra_env),
        check=False,
    )


@pytest.fixture
def run_cli(cli_real_cli_env: CliEnv):
    def _run(
        *args: str,
        timeout: int = 120,
        extra_env: dict[str, str] | None = None,
    ) -> subprocess.CompletedProcess:
        try:
            return subprocess.run(  # noqa: S603 - controlled CLI module invocation
                _cli_command(cli_real_cli_env, args),
                cwd=str(cli_real_cli_env.repo_root),
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=timeout,
                env=_build_cli_env(cli_real_cli_env, extra_env=extra_env),
                check=False,
            )
        except subprocess.TimeoutExpired:
            pytest.fail(f"CLI command timed out after {timeout}s: {' '.join(args)}", pytrace=False)

    return _run


@pytest.fixture
def run_cli_popen(cli_real_cli_env: CliEnv):
    """Spawn long-running CLI commands and clean them up on test teardown."""
    processes: list[subprocess.Popen] = []

    def _spawn(*args: str, extra_env: dict[str, str] | None = None) -> subprocess.Popen:
        proc = subprocess.Popen(  # noqa: S603 - controlled CLI module invocation
            _cli_command(cli_real_cli_env, args),
            cwd=str(cli_real_cli_env.repo_root),
            env=_build_cli_env(cli_real_cli_env, extra_env=extra_env),
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
            proc.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            try:
                proc.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                pass


def wait_for_http_200(url: str, *, timeout: float = 15.0, interval: float = 0.5) -> bool:
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


def sqlite_utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def read_pid_file(path: Path) -> int | None:
    try:
        raw = path.read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        return None
    if not raw:
        return None
    try:
        return int(raw)
    except ValueError:
        return None


def skip_if_server_pid_exists(cli_env: CliEnv) -> None:
    if not cli_env.pid_path.exists():
        return
    pid = read_pid_file(cli_env.pid_path)
    suffix = f" PID {pid}" if pid is not None else ""
    pytest.skip(
        f"live server PID file already exists at {cli_env.pid_path}{suffix}; "
        "stop the existing server or use a separate CLI_REAL_TG_CONFIG"
    )


def wait_for_pid_file(
    path: Path,
    expected_pid: int,
    *,
    timeout: float = 15.0,
    interval: float = 0.2,
) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if read_pid_file(path) == expected_pid:
            return True
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
_AUTH_RE = re.compile(r"AuthKeyError|AuthKeyUnregistered|session\s+expired|UnauthorizedError", re.IGNORECASE)
_SILENT_FAILURE_RE = re.compile(
    r"Traceback|ModuleNotFoundError|No connected accounts|No accounts found|"
    r"Could not resolve channel|Error fetching broadcast stats|Failed to initialize|"
    r"Failed to load|Error sending reaction|RuntimeError",
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
def live_channel(cli_real_cli_env: CliRealCliEnv) -> tuple[str, str]:
    if cli_real_cli_env.channel_pk is None or cli_real_cli_env.channel_id is None:
        pytest.skip("live CLI database has no active channel")
    return cli_real_cli_env.channel_pk, str(cli_real_cli_env.channel_id)


@pytest.fixture
def live_channel_username(cli_real_cli_env: CliRealCliEnv) -> str:
    if not cli_real_cli_env.channel_username:
        pytest.skip("live CLI database has no active channel with username")
    username = cli_real_cli_env.channel_username
    return username if username.startswith("@") else f"@{username}"


@pytest.fixture
def live_phone(cli_real_cli_env: CliRealCliEnv) -> str:
    return cli_real_cli_env.primary_phone


_LEADING_INT_ROW_RE = re.compile(r"^\s*(\d+)\s+\S", re.MULTILINE)


@pytest.fixture
def discover_first_pipeline_id(run_cli, assert_cli_ok):
    def _discover() -> str:
        result = run_cli("pipeline", "list")
        assert_cli_ok(result)
        match = _LEADING_INT_ROW_RE.search(result.stdout)
        if not match:
            pytest.skip("no pipelines - `pipeline list` returned no rows")
        return match.group(1)

    return _discover


@pytest.fixture
def discover_first_run_id(run_cli, assert_cli_ok, discover_first_pipeline_id):
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
    def _discover() -> str:
        result = run_cli("search-query", "list")
        assert_cli_ok(result)
        match = _LEADING_INT_ROW_RE.search(result.stdout)
        if not match:
            pytest.skip("no search queries - `search-query list` returned no rows")
        return match.group(1)

    return _discover


_AGENT_THREAD_ROW_RE = re.compile(r"^\[(\d+)\]", re.MULTILINE)


@pytest.fixture
def discover_first_agent_thread_id(run_cli, assert_cli_ok):
    def _discover() -> str:
        result = run_cli("agent", "threads")
        assert_cli_ok(result)
        match = _AGENT_THREAD_ROW_RE.search(result.stdout)
        if not match:
            pytest.skip("no agent threads - `agent threads` returned no rows")
        return match.group(1)

    return _discover
