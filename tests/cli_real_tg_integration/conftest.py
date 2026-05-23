from __future__ import annotations

import os
import re
import sqlite3
import subprocess
import sys
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
        "web": {"password": "cli-real-tg-test"},
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


@pytest.fixture
def run_cli(cli_real_telegram_sandbox: CliRealTelegramSandbox):
    def _run(*args: str, timeout: int = 120) -> subprocess.CompletedProcess:
        env = os.environ.copy()
        existing = env.get("PYTHONPATH", "")
        env["PYTHONPATH"] = (
            f"{cli_real_telegram_sandbox.source_root}{os.pathsep}{existing}"
            if existing
            else str(cli_real_telegram_sandbox.source_root)
        )
        env["PYTHONSAFEPATH"] = "1"
        env["TG_API_ID"] = str(cli_real_telegram_sandbox.api_id)
        env["TG_API_HASH"] = cli_real_telegram_sandbox.api_hash
        env["TG_BACKEND_MODE"] = "native"
        env["TG_CLI_TRANSPORT"] = "in_process"
        env["TG_SESSION_CACHE_DIR"] = str(cli_real_telegram_sandbox.session_cache_dir)
        return subprocess.run(
            [
                sys.executable,
                "-m",
                "src.main",
                "--config",
                str(cli_real_telegram_sandbox.config_path),
                *args,
            ],
            cwd=str(cli_real_telegram_sandbox.work_dir),
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
