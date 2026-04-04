from __future__ import annotations

import asyncio
import os
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Collection, Mapping
from unittest.mock import patch

import pytest
from telethon import TelegramClient
from telethon.sessions import StringSession

from src.config import AppConfig
from src.database import Database
from src.telegram.auth import TelegramAuth
from src.telegram.session_materializer import SessionMaterializer
from tests.helpers import RealPoolHarness

REAL_TG_SAFE_MARK = "real_tg_safe"
REAL_TG_MANUAL_MARK = "real_tg_manual"
REAL_TG_NEVER_MARK = "real_tg_never"
REAL_TG_LIVE_FIXTURE = "real_telegram_sandbox"
REAL_TG_SAFE_GATE_ENV = "RUN_REAL_TELEGRAM_SAFE"
REAL_TG_MANUAL_GATE_ENV = "RUN_REAL_TELEGRAM_MANUAL"
REAL_TG_REQUIRED_ENV_VARS = (
    "REAL_TG_API_ID",
    "REAL_TG_API_HASH",
    "REAL_TG_PHONE",
    "REAL_TG_SESSION",
)
REAL_TG_OPTIONAL_ENV_VARS = (
    "REAL_TG_READ_CHANNEL_USERNAME",
    "REAL_TG_READ_CHANNEL_ID",
    "REAL_TG_PRIVATE_CHAT_ID",
    "REAL_TG_BOT_USERNAME",
)


@pytest.fixture
async def db():
    """In-memory test database."""
    database = Database(":memory:")
    await database.initialize()
    yield database
    await database.close()


@pytest.fixture
def cli_db(tmp_path):
    """Sync fixture: real SQLite for CLI tests."""
    db_path = str(tmp_path / "cli_test.db")
    database = Database(db_path)
    asyncio.run(database.initialize())
    yield database
    asyncio.run(database.close())


@pytest.fixture
def cli_env(cli_db):
    """Patch runtime.init_db to return real db without loading config.yaml."""
    config = AppConfig()

    async def fake_init_db(config_path: str):
        return config, cli_db

    with patch("src.cli.runtime.init_db", side_effect=fake_init_db):
        yield cli_db


@pytest.fixture
def cli_init_patch():
    """Patch one or more CLI init_db targets to return the provided database."""

    @contextmanager
    def _patch(
        db,
        *targets: str,
        config: AppConfig | None = None,
        fresh_database: bool = False,
    ):
        runtime_config = config or AppConfig()

        async def fake_init_db(_config_path: str):
            if isinstance(db, Database) and fresh_database:
                cmd_db = Database(db._db_path, session_encryption_secret=db._session_encryption_secret)
                await cmd_db.initialize()
                return runtime_config, cmd_db
            if isinstance(db, Database) and db._connection.db is None:
                await db.initialize()
            return runtime_config, db

        with ExitStack() as stack:
            for target in targets:
                stack.enter_context(patch(target, side_effect=fake_init_db))
            yield runtime_config, db

    return _patch


@pytest.fixture
def config():
    return AppConfig()


@dataclass(frozen=True)
class RealTelegramSandboxConfig:
    api_id: int
    api_hash: str
    phone: str
    session_string: str
    saved_messages_target: str = "me"
    read_channel_username: str | None = None
    read_channel_id: int | None = None
    private_chat_id: int | None = None
    bot_username: str | None = None


@dataclass(frozen=True)
class RealTelegramSandbox:
    client: TelegramClient
    api_id: int
    api_hash: str
    phone: str
    session_string: str
    saved_messages_target: str = "me"
    read_channel_username: str | None = None
    read_channel_id: int | None = None
    private_chat_id: int | None = None
    bot_username: str | None = None


def _resolve_real_tg_mode(node) -> str | None:
    markers = [
        name
        for name in (REAL_TG_SAFE_MARK, REAL_TG_MANUAL_MARK, REAL_TG_NEVER_MARK)
        if node.get_closest_marker(name)
    ]
    if len(markers) > 1:
        raise pytest.UsageError(
            f"{node.nodeid} uses multiple real Telegram policy markers: {', '.join(markers)}"
        )
    return markers[0] if markers else None


def _evaluate_real_tg_policy(
    *,
    mode: str | None,
    fixturenames: Collection[str],
    environ: Mapping[str, str],
) -> tuple[str | None, str | None]:
    uses_live_fixture = REAL_TG_LIVE_FIXTURE in fixturenames

    if uses_live_fixture and mode is None:
        return (
            "fail",
            f"{REAL_TG_LIVE_FIXTURE} requires @{REAL_TG_SAFE_MARK} or @{REAL_TG_MANUAL_MARK}.",
        )

    if mode in {REAL_TG_SAFE_MARK, REAL_TG_MANUAL_MARK} and not uses_live_fixture:
        return (
            "fail",
            f"@{mode} tests must use the {REAL_TG_LIVE_FIXTURE} fixture.",
        )

    if mode == REAL_TG_NEVER_MARK and uses_live_fixture:
        return (
            "fail",
            f"@{REAL_TG_NEVER_MARK} tests cannot request {REAL_TG_LIVE_FIXTURE}.",
        )

    if mode == REAL_TG_SAFE_MARK and environ.get(REAL_TG_SAFE_GATE_ENV) != "1":
        return (
            "skip",
            f"real Telegram safe tests are disabled; set {REAL_TG_SAFE_GATE_ENV}=1 to run them.",
        )

    if mode == REAL_TG_MANUAL_MARK and environ.get(REAL_TG_MANUAL_GATE_ENV) != "1":
        return (
            "skip",
            "real Telegram manual tests are disabled; "
            f"set {REAL_TG_MANUAL_GATE_ENV}=1 to run them.",
        )

    return None, None


def _build_real_telegram_sandbox_config(
    environ: Mapping[str, str],
) -> RealTelegramSandboxConfig:
    missing = [name for name in REAL_TG_REQUIRED_ENV_VARS if not environ.get(name)]
    if missing:
        raise RuntimeError(
            "Missing real Telegram sandbox environment variables: "
            + ", ".join(missing)
            + ". Dedicated REAL_TG_* vars are required; generic TG_* vars are not used."
        )

    def _optional_int(name: str) -> int | None:
        raw = environ.get(name)
        return int(raw) if raw else None

    return RealTelegramSandboxConfig(
        api_id=int(environ["REAL_TG_API_ID"]),
        api_hash=environ["REAL_TG_API_HASH"],
        phone=environ["REAL_TG_PHONE"],
        session_string=environ["REAL_TG_SESSION"],
        read_channel_username=environ.get("REAL_TG_READ_CHANNEL_USERNAME") or None,
        read_channel_id=_optional_int("REAL_TG_READ_CHANNEL_ID"),
        private_chat_id=_optional_int("REAL_TG_PRIVATE_CHAT_ID"),
        bot_username=environ.get("REAL_TG_BOT_USERNAME") or None,
    )


@dataclass
class TelethonCliSpy:
    queued_clients: list[object] = field(default_factory=list)
    created: list[tuple[object, object]] = field(default_factory=list)
    default_client: object | None = None
    factory: Callable[[object], object] | None = None
    by_phone: dict[str, object] = field(default_factory=dict)

    def enqueue(self, client: object) -> object:
        self.queued_clients.append(client)
        return client

    def bind(self, phone: str, client: object) -> object:
        self.by_phone[phone.lstrip("+") or "account"] = client
        return client


@dataclass
class NativeAuthSpy:
    queued_clients: list[object] = field(default_factory=list)
    created: list[tuple[str, object]] = field(default_factory=list)
    default_client: object | None = None
    factory: Callable[[str], object] | None = None
    by_session: dict[str, object] = field(default_factory=dict)

    def enqueue(self, client: object) -> object:
        self.queued_clients.append(client)
        return client

    def bind(self, session_string: str, client: object) -> object:
        self.by_session[session_string] = client
        return client


@pytest.fixture
def telethon_cli_spy():
    return TelethonCliSpy()


@pytest.fixture
def native_auth_spy():
    return NativeAuthSpy()


@pytest.fixture
def real_pool_harness_factory(db, telethon_cli_spy, native_auth_spy, tmp_path):
    def _factory(
        *,
        backend_mode: str = "auto",
        cli_transport: str = "hybrid",
        session_cache_dir: str | Path | None = None,
        auth: TelegramAuth | None = None,
    ):
        return RealPoolHarness.build(
            db=db,
            telethon_cli_spy=telethon_cli_spy,
            native_auth_spy=native_auth_spy,
            backend_mode=backend_mode,
            cli_transport=cli_transport,
            session_cache_dir=session_cache_dir or (tmp_path / "sessions"),
            auth=auth,
        )

    return _factory


@pytest.fixture
async def real_telegram_sandbox():
    cfg = _build_real_telegram_sandbox_config(os.environ)
    client = TelegramClient(StringSession(cfg.session_string), cfg.api_id, cfg.api_hash)
    await client.connect()
    try:
        if not await client.is_user_authorized():
            raise RuntimeError(
                "REAL_TG_SESSION is not authorized for the configured sandbox account."
            )
        yield RealTelegramSandbox(
            client=client,
            api_id=cfg.api_id,
            api_hash=cfg.api_hash,
            phone=cfg.phone,
            session_string=cfg.session_string,
            saved_messages_target=cfg.saved_messages_target,
            read_channel_username=cfg.read_channel_username,
            read_channel_id=cfg.read_channel_id,
            private_chat_id=cfg.private_chat_id,
            bot_username=cfg.bot_username,
        )
    finally:
        await client.disconnect()


def pytest_runtest_setup(item):
    mode = _resolve_real_tg_mode(item)
    action, message = _evaluate_real_tg_policy(
        mode=mode,
        fixturenames=item.fixturenames,
        environ=os.environ,
    )
    if action == "skip":
        pytest.skip(message)
    if action == "fail":
        pytest.fail(message, pytrace=False)


def pytest_collection_modifyitems(config, items):
    for item in items:
        if item.get_closest_marker("aiosqlite_serial") and not item.get_closest_marker("xdist_group"):
            item.add_marker(pytest.mark.xdist_group("aiosqlite_serial"))


@pytest.fixture(autouse=True)
def _enforce_cli_transport(
    monkeypatch,
    request,
    telethon_cli_spy: TelethonCliSpy,
    native_auth_spy: NativeAuthSpy,
    tmp_path,
):
    if REAL_TG_LIVE_FIXTURE in request.fixturenames:
        return

    def _fake_create_client(namespace):
        if telethon_cli_spy.queued_clients:
            client = telethon_cli_spy.queued_clients.pop(0)
        elif Path(str(namespace.session)).name in telethon_cli_spy.by_phone:
            client = telethon_cli_spy.by_phone[Path(str(namespace.session)).name]
        elif telethon_cli_spy.factory is not None:
            client = telethon_cli_spy.factory(namespace)
        elif telethon_cli_spy.default_client is not None:
            client = telethon_cli_spy.default_client
        else:
            raise AssertionError(
                "telethon_cli_spy has no queued/bound/default client for namespace "
                f"{namespace!r}. Use harness.queue_cli_client() or telethon_cli_spy.enqueue()."
            )
        telethon_cli_spy.created.append((namespace, client))
        return client

    monkeypatch.setattr(
        "src.telegram.backends.telethon_cli_runtime.create_client",
        _fake_create_client,
    )

    if not request.node.get_closest_marker("real_materializer"):

        def _fake_materialize(self: SessionMaterializer, phone: str, session_string: str) -> str:
            safe_phone = phone.lstrip("+") or "account"
            target = Path(tmp_path) / "materialized_sessions" / safe_phone
            target.parent.mkdir(parents=True, exist_ok=True)
            return str(target)

        monkeypatch.setattr(SessionMaterializer, "materialize", _fake_materialize)

    if request.node.get_closest_marker("native_backend_allowed"):

        async def _fake_native_transport(self, session_string: str):
            if native_auth_spy.queued_clients:
                client = native_auth_spy.queued_clients.pop(0)
            elif session_string in native_auth_spy.by_session:
                client = native_auth_spy.by_session[session_string]
            elif native_auth_spy.factory is not None:
                client = native_auth_spy.factory(session_string)
            elif native_auth_spy.default_client is not None:
                client = native_auth_spy.default_client
            else:
                raise AssertionError(
                    "native_auth_spy has no queued/bound/default client for session "
                    f"{session_string!r}. Use harness.queue_native_client() or "
                    "native_auth_spy.enqueue()."
                )
            native_auth_spy.created.append((session_string, client))
            return client

        monkeypatch.setattr(TelegramAuth, "create_client_from_session", _fake_native_transport)
        return

    async def _forbid_native_transport(self, session_string: str):
        raise AssertionError(
            "Native Telethon transport is forbidden in this test. "
            "Use telethon-cli-backed runtime or mark the test with native_backend_allowed."
        )

    monkeypatch.setattr(TelegramAuth, "create_client_from_session", _forbid_native_transport)


# ---------------------------------------------------------------------------
# Agent tools shared helpers
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_db():
    """Mock Database for agent tool tests."""
    from unittest.mock import AsyncMock, MagicMock

    db = MagicMock(spec=Database)
    db.get_setting = AsyncMock(return_value=None)
    db.set_setting = AsyncMock()
    db.get_stats = AsyncMock(return_value={"channels": 10, "messages": 1000})
    return db
