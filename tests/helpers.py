"""Shared test helpers."""

from __future__ import annotations

import base64
import hashlib
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from src.collection_queue import CollectionQueue
from src.config import AppConfig, TelegramRuntimeConfig
from src.database import Database
from src.models import Account
from src.scheduler.manager import SchedulerManager
from src.search.ai_search import AISearchEngine
from src.search.engine import SearchEngine
from src.telegram.auth import TelegramAuth
from src.telegram.client_pool import ClientPool
from src.telegram.collector import Collector
from src.web.app import create_app


class AsyncIterEmpty:
    """Async iterator that yields nothing."""

    def __aiter__(self):
        return self

    async def __anext__(self):
        raise StopAsyncIteration


class AsyncIterMessages:
    """Async iterator over a list of messages."""

    def __init__(self, messages):
        self._iter = iter(messages)

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self._iter)
        except StopIteration:
            raise StopAsyncIteration


class FakeConversation:
    """Simple async conversation double for BotFather-like flows."""

    def __init__(
        self,
        *,
        responses: list[object] | None = None,
        edits: list[object] | None = None,
    ):
        self._responses = list(responses or [])
        self._edits = list(edits or [])
        self.sent_messages: list[object] = []
        self.__aenter__ = AsyncMock(return_value=self)
        self.__aexit__ = AsyncMock(return_value=None)
        self.send_message = AsyncMock(side_effect=self._send_message)
        self.get_response = AsyncMock(side_effect=self._get_response)
        self.get_edit = AsyncMock(side_effect=self._get_edit)

    async def _send_message(self, message: object) -> None:
        self.sent_messages.append(message)

    async def _get_response(self) -> object:
        if not self._responses:
            raise RuntimeError("No queued conversation response")
        return self._responses.pop(0)

    async def _get_edit(self) -> object:
        if not self._edits:
            raise RuntimeError("No queued conversation edit")
        return self._edits.pop(0)


class FakeTelethonClient:
    """Controllable Telethon-like client for collector tests."""

    def __init__(
        self,
        *,
        entity_resolver=None,
        dialogs=None,
        iter_messages_factory=None,
    ):
        self._entity_resolver = entity_resolver or (lambda arg: SimpleNamespace())
        self._dialogs = [] if dialogs is None else dialogs
        self._iter_messages_factory = iter_messages_factory or (lambda *a, **kw: AsyncIterEmpty())
        self.get_entity = AsyncMock(side_effect=self._get_entity)
        self.get_dialogs = AsyncMock(side_effect=self._get_dialogs)
        self.iter_messages = MagicMock(side_effect=self._iter_messages)

    async def _get_entity(self, arg):
        result = self._entity_resolver(arg)
        if isinstance(result, Exception):
            raise result
        return result

    async def _get_dialogs(self):
        if isinstance(self._dialogs, Exception):
            raise self._dialogs
        return self._dialogs

    def _iter_messages(self, *args, **kwargs):
        return self._iter_messages_factory(*args, **kwargs)


class FakeCliTelethonClient:
    """Telethon-like client double used behind telethon-cli runtime.create_client."""

    def __init__(
        self,
        *,
        me=None,
        entity_resolver=None,
        input_entity_resolver=None,
        dialogs=None,
        iter_dialogs_factory=None,
        iter_messages_factory=None,
        invoke_side_effect=None,
        profile_photo_result=False,
        conversation_factory=None,
        send_message_side_effect=None,
        send_file_side_effect=None,
        delete_dialog_side_effect=None,
        authorized=True,
    ):
        self._me = me or SimpleNamespace(
            first_name="",
            last_name="",
            username=None,
            premium=False,
        )
        self._entity_resolver = entity_resolver or (lambda arg: SimpleNamespace())
        self._input_entity_resolver = input_entity_resolver or self._entity_resolver
        self._dialogs = [] if dialogs is None else dialogs
        self._iter_dialogs_factory = iter_dialogs_factory or (lambda: AsyncIterEmpty())
        self._iter_messages_factory = iter_messages_factory or (lambda *a, **kw: AsyncIterEmpty())
        self._invoke_side_effect = invoke_side_effect
        self._profile_photo_result = profile_photo_result
        self._conversation_factory = conversation_factory
        self._send_message_side_effect = send_message_side_effect
        self._send_file_side_effect = send_file_side_effect
        self._delete_dialog_side_effect = delete_dialog_side_effect

        self.flood_sleep_threshold = 60
        self.connect = AsyncMock()
        self.disconnect = AsyncMock()
        self.is_user_authorized = AsyncMock(return_value=authorized)
        self.get_me = AsyncMock(side_effect=self._get_me)
        self.download_profile_photo = AsyncMock(side_effect=self._download_profile_photo)
        self.get_entity = AsyncMock(side_effect=self._get_entity)
        self.get_input_entity = AsyncMock(side_effect=self._get_input_entity)
        self.get_dialogs = AsyncMock(side_effect=self._get_dialogs)
        self.iter_dialogs = MagicMock(side_effect=self._iter_dialogs)
        self.iter_messages = MagicMock(side_effect=self._iter_messages)
        self.send_message = AsyncMock(side_effect=self._send_message)
        self.send_file = AsyncMock(side_effect=self._send_file)
        self.delete_dialog = AsyncMock(side_effect=self._delete_dialog)
        self.conversation = MagicMock(side_effect=self._conversation)
        self.invoke = AsyncMock(side_effect=self._invoke)

    async def _get_me(self):
        return self._me

    async def _download_profile_photo(self, _entity, *, file):
        if callable(getattr(file, "write", None)) and self._profile_photo_result:
            file.write(b"img")
        return self._profile_photo_result

    async def _get_entity(self, arg):
        result = self._entity_resolver(arg)
        if isinstance(result, Exception):
            raise result
        return result

    async def _get_input_entity(self, arg):
        result = self._input_entity_resolver(arg)
        if isinstance(result, Exception):
            raise result
        return result

    async def _get_dialogs(self):
        if isinstance(self._dialogs, Exception):
            raise self._dialogs
        return self._dialogs

    def _iter_dialogs(self):
        return self._iter_dialogs_factory()

    def _iter_messages(self, *args, **kwargs):
        return self._iter_messages_factory(*args, **kwargs)

    async def _send_message(self, *args, **kwargs):
        if callable(self._send_message_side_effect):
            result = await _maybe_await(self._send_message_side_effect(*args, **kwargs))
            if isinstance(result, Exception):
                raise result
            return result
        if isinstance(self._send_message_side_effect, Exception):
            raise self._send_message_side_effect
        return self._send_message_side_effect

    async def _send_file(self, *args, **kwargs):
        if callable(self._send_file_side_effect):
            result = await _maybe_await(self._send_file_side_effect(*args, **kwargs))
            if isinstance(result, Exception):
                raise result
            return result
        if isinstance(self._send_file_side_effect, Exception):
            raise self._send_file_side_effect
        return self._send_file_side_effect

    async def _delete_dialog(self, *args, **kwargs):
        if callable(self._delete_dialog_side_effect):
            result = await _maybe_await(self._delete_dialog_side_effect(*args, **kwargs))
            if isinstance(result, Exception):
                raise result
            return result
        if isinstance(self._delete_dialog_side_effect, Exception):
            raise self._delete_dialog_side_effect
        return self._delete_dialog_side_effect

    def _conversation(self, *args, **kwargs):
        if callable(self._conversation_factory):
            return self._conversation_factory(*args, **kwargs)
        if self._conversation_factory is not None:
            return self._conversation_factory
        return FakeConversation()

    async def _invoke(self, request):
        if callable(self._invoke_side_effect):
            result = self._invoke_side_effect(request)
            if isinstance(result, Exception):
                raise result
            return result
        if isinstance(self._invoke_side_effect, Exception):
            raise self._invoke_side_effect
        return self._invoke_side_effect

    async def __call__(self, request):
        return await self.invoke(request)


class FakeClientPool(MagicMock):
    """Pool double with controllable async methods and dialog cache state."""

    def __init__(self, **kwargs):
        super().__init__()
        self.clients = kwargs.pop("clients", {})
        self.release_client = kwargs.pop("release_client", AsyncMock())
        self.report_flood = kwargs.pop("report_flood", AsyncMock())
        self.get_client_by_phone = kwargs.pop("get_client_by_phone", AsyncMock(return_value=None))
        self.get_available_client = kwargs.pop("get_available_client", AsyncMock(return_value=None))
        self.get_stats_availability = kwargs.pop("get_stats_availability", AsyncMock())
        self._dialogs_fetched: set[str] = set()
        self.is_dialogs_fetched = lambda phone: phone in self._dialogs_fetched
        self.mark_dialogs_fetched = lambda phone: self._dialogs_fetched.add(phone)
        for key, value in kwargs.items():
            setattr(self, key, value)

    @staticmethod
    def _classify_entity(entity) -> tuple[str, bool]:
        from src.telegram.client_pool import ClientPool

        return ClientPool._classify_entity(entity)


def make_mock_reactions(items: list[tuple[str, int]]) -> SimpleNamespace:
    """Create a mock MessageReactions object.

    Args:
        items: list of (emoji_or_custom_id, count) tuples.
            Plain strings are treated as emoticons;
            integers are treated as custom emoji document_ids.
    """
    results = []
    for emoji, count in items:
        if isinstance(emoji, int):
            reaction = SimpleNamespace(emoticon=None, document_id=emoji)
        else:
            reaction = SimpleNamespace(emoticon=emoji)
        results.append(SimpleNamespace(reaction=reaction, count=count))
    return SimpleNamespace(results=results)


def make_mock_message(msg_id, text=None, media=None, sender_id=None, *, date=None, reactions=None):
    return SimpleNamespace(
        id=msg_id,
        text=text,
        media=media,
        sender_id=sender_id,
        sender=None,
        date=date or datetime(2025, 1, 1, tzinfo=timezone.utc),
        reactions=reactions,
    )


def make_stats_availability(state: str, *, next_available_at_utc=None):
    return SimpleNamespace(state=state, next_available_at_utc=next_available_at_utc)


def make_mock_pool(**kwargs) -> MagicMock:
    """Create a MagicMock pool with async methods properly mocked."""
    return FakeClientPool(**kwargs)


@dataclass
class RealPoolHarness:
    """Real ClientPool harness with deterministic fake backends."""

    db: object
    auth: TelegramAuth
    pool: ClientPool
    telethon_cli_spy: object
    native_auth_spy: object

    @classmethod
    def build(
        cls,
        *,
        db: object,
        telethon_cli_spy: object,
        native_auth_spy: object,
        backend_mode: str = "auto",
        cli_transport: str = "hybrid",
        session_cache_dir: str | Path = "data/test_telegram_sessions",
        auth: TelegramAuth | None = None,
    ) -> RealPoolHarness:
        auth = auth or TelegramAuth(12345, "test_hash")
        pool = ClientPool(
            auth,
            db,
            runtime_config=TelegramRuntimeConfig(
                backend_mode=backend_mode,
                cli_transport=cli_transport,
                session_cache_dir=str(session_cache_dir),
            ),
        )
        return cls(
            db=db,
            auth=auth,
            pool=pool,
            telethon_cli_spy=telethon_cli_spy,
            native_auth_spy=native_auth_spy,
        )

    def queue_cli_client(
        self,
        client: FakeCliTelethonClient | None = None,
        *,
        phone: str | None = None,
        **kwargs: Any,
    ) -> FakeCliTelethonClient:
        client = client or FakeCliTelethonClient(**kwargs)
        if phone is None:
            self.telethon_cli_spy.enqueue(client)
        else:
            self.telethon_cli_spy.bind(phone, client)
        return client

    def queue_native_client(
        self,
        client: FakeCliTelethonClient | None = None,
        *,
        session_string: str | None = None,
        **kwargs: Any,
    ) -> FakeCliTelethonClient:
        client = client or FakeCliTelethonClient(**kwargs)
        if session_string is None:
            self.native_auth_spy.enqueue(client)
        else:
            self.native_auth_spy.bind(session_string, client)
        return client

    async def add_account(
        self,
        phone: str,
        *,
        session_string: str | None = None,
        is_primary: bool = False,
        is_premium: bool = False,
        is_active: bool = True,
        flood_wait_until=None,
    ) -> Account:
        session_string = session_string or f"session-{phone}"
        account = Account(
            phone=phone,
            session_string=session_string,
            is_primary=is_primary,
            is_premium=is_premium,
            is_active=is_active,
            flood_wait_until=flood_wait_until,
        )
        await self.db.add_account(account)
        return account

    async def initialize_connected_accounts(self) -> None:
        await self.pool.initialize()

    async def auth_connect_account(
        self,
        phone: str,
        *,
        session_string: str | None = None,
    ) -> str:
        session_string = session_string or f"session-{phone}"
        await self.pool.add_client(phone, session_string)
        return session_string

    async def connect_account(
        self,
        phone: str,
        *,
        session_string: str | None = None,
        is_primary: bool = False,
        is_premium: bool = False,
        is_active: bool = True,
        flood_wait_until=None,
    ) -> Account:
        account = await self.add_account(
            phone,
            session_string=session_string,
            is_primary=is_primary,
            is_premium=is_premium,
            is_active=is_active,
            flood_wait_until=flood_wait_until,
        )
        await self.initialize_connected_accounts()
        return account


def make_test_config(
    tmp_path: Path,
    *,
    db_name: str = "test.db",
    password: str = "testpass",
) -> AppConfig:
    config = AppConfig()
    config.database.path = str(tmp_path / db_name)
    config.telegram.api_id = 12345
    config.telegram.api_hash = "test_hash"
    config.web.password = password
    return config


async def build_web_app(
    config: AppConfig,
    harness: RealPoolHarness,
    *,
    db: Database | None = None,
    add_account: str | None = None,
    session_secret: str = "test_secret_key",
) -> tuple[FastAPI, Database]:
    app = create_app(config)
    if db is None:
        db = Database(config.database.path)
        await db.initialize()
    app.state.db = db
    app.state.auth = harness.auth
    app.state.pool = harness.pool
    app.state.notifier = None
    collector = Collector(app.state.pool, db, config.scheduler)
    app.state.collector = collector
    app.state.collection_queue = CollectionQueue(collector, db)
    app.state.search_engine = SearchEngine(db)
    app.state.ai_search = AISearchEngine(config.llm, db)
    app.state.scheduler = SchedulerManager(config.scheduler)
    app.state.session_secret = session_secret
    if add_account:
        await db.add_account(Account(phone=add_account, session_string="test_session"))
    return app, db


@asynccontextmanager
async def make_auth_client(app, *, password: str = "testpass", with_auth: bool = True):
    transport = ASGITransport(app=app)
    headers = {"Origin": "http://test"}
    if with_auth:
        auth_header = base64.b64encode(f":{password}".encode()).decode()
        headers["Authorization"] = f"Basic {auth_header}"
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        follow_redirects=True,
        headers=headers,
    ) as c:
        yield c


def make_channel_entity(
    identifier: str | int = "test_channel",
    *,
    broadcast: bool = True,
    scam: bool = False,
    fake: bool = False,
    **overrides,
) -> SimpleNamespace:
    if isinstance(identifier, int):
        channel_id = identifier
        title = f"Channel {identifier}"
        username = None
    else:
        ident = identifier.strip().lower().lstrip("@")
        channel_id = int(hashlib.md5(ident.encode()).hexdigest(), 16) % 10**10
        title = f"Channel {ident}"
        username = ident if not ident.lstrip("-").isdigit() else None
    defaults = dict(
        id=channel_id,
        title=title,
        username=username,
        broadcast=broadcast,
        megagroup=False,
        gigagroup=False,
        forum=False,
        monoforum=False,
        scam=scam,
        fake=fake,
        restricted=False,
    )
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


async def _maybe_await(value):
    if hasattr(value, "__await__"):
        return await value
    return value
