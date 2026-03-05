from __future__ import annotations

import aiosqlite

from src.database.connection import DBConnection
from src.database.migrations import run_migrations
from src.database.repositories.accounts import AccountsRepository
from src.database.repositories.channel_stats import ChannelStatsRepository
from src.database.repositories.channels import ChannelsRepository
from src.database.repositories.collection_tasks import CollectionTasksRepository
from src.database.repositories.keywords import KeywordsRepository
from src.database.repositories.messages import MessagesRepository
from src.database.repositories.search_log import SearchLogRepository
from src.database.repositories.settings import SettingsRepository
from src.database.schema import SCHEMA_SQL
from src.models import Account, Channel, ChannelStats, CollectionTask, Keyword, Message
from src.security import SessionCipher


class Database:
    def __init__(
        self,
        db_path: str = "data/tg_search.db",
        session_encryption_secret: str | None = None,
    ):
        self._db_path = db_path
        self._session_encryption_secret = session_encryption_secret
        self._connection = DBConnection(db_path)
        self._db: aiosqlite.Connection | None = None
        self._accounts: AccountsRepository | None = None
        self._channels: ChannelsRepository | None = None
        self._messages: MessagesRepository | None = None
        self._keywords: KeywordsRepository | None = None
        self._tasks: CollectionTasksRepository | None = None
        self._search_log: SearchLogRepository | None = None
        self._channel_stats: ChannelStatsRepository | None = None
        self._settings: SettingsRepository | None = None

    async def _has_encrypted_sessions(self) -> bool:
        assert self._db is not None
        cur = await self._db.execute(
            """
            SELECT 1
            FROM accounts
            WHERE session_string LIKE 'enc:v1:%'
               OR session_string LIKE 'enc:v2:%'
            LIMIT 1
            """
        )
        return bool(await cur.fetchone())

    async def initialize(self) -> None:
        self._db = await self._connection.connect()
        await self._db.executescript(SCHEMA_SQL)
        await self._db.commit()
        await run_migrations(self._db)

        if not self._session_encryption_secret and await self._has_encrypted_sessions():
            raise RuntimeError(
                "Encrypted account sessions found in DB but SESSION_ENCRYPTION_KEY is not set. "
                "Set SESSION_ENCRYPTION_KEY to start the application."
            )

        session_cipher = None
        if self._session_encryption_secret:
            session_cipher = SessionCipher(self._session_encryption_secret)

        self._accounts = AccountsRepository(self._db, session_cipher=session_cipher)
        self._channels = ChannelsRepository(self._db)
        self._messages = MessagesRepository(self._db)
        self._keywords = KeywordsRepository(self._db)
        self._tasks = CollectionTasksRepository(self._db)
        self._search_log = SearchLogRepository(self._db)
        self._channel_stats = ChannelStatsRepository(self._db)
        self._settings = SettingsRepository(self._db)

        await self._accounts.migrate_sessions()

    async def close(self) -> None:
        await self._connection.close()

    async def execute(self, sql: str, params: tuple = ()) -> aiosqlite.Cursor:
        return await self._connection.execute(sql, params)

    async def execute_fetchall(self, sql: str, params: tuple = ()) -> list:
        return await self._connection.execute_fetchall(sql, params)

    @property
    def db(self) -> aiosqlite.Connection | None:
        return self._db

    def _require(self) -> None:
        if any(
            repo is None
            for repo in (
                self._accounts,
                self._channels,
                self._messages,
                self._keywords,
                self._tasks,
                self._search_log,
                self._channel_stats,
                self._settings,
            )
        ):
            raise RuntimeError("Database.initialize() has not been called")

    async def add_account(self, account: Account) -> int:
        self._require()
        return await self._accounts.add_account(account)

    async def get_accounts(self, active_only: bool = False) -> list[Account]:
        self._require()
        return await self._accounts.get_accounts(active_only)

    async def update_account_flood(self, phone: str, until) -> None:
        self._require()
        await self._accounts.update_account_flood(phone, until)

    async def update_account_premium(self, phone: str, is_premium: bool) -> None:
        self._require()
        await self._accounts.update_account_premium(phone, is_premium)

    async def set_account_active(self, account_id: int, active: bool) -> None:
        self._require()
        await self._accounts.set_account_active(account_id, active)

    async def delete_account(self, account_id: int) -> None:
        self._require()
        await self._accounts.delete_account(account_id)

    async def add_channel(self, channel: Channel) -> int:
        self._require()
        return await self._channels.add_channel(channel)

    async def get_channels(
        self, active_only: bool = False, include_filtered: bool = True
    ) -> list[Channel]:
        self._require()
        return await self._channels.get_channels(active_only, include_filtered)

    async def get_channel_by_pk(self, pk: int) -> Channel | None:
        self._require()
        return await self._channels.get_channel_by_pk(pk)

    async def get_channels_with_counts(
        self, active_only: bool = False, include_filtered: bool = True
    ) -> list[Channel]:
        self._require()
        return await self._channels.get_channels_with_counts(active_only, include_filtered)

    async def update_channel_last_id(self, channel_id: int, last_id: int) -> None:
        self._require()
        await self._channels.update_channel_last_id(channel_id, last_id)

    async def set_channel_active(self, pk: int, active: bool) -> None:
        self._require()
        await self._channels.set_channel_active(pk, active)

    async def set_channel_filtered(self, pk: int, filtered: bool) -> None:
        self._require()
        await self._channels.set_channel_filtered(pk, filtered)

    async def set_channels_filtered_bulk(self, updates: list[tuple[int, str]]) -> int:
        self._require()
        return await self._channels.set_filtered_bulk(updates)

    async def reset_all_channel_filters(self) -> int:
        self._require()
        return await self._channels.reset_all_filters()

    async def delete_channel(self, pk: int) -> None:
        self._require()
        await self._channels.delete_channel(pk)

    async def insert_message(self, msg: Message) -> bool:
        self._require()
        return await self._messages.insert_message(msg)

    async def insert_messages_batch(self, messages: list[Message]) -> int:
        self._require()
        return await self._messages.insert_messages_batch(messages)

    async def search_messages(
        self,
        query: str = "",
        channel_id: int | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[list[Message], int]:
        self._require()
        return await self._messages.search_messages(
            query=query,
            channel_id=channel_id,
            date_from=date_from,
            date_to=date_to,
            limit=limit,
            offset=offset,
        )

    async def get_stats(self) -> dict:
        self._require()
        return await self._messages.get_stats()

    async def add_keyword(self, keyword: Keyword) -> int:
        self._require()
        return await self._keywords.add_keyword(keyword)

    async def get_keywords(self, active_only: bool = False) -> list[Keyword]:
        self._require()
        return await self._keywords.get_keywords(active_only)

    async def set_keyword_active(self, keyword_id: int, active: bool) -> None:
        self._require()
        await self._keywords.set_keyword_active(keyword_id, active)

    async def delete_keyword(self, keyword_id: int) -> None:
        self._require()
        await self._keywords.delete_keyword(keyword_id)

    async def create_collection_task(self, channel_id: int, channel_title: str | None) -> int:
        self._require()
        return await self._tasks.create_collection_task(channel_id, channel_title)

    async def update_collection_task_progress(self, task_id: int, messages_collected: int) -> None:
        self._require()
        await self._tasks.update_collection_task_progress(task_id, messages_collected)

    async def update_collection_task(
        self,
        task_id: int,
        status: str,
        messages_collected: int | None = None,
        error: str | None = None,
    ) -> None:
        self._require()
        await self._tasks.update_collection_task(task_id, status, messages_collected, error)

    async def get_collection_task(self, task_id: int) -> CollectionTask | None:
        self._require()
        return await self._tasks.get_collection_task(task_id)

    async def get_collection_tasks(self, limit: int = 20) -> list[CollectionTask]:
        self._require()
        return await self._tasks.get_collection_tasks(limit)

    async def cancel_collection_task(self, task_id: int) -> bool:
        self._require()
        return await self._tasks.cancel_collection_task(task_id)

    async def log_search(self, phone: str, query: str, results_count: int) -> None:
        self._require()
        await self._search_log.log_search(phone, query, results_count)

    async def get_recent_searches(self, limit: int = 20) -> list[dict]:
        self._require()
        return await self._search_log.get_recent_searches(limit)

    async def save_channel_stats(self, stats: ChannelStats) -> int:
        self._require()
        return await self._channel_stats.save_channel_stats(stats)

    async def get_channel_stats(self, channel_id: int, limit: int = 1) -> list[ChannelStats]:
        self._require()
        return await self._channel_stats.get_channel_stats(channel_id, limit)

    async def get_latest_stats_for_all(self) -> dict[int, ChannelStats]:
        self._require()
        return await self._channel_stats.get_latest_stats_for_all()

    async def get_setting(self, key: str) -> str | None:
        self._require()
        return await self._settings.get_setting(key)

    async def set_setting(self, key: str, value: str) -> None:
        self._require()
        await self._settings.set_setting(key, value)
