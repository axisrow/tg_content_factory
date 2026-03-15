from __future__ import annotations

from datetime import datetime
from typing import Any

import aiosqlite

from src.database.bundles import DatabaseRepositories
from src.database.connection import DBConnection
from src.database.migrations import run_migrations
from src.database.repositories.accounts import AccountsRepository
from src.database.repositories.channel_stats import ChannelStatsRepository
from src.database.repositories.channels import ChannelsRepository
from src.database.repositories.collection_tasks import CollectionTasksRepository
from src.database.repositories.dialog_cache import DialogCacheRepository
from src.database.repositories.filters import FilterRepository
from src.database.repositories.messages import MessagesRepository
from src.database.repositories.notification_bots import NotificationBotsRepository
from src.database.repositories.photo_loader import PhotoLoaderRepository
from src.database.repositories.search_log import SearchLogRepository
from src.database.repositories.search_queries import SearchQueriesRepository
from src.database.repositories.settings import SettingsRepository
from src.database.schema import SCHEMA_SQL
from src.models import (
    Account,
    Channel,
    ChannelStats,
    CollectionTask,
    CollectionTaskStatus,
    Message,
    NotificationBot,
    SearchQuery,
    StatsAllTaskPayload,
)
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
        self._fts_available: bool = True
        self._accounts: AccountsRepository | None = None
        self._channels: ChannelsRepository | None = None
        self._messages: MessagesRepository | None = None
        self._tasks: CollectionTasksRepository | None = None
        self._search_log: SearchLogRepository | None = None
        self._channel_stats: ChannelStatsRepository | None = None
        self._settings: SettingsRepository | None = None
        self._filters: FilterRepository | None = None
        self._notification_bots: NotificationBotsRepository | None = None
        self._search_queries: SearchQueriesRepository | None = None
        self._photo_loader: PhotoLoaderRepository | None = None
        self._dialog_cache: DialogCacheRepository | None = None
        self._repos: DatabaseRepositories | None = None

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
        self._fts_available = await run_migrations(self._db)

        if not self._session_encryption_secret and await self._has_encrypted_sessions():
            await self._connection.close()
            self._db = None
            raise RuntimeError(
                "Encrypted account sessions found in DB but SESSION_ENCRYPTION_KEY is not set. "
                "Set SESSION_ENCRYPTION_KEY to start the application."
            )

        session_cipher = None
        if self._session_encryption_secret:
            session_cipher = SessionCipher(self._session_encryption_secret)

        self._accounts = AccountsRepository(self._db, session_cipher=session_cipher)
        self._channels = ChannelsRepository(self._db)
        self._messages = MessagesRepository(self._db, fts_available=self._fts_available)
        self._tasks = CollectionTasksRepository(self._db)
        self._search_log = SearchLogRepository(self._db)
        self._channel_stats = ChannelStatsRepository(self._db)
        self._settings = SettingsRepository(self._db)
        self._filters = FilterRepository(self._db)
        self._notification_bots = NotificationBotsRepository(self._db)
        self._search_queries = SearchQueriesRepository(self._db)
        self._photo_loader = PhotoLoaderRepository(self._db)
        self._dialog_cache = DialogCacheRepository(self._db)
        self._repos = DatabaseRepositories(
            accounts=self._accounts,
            channels=self._channels,
            messages=self._messages,
            tasks=self._tasks,
            search_log=self._search_log,
            channel_stats=self._channel_stats,
            settings=self._settings,
            filters=self._filters,
            notification_bots=self._notification_bots,
            search_queries=self._search_queries,
            photo_loader=self._photo_loader,
            dialog_cache=self._dialog_cache,
        )

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

    @property
    def fts_available(self) -> bool:
        return self._fts_available

    @property
    def filter_repo(self) -> FilterRepository:
        self._require()
        assert self._filters is not None
        return self._filters

    @property
    def repos(self) -> DatabaseRepositories:
        self._require()
        assert self._repos is not None
        return self._repos

    def _require(self) -> None:
        if any(
            repo is None
            for repo in (
                self._accounts,
                self._channels,
                self._messages,
                self._tasks,
                self._search_log,
                self._channel_stats,
                self._settings,
                self._filters,
                self._notification_bots,
                self._search_queries,
                self._photo_loader,
                self._dialog_cache,
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

    async def get_channel_by_channel_id(self, channel_id: int) -> Channel | None:
        self._require()
        return await self._channels.get_channel_by_channel_id(channel_id)

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

    async def set_channels_filtered_bulk(
        self, updates: list[tuple[int, str]], *, commit: bool = True
    ) -> int:
        self._require()
        return await self._channels.set_filtered_bulk(updates, commit=commit)

    async def reset_all_channel_filters(self, *, commit: bool = True) -> int:
        self._require()
        return await self._channels.reset_all_filters(commit=commit)

    async def set_channel_type(self, channel_id: int, channel_type: str) -> None:
        self._require()
        await self._channels.set_channel_type(channel_id, channel_type)

    async def update_channel_meta(
        self, channel_id: int, *, username: str | None, title: str | None
    ) -> None:
        self._require()
        await self._channels.update_channel_meta(channel_id, username=username, title=title)

    async def get_forum_topics(self, channel_id: int) -> list[dict]:
        self._require()
        return await self._channels.get_forum_topics(channel_id)

    async def upsert_forum_topics(self, channel_id: int, topics: list[dict]) -> None:
        self._require()
        await self._channels.upsert_forum_topics(channel_id, topics)

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
        topic_id: int | None = None,
    ) -> tuple[list[Message], int]:
        self._require()
        return await self._messages.search_messages(
            query=query,
            channel_id=channel_id,
            date_from=date_from,
            date_to=date_to,
            limit=limit,
            offset=offset,
            topic_id=topic_id,
        )

    async def delete_messages_for_channel(self, channel_id: int) -> int:
        self._require()
        return await self._messages.delete_messages_for_channel(channel_id)

    async def get_stats(self) -> dict:
        self._require()
        return await self._messages.get_stats()

    async def get_notification_queries(
        self, active_only: bool = True
    ) -> list[SearchQuery]:
        self._require()
        return await self._search_queries.get_notification_queries(active_only)

    async def create_collection_task(
        self,
        channel_id: int,
        channel_title: str | None,
        *,
        channel_username: str | None = None,
        run_after: datetime | None = None,
        payload: dict[str, Any] | None = None,
        parent_task_id: int | None = None,
    ) -> int:
        self._require()
        return await self._tasks.create_collection_task(
            channel_id,
            channel_title,
            channel_username=channel_username,
            run_after=run_after,
            payload=payload,
            parent_task_id=parent_task_id,
        )

    async def update_collection_task_progress(self, task_id: int, messages_collected: int) -> None:
        self._require()
        await self._tasks.update_collection_task_progress(task_id, messages_collected)

    async def update_collection_task(
        self,
        task_id: int,
        status: CollectionTaskStatus | str,
        messages_collected: int | None = None,
        error: str | None = None,
        note: str | None = None,
    ) -> None:
        self._require()
        await self._tasks.update_collection_task(task_id, status, messages_collected, error, note)

    async def get_collection_task(self, task_id: int) -> CollectionTask | None:
        self._require()
        return await self._tasks.get_collection_task(task_id)

    async def get_collection_tasks(self, limit: int = 20) -> list[CollectionTask]:
        self._require()
        return await self._tasks.get_collection_tasks(limit)

    async def count_collection_tasks(self, status_filter: str | None = None) -> int:
        self._require()
        return await self._tasks.count_collection_tasks(status_filter)

    async def get_collection_tasks_paginated(
        self, limit: int = 20, offset: int = 0, status_filter: str | None = None
    ) -> tuple[list[CollectionTask], int]:
        self._require()
        return await self._tasks.get_collection_tasks_paginated(limit, offset, status_filter)

    async def get_active_collection_tasks_for_channel(
        self,
        channel_id: int,
    ) -> list[CollectionTask]:
        self._require()
        return await self._tasks.get_active_collection_tasks_for_channel(channel_id)

    async def get_channel_ids_with_active_tasks(self) -> set[int]:
        self._require()
        return await self._tasks.get_channel_ids_with_active_tasks()

    async def get_active_stats_task(self) -> CollectionTask | None:
        self._require()
        return await self._tasks.get_active_stats_task()

    async def claim_next_due_stats_task(self, now: datetime) -> CollectionTask | None:
        self._require()
        return await self._tasks.claim_next_due_stats_task(now)

    async def create_stats_task(
        self,
        payload: StatsAllTaskPayload,
        *,
        run_after: datetime | None = None,
        parent_task_id: int | None = None,
    ) -> int:
        self._require()
        return await self._tasks.create_stats_task(
            payload,
            run_after=run_after,
            parent_task_id=parent_task_id,
        )

    async def create_stats_continuation_task(
        self,
        *,
        payload: StatsAllTaskPayload,
        run_after: datetime | None,
        parent_task_id: int,
    ) -> int:
        self._require()
        return await self._tasks.create_stats_continuation_task(
            payload=payload,
            run_after=run_after,
            parent_task_id=parent_task_id,
        )

    async def get_pending_channel_tasks(self) -> list[CollectionTask]:
        self._require()
        return await self._tasks.get_pending_channel_tasks()

    async def fail_running_collection_tasks_on_startup(self) -> int:
        self._require()
        return await self._tasks.fail_running_collection_tasks_on_startup()

    async def requeue_running_stats_tasks_on_startup(self, now: datetime) -> int:
        self._require()
        return await self._tasks.requeue_running_stats_tasks_on_startup(now)

    async def cancel_collection_task(self, task_id: int, note: str | None = None) -> bool:
        self._require()
        return await self._tasks.cancel_collection_task(task_id, note=note)

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

    async def get_notification_bot(self, tg_user_id: int) -> NotificationBot | None:
        self._require()
        assert self._notification_bots is not None
        return await self._notification_bots.get_bot(tg_user_id)

    async def save_notification_bot(self, bot: NotificationBot) -> int:
        self._require()
        assert self._notification_bots is not None
        return await self._notification_bots.save_bot(bot)

    async def delete_notification_bot(self, tg_user_id: int) -> None:
        self._require()
        assert self._notification_bots is not None
        await self._notification_bots.delete_bot(tg_user_id)

    # ── Agent chat ─────────────────────────────────────────────────────────────

    async def create_agent_thread(self, title: str = "Новый тред") -> int:
        self._require()
        assert self._db is not None
        cur = await self._db.execute(
            "INSERT INTO agent_threads (title) VALUES (?)", (title,)
        )
        await self._db.commit()
        assert cur.lastrowid is not None
        return cur.lastrowid

    async def get_agent_threads(self) -> list[dict]:
        self._require()
        assert self._db is not None
        cur = await self._db.execute(
            "SELECT id, title, created_at FROM agent_threads ORDER BY created_at DESC"
        )
        rows = await cur.fetchall()
        return [dict(r) for r in rows]

    async def get_agent_thread(self, thread_id: int) -> dict | None:
        self._require()
        assert self._db is not None
        cur = await self._db.execute(
            "SELECT id, title, created_at FROM agent_threads WHERE id = ?", (thread_id,)
        )
        row = await cur.fetchone()
        return dict(row) if row else None

    async def rename_agent_thread(self, thread_id: int, title: str) -> None:
        self._require()
        assert self._db is not None
        await self._db.execute(
            "UPDATE agent_threads SET title = ? WHERE id = ?", (title, thread_id)
        )
        await self._db.commit()

    async def delete_agent_thread(self, thread_id: int) -> None:
        self._require()
        assert self._db is not None
        await self._db.execute("DELETE FROM agent_threads WHERE id = ?", (thread_id,))
        await self._db.commit()

    async def get_agent_messages(self, thread_id: int) -> list[dict]:
        self._require()
        assert self._db is not None
        cur = await self._db.execute(
            "SELECT id, thread_id, role, content, created_at"
            " FROM agent_messages WHERE thread_id = ? ORDER BY created_at ASC",
            (thread_id,),
        )
        rows = await cur.fetchall()
        return [dict(r) for r in rows]

    async def save_agent_message(self, thread_id: int, role: str, content: str) -> None:
        self._require()
        assert self._db is not None
        await self._db.execute(
            "INSERT INTO agent_messages (thread_id, role, content) VALUES (?, ?, ?)",
            (thread_id, role, content),
        )
        await self._db.commit()

    async def delete_last_agent_exchange(self, thread_id: int) -> None:
        """Delete the last user message and any assistant reply from a thread."""
        self._require()
        assert self._db is not None
        # Find the last user message
        cur = await self._db.execute(
            "SELECT id FROM agent_messages"
            " WHERE thread_id = ? AND role = 'user'"
            " ORDER BY id DESC LIMIT 1",
            (thread_id,),
        )
        row = await cur.fetchone()
        if row is None:
            return
        last_user_id = row["id"]
        # Delete that user message and any messages after it (assistant reply)
        await self._db.execute(
            "DELETE FROM agent_messages WHERE thread_id = ? AND id >= ?",
            (thread_id, last_user_id),
        )
        await self._db.commit()
