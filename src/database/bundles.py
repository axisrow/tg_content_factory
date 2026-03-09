from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING

from src.database.repositories.accounts import AccountsRepository
from src.database.repositories.channel_stats import ChannelStatsRepository
from src.database.repositories.channels import ChannelsRepository
from src.database.repositories.collection_tasks import CollectionTasksRepository
from src.database.repositories.filters import FilterRepository
from src.database.repositories.messages import MessagesRepository
from src.database.repositories.notification_bots import NotificationBotsRepository
from src.database.repositories.search_log import SearchLogRepository
from src.database.repositories.search_queries import SearchQueriesRepository
from src.database.repositories.settings import SettingsRepository
from src.models import (
    Account,
    Channel,
    ChannelStats,
    CollectionTask,
    CollectionTaskStatus,
    Message,
    NotificationBot,
    SearchQuery,
    SearchQueryDailyStat,
    StatsAllTaskPayload,
)

if TYPE_CHECKING:
    from src.database.facade import Database


@dataclass(frozen=True)
class DatabaseRepositories:
    accounts: AccountsRepository
    channels: ChannelsRepository
    messages: MessagesRepository
    tasks: CollectionTasksRepository
    search_log: SearchLogRepository
    channel_stats: ChannelStatsRepository
    settings: SettingsRepository
    filters: FilterRepository
    notification_bots: NotificationBotsRepository
    search_queries: SearchQueriesRepository


@dataclass(frozen=True)
class AccountBundle:
    accounts: AccountsRepository

    @classmethod
    def from_database(cls, db: "Database") -> "AccountBundle":
        return cls(db.repos.accounts)

    async def list_accounts(self, active_only: bool = False) -> list[Account]:
        return await self.accounts.get_accounts(active_only)

    async def add_account(self, account: Account) -> int:
        return await self.accounts.add_account(account)

    async def set_active(self, account_id: int, active: bool) -> None:
        await self.accounts.set_account_active(account_id, active)

    async def delete_account(self, account_id: int) -> None:
        await self.accounts.delete_account(account_id)

    async def update_flood(self, phone: str, until) -> None:
        await self.accounts.update_account_flood(phone, until)

    async def update_premium(self, phone: str, is_premium: bool) -> None:
        await self.accounts.update_account_premium(phone, is_premium)


@dataclass(frozen=True)
class ChannelBundle:
    channels: ChannelsRepository
    channel_stats: ChannelStatsRepository
    tasks: CollectionTasksRepository

    @classmethod
    def from_database(cls, db: "Database") -> "ChannelBundle":
        repos = db.repos
        return cls(repos.channels, repos.channel_stats, repos.tasks)

    async def add_channel(self, channel: Channel) -> int:
        return await self.channels.add_channel(channel)

    async def list_channels(
        self,
        active_only: bool = False,
        include_filtered: bool = True,
    ) -> list[Channel]:
        return await self.channels.get_channels(active_only, include_filtered)

    async def list_channels_with_counts(
        self,
        active_only: bool = False,
        include_filtered: bool = True,
    ) -> list[Channel]:
        return await self.channels.get_channels_with_counts(active_only, include_filtered)

    async def get_by_pk(self, pk: int) -> Channel | None:
        return await self.channels.get_channel_by_pk(pk)

    async def get_by_channel_id(self, channel_id: int) -> Channel | None:
        return await self.channels.get_channel_by_channel_id(channel_id)

    async def set_active(self, pk: int, active: bool) -> None:
        await self.channels.set_channel_active(pk, active)

    async def set_type(self, channel_id: int, channel_type: str) -> None:
        await self.channels.set_channel_type(channel_id, channel_type)

    async def update_last_id(self, channel_id: int, last_id: int) -> None:
        await self.channels.update_channel_last_id(channel_id, last_id)

    async def update_meta(
        self,
        channel_id: int,
        *,
        username: str | None,
        title: str | None,
    ) -> None:
        await self.channels.update_channel_meta(channel_id, username=username, title=title)

    async def set_filtered_bulk(
        self,
        updates: list[tuple[int, str]],
        *,
        commit: bool = True,
    ) -> int:
        return await self.channels.set_filtered_bulk(updates, commit=commit)

    async def reset_all_filters(self, *, commit: bool = True) -> int:
        return await self.channels.reset_all_filters(commit=commit)

    async def delete_channel(self, pk: int) -> None:
        await self.channels.delete_channel(pk)

    async def save_stats(self, stats: ChannelStats) -> int:
        return await self.channel_stats.save_channel_stats(stats)

    async def get_stats(self, channel_id: int, limit: int = 1) -> list[ChannelStats]:
        return await self.channel_stats.get_channel_stats(channel_id, limit)

    async def get_latest_stats_for_all(self) -> dict[int, ChannelStats]:
        return await self.channel_stats.get_latest_stats_for_all()

    async def create_collection_task(
        self,
        channel_id: int,
        channel_title: str | None,
        *,
        channel_username: str | None = None,
        run_after: datetime | None = None,
        payload: dict | None = None,
        parent_task_id: int | None = None,
    ) -> int:
        return await self.tasks.create_collection_task(
            channel_id,
            channel_title,
            channel_username=channel_username,
            run_after=run_after,
            payload=payload,
            parent_task_id=parent_task_id,
        )

    async def update_collection_task(
        self,
        task_id: int,
        status: CollectionTaskStatus | str,
        messages_collected: int | None = None,
        error: str | None = None,
        note: str | None = None,
    ) -> None:
        await self.tasks.update_collection_task(
            task_id,
            status,
            messages_collected,
            error,
            note,
        )

    async def update_collection_task_progress(self, task_id: int, messages_collected: int) -> None:
        await self.tasks.update_collection_task_progress(task_id, messages_collected)

    async def get_collection_task(self, task_id: int) -> CollectionTask | None:
        return await self.tasks.get_collection_task(task_id)

    async def get_collection_tasks(self, limit: int = 20) -> list[CollectionTask]:
        return await self.tasks.get_collection_tasks(limit)

    async def get_active_collection_tasks_for_channel(
        self,
        channel_id: int,
    ) -> list[CollectionTask]:
        return await self.tasks.get_active_collection_tasks_for_channel(channel_id)

    async def get_channel_ids_with_active_tasks(self) -> set[int]:
        return await self.tasks.get_channel_ids_with_active_tasks()

    async def get_active_stats_task(self) -> CollectionTask | None:
        return await self.tasks.get_active_stats_task()

    async def claim_next_due_stats_task(self, now: datetime) -> CollectionTask | None:
        return await self.tasks.claim_next_due_stats_task(now)

    async def create_stats_task(
        self,
        payload: StatsAllTaskPayload,
        *,
        run_after: datetime | None = None,
        parent_task_id: int | None = None,
    ) -> int:
        return await self.tasks.create_stats_task(
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
        return await self.tasks.create_stats_continuation_task(
            payload=payload,
            run_after=run_after,
            parent_task_id=parent_task_id,
        )

    async def get_pending_channel_tasks(self) -> list[CollectionTask]:
        return await self.tasks.get_pending_channel_tasks()

    async def fail_running_collection_tasks_on_startup(self) -> int:
        return await self.tasks.fail_running_collection_tasks_on_startup()

    async def requeue_running_stats_tasks_on_startup(self, now: datetime) -> int:
        return await self.tasks.requeue_running_stats_tasks_on_startup(now)

    async def cancel_collection_task(self, task_id: int, note: str | None = None) -> bool:
        return await self.tasks.cancel_collection_task(task_id, note=note)


@dataclass(frozen=True)
class CollectionBundle:
    channels: ChannelsRepository
    messages: MessagesRepository
    filters: FilterRepository
    settings: SettingsRepository
    search_queries: SearchQueriesRepository
    tasks: CollectionTasksRepository
    channel_stats: ChannelStatsRepository

    @classmethod
    def from_database(cls, db: "Database") -> "CollectionBundle":
        repos = db.repos
        return cls(
            repos.channels,
            repos.messages,
            repos.filters,
            repos.settings,
            repos.search_queries,
            repos.tasks,
            repos.channel_stats,
        )

    async def list_channels(
        self,
        active_only: bool = False,
        include_filtered: bool = True,
    ) -> list[Channel]:
        return await self.channels.get_channels(active_only, include_filtered)

    async def get_by_pk(self, pk: int) -> Channel | None:
        return await self.channels.get_channel_by_pk(pk)

    async def get_by_channel_id(self, channel_id: int) -> Channel | None:
        return await self.channels.get_channel_by_channel_id(channel_id)

    async def update_last_id(self, channel_id: int, last_id: int) -> None:
        await self.channels.update_channel_last_id(channel_id, last_id)

    async def update_meta(
        self,
        channel_id: int,
        *,
        username: str | None,
        title: str | None,
    ) -> None:
        await self.channels.update_channel_meta(channel_id, username=username, title=title)

    async def set_active(self, pk: int, active: bool) -> None:
        await self.channels.set_channel_active(pk, active)

    async def set_type(self, channel_id: int, channel_type: str) -> None:
        await self.channels.set_channel_type(channel_id, channel_type)

    async def set_filtered_bulk(
        self,
        updates: list[tuple[int, str]],
        *,
        commit: bool = True,
    ) -> int:
        return await self.channels.set_filtered_bulk(updates, commit=commit)

    async def reset_all_filters(self, *, commit: bool = True) -> int:
        return await self.channels.reset_all_filters(commit=commit)

    async def insert_message(self, msg: Message) -> bool:
        return await self.messages.insert_message(msg)

    async def insert_messages_batch(self, messages: list[Message]) -> int:
        return await self.messages.insert_messages_batch(messages)

    async def search_messages(
        self,
        query: str = "",
        channel_id: int | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[list[Message], int]:
        return await self.messages.search_messages(
            query=query,
            channel_id=channel_id,
            date_from=date_from,
            date_to=date_to,
            limit=limit,
            offset=offset,
        )

    async def delete_messages_for_channel(self, channel_id: int) -> int:
        return await self.messages.delete_messages_for_channel(channel_id)

    async def get_message_stats(self) -> dict:
        return await self.messages.get_stats()

    async def count_matching_prefixes_in_other_channels(
        self,
        channel_id: int,
        prefixes: list[str],
    ) -> int:
        return await self.filters.count_matching_prefixes_in_other_channels(channel_id, prefixes)

    async def get_setting(self, key: str) -> str | None:
        return await self.settings.get_setting(key)

    async def set_setting(self, key: str, value: str) -> None:
        await self.settings.set_setting(key, value)

    async def list_notification_queries(
        self, active_only: bool = True
    ) -> list[SearchQuery]:
        return await self.search_queries.get_notification_queries(active_only)

    async def get_channel_stats(self, channel_id: int, limit: int = 1) -> list[ChannelStats]:
        return await self.channel_stats.get_channel_stats(channel_id, limit)

    async def create_collection_task(
        self,
        channel_id: int,
        channel_title: str | None,
        *,
        channel_username: str | None = None,
        run_after: datetime | None = None,
        payload: dict | None = None,
        parent_task_id: int | None = None,
    ) -> int:
        return await self.tasks.create_collection_task(
            channel_id,
            channel_title,
            channel_username=channel_username,
            run_after=run_after,
            payload=payload,
            parent_task_id=parent_task_id,
        )


@dataclass(frozen=True)
class NotificationBundle:
    accounts: AccountsRepository
    settings: SettingsRepository
    notification_bots: NotificationBotsRepository

    @classmethod
    def from_database(cls, db: "Database") -> "NotificationBundle":
        repos = db.repos
        return cls(repos.accounts, repos.settings, repos.notification_bots)

    async def list_accounts(self, active_only: bool = False) -> list[Account]:
        return await self.accounts.get_accounts(active_only)

    async def get_setting(self, key: str) -> str | None:
        return await self.settings.get_setting(key)

    async def set_setting(self, key: str, value: str) -> None:
        await self.settings.set_setting(key, value)

    async def get_bot(self, tg_user_id: int) -> NotificationBot | None:
        return await self.notification_bots.get_bot(tg_user_id)

    async def save_bot(self, bot: NotificationBot) -> int:
        return await self.notification_bots.save_bot(bot)

    async def delete_bot(self, tg_user_id: int) -> None:
        await self.notification_bots.delete_bot(tg_user_id)


@dataclass(frozen=True)
class SearchBundle:
    messages: MessagesRepository
    search_log: SearchLogRepository
    channels: ChannelsRepository

    @classmethod
    def from_database(cls, db: "Database") -> "SearchBundle":
        repos = db.repos
        return cls(repos.messages, repos.search_log, repos.channels)

    async def search_messages(
        self,
        query: str = "",
        channel_id: int | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[list[Message], int]:
        return await self.messages.search_messages(
            query=query,
            channel_id=channel_id,
            date_from=date_from,
            date_to=date_to,
            limit=limit,
            offset=offset,
        )

    async def add_channel(self, channel: Channel) -> int:
        return await self.channels.add_channel(channel)

    async def insert_messages_batch(self, messages: list[Message]) -> int:
        return await self.messages.insert_messages_batch(messages)

    async def log_search(self, phone: str, query: str, results_count: int) -> None:
        await self.search_log.log_search(phone, query, results_count)

    async def get_recent_searches(self, limit: int = 20) -> list[dict]:
        return await self.search_log.get_recent_searches(limit)


@dataclass(frozen=True)
class SchedulerBundle:
    settings: SettingsRepository
    search_queries: SearchQueriesRepository
    tasks: CollectionTasksRepository
    search_log: SearchLogRepository

    @classmethod
    def from_database(cls, db: "Database") -> "SchedulerBundle":
        repos = db.repos
        return cls(repos.settings, repos.search_queries, repos.tasks, repos.search_log)

    async def get_setting(self, key: str) -> str | None:
        return await self.settings.get_setting(key)

    async def set_setting(self, key: str, value: str) -> None:
        await self.settings.set_setting(key, value)

    async def list_notification_queries(
        self, active_only: bool = True
    ) -> list[SearchQuery]:
        return await self.search_queries.get_notification_queries(active_only)

    async def get_collection_tasks(self, limit: int = 20) -> list[CollectionTask]:
        return await self.tasks.get_collection_tasks(limit)

    async def get_recent_searches(self, limit: int = 20) -> list[dict]:
        return await self.search_log.get_recent_searches(limit)


@dataclass(frozen=True)
class SearchQueryBundle:
    search_queries: SearchQueriesRepository
    messages: MessagesRepository

    @classmethod
    def from_database(cls, db: "Database") -> "SearchQueryBundle":
        repos = db.repos
        return cls(repos.search_queries, repos.messages)

    async def add(self, sq: SearchQuery) -> int:
        return await self.search_queries.add(sq)

    async def get_all(self, active_only: bool = False) -> list[SearchQuery]:
        return await self.search_queries.get_all(active_only)

    async def get_by_id(self, sq_id: int) -> SearchQuery | None:
        return await self.search_queries.get_by_id(sq_id)

    async def set_active(self, sq_id: int, active: bool) -> None:
        await self.search_queries.set_active(sq_id, active)

    async def update(self, sq_id: int, sq: SearchQuery) -> None:
        await self.search_queries.update(sq_id, sq)

    async def delete(self, sq_id: int) -> None:
        await self.search_queries.delete(sq_id)

    async def count_fts_matches(self, query: str) -> int:
        return await self.messages.count_fts_matches(query)

    async def record_stat(self, query_id: int, match_count: int) -> None:
        await self.search_queries.record_stat(query_id, match_count)

    async def get_daily_stats(
        self, query_id: int, days: int = 30
    ) -> list[SearchQueryDailyStat]:
        return await self.search_queries.get_daily_stats(query_id, days)

    async def get_stats_for_all(self, days: int = 30) -> dict[int, list[SearchQueryDailyStat]]:
        return await self.search_queries.get_stats_for_all(days)

    async def get_fts_daily_stats(
        self, query: str, days: int = 30
    ) -> list[SearchQueryDailyStat]:
        return await self.messages.get_fts_daily_stats(query, days)

    async def count_fts_matches_for_query(self, sq: SearchQuery) -> int:
        return await self.messages.count_fts_matches_for_query(sq)

    async def get_fts_daily_stats_for_query(
        self, sq: SearchQuery, days: int = 30
    ) -> list[SearchQueryDailyStat]:
        return await self.messages.get_fts_daily_stats_for_query(sq, days)

    async def get_last_recorded_at(self, query_id: int) -> str | None:
        return await self.search_queries.get_last_recorded_at(query_id)

    async def get_last_recorded_at_all(self) -> dict[int, str]:
        return await self.search_queries.get_last_recorded_at_all()
