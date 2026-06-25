"""Типизированные бандлы поверх репозиториев — узкие фасады доступа к БД.

`DatabaseRepositories` — плоский агрегатор всех репозиториев, который `Database`
отдаёт как `db.repos.*` (единая точка доступа к слою хранения). Остальные
`*Bundle` — это доменно-узкие срезы: каждый собирает только те репозитории,
что нужны одному потребителю (account-сервис, сбор, нотификации, поиск,
шедулер, пайплайны, photo-loader), и предоставляет ему методы-обёртки с
осмысленными именами вместо прямого доступа к `db.repos`. Так зависимости
сервиса от хранилища видны в одном месте, а сам сервис не тянет весь набор
репозиториев. Бандлы `frozen`/иммутабельны и создаются через `from_database`.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING

from src.database.repositories.accounts import AccountsRepository
from src.database.repositories.channel_ratings import ChannelRatingsRepository
from src.database.repositories.channel_stats import ChannelStatsRepository
from src.database.repositories.channels import ChannelsRepository
from src.database.repositories.collection_tasks import CollectionTasksRepository
from src.database.repositories.content_pipelines import ContentPipelinesRepository
from src.database.repositories.dialog_cache import DialogCacheRepository
from src.database.repositories.filters import FilterRepository
from src.database.repositories.generated_images import GeneratedImagesRepository
from src.database.repositories.generation_runs import GenerationRunsRepository
from src.database.repositories.messages import MessageSearchPage, MessagesRepository
from src.database.repositories.notification_bots import NotificationBotsRepository
from src.database.repositories.notified_messages import NotifiedMessagesRepository
from src.database.repositories.photo_loader import PhotoLoaderRepository
from src.database.repositories.pipeline_action_log import PipelineActionLogRepository
from src.database.repositories.pipeline_templates import PipelineTemplatesRepository
from src.database.repositories.runtime_snapshots import RuntimeSnapshotsRepository
from src.database.repositories.search_log import SearchLogRepository
from src.database.repositories.search_queries import SearchQueriesRepository
from src.database.repositories.settings import SettingsRepository
from src.database.repositories.telegram_commands import TelegramCommandsRepository
from src.models import (
    Account,
    AccountSummary,
    Channel,
    ChannelStats,
    CollectionTask,
    CollectionTaskStatus,
    ContentPipeline,
    Message,
    NotificationBot,
    PhotoAutoUploadJob,
    PhotoBatch,
    PhotoBatchItem,
    PhotoBatchStatus,
    PhotoSendMode,
    PipelineSource,
    PipelineTarget,
    SearchParams,
    SearchQuery,
    SearchQueryDailyStat,
    StatsAllTaskPayload,
)

if TYPE_CHECKING:
    from src.database.facade import Database


@dataclass(frozen=True)
class DatabaseRepositories:
    """Плоский агрегатор всех репозиториев, доступный как `db.repos.*`.

    Единая точка доступа к слою хранения: каждый атрибут — отдельный репозиторий
    своего домена (аккаунты, каналы, сообщения, задачи сбора и т.д.). Сервисы и
    web/CLI читают данные через `db.repos.<repo>.<method>()`, а `*Bundle` ниже
    собираются из подмножества этих же репозиториев.
    """

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
    photo_loader: PhotoLoaderRepository
    dialog_cache: DialogCacheRepository
    content_pipelines: ContentPipelinesRepository
    generation_runs: GenerationRunsRepository
    generated_images: GeneratedImagesRepository
    pipeline_templates: PipelineTemplatesRepository
    telegram_commands: TelegramCommandsRepository
    runtime_snapshots: RuntimeSnapshotsRepository
    channel_ratings: ChannelRatingsRepository
    pipeline_action_log: PipelineActionLogRepository
    notified_messages: NotifiedMessagesRepository


@dataclass(frozen=True)
class AccountBundle:
    """Фасад для управления Telegram-аккаунтами пула (CRUD + флаги состояния)."""

    accounts: AccountsRepository

    @classmethod
    def from_database(cls, db: "Database") -> "AccountBundle":
        """Собрать бандл из репозитория аккаунтов общего `Database`."""
        return cls(db.repos.accounts)

    async def list_accounts(self, active_only: bool = False) -> list[Account]:
        """Список аккаунтов (с секретом сессии); `active_only` — только активные."""
        return await self.accounts.get_accounts(active_only)

    async def list_live_usable_accounts(self, active_only: bool = False) -> list[Account]:
        """Аккаунты, пригодные для живого подключения (валидная читаемая сессия)."""
        return await self.accounts.get_live_usable_accounts(active_only)

    async def list_account_summaries(self, active_only: bool = False) -> list[AccountSummary]:
        """Безопасные для UI сводки аккаунтов (`AccountSummary`, без `session_string`)."""
        return await self.accounts.get_account_summaries(active_only)

    async def add_account(self, account: Account) -> int:
        """Добавить аккаунт; возвращает его id."""
        return await self.accounts.add_account(account)

    async def set_active(self, account_id: int, active: bool) -> None:
        """Включить/выключить аккаунт по id."""
        await self.accounts.set_account_active(account_id, active)

    async def delete_account(self, account_id: int) -> None:
        """Удалить аккаунт по id."""
        await self.accounts.delete_account(account_id)

    async def update_flood(self, phone: str, until) -> None:
        """Записать `flood_wait_until` для аккаунта (момент окончания FLOOD_WAIT)."""
        await self.accounts.update_account_flood(phone, until)

    async def update_premium(self, phone: str, is_premium: bool) -> None:
        """Обновить флаг Premium-статуса аккаунта."""
        await self.accounts.update_account_premium(phone, is_premium)


@dataclass(frozen=True)
class ChannelBundle:
    """Фасад управления каналами: сам канал, его статистика и задачи сбора.

    Сводит вместе репозитории `channels`, `channel_stats` и `tasks`, чтобы
    операции над каналом (CRUD, метаданные, фильтрация) и связанный с ним сбор
    (создание/обновление collection-задач) жили за одним интерфейсом.
    """

    channels: ChannelsRepository
    channel_stats: ChannelStatsRepository
    tasks: CollectionTasksRepository

    @classmethod
    def from_database(cls, db: "Database") -> "ChannelBundle":
        """Собрать бандл из репозиториев каналов/статистики/задач общего `Database`."""
        repos = db.repos
        return cls(repos.channels, repos.channel_stats, repos.tasks)

    async def add_channel(self, channel: Channel) -> int:
        """Добавить канал; возвращает его pk (первичный ключ БД)."""
        return await self.channels.add_channel(channel)

    async def list_channels(
        self,
        active_only: bool = False,
        include_filtered: bool = True,
    ) -> list[Channel]:
        """Список каналов; `active_only` — только активные, `include_filtered` — с отфильтрованными."""
        return await self.channels.get_channels(active_only, include_filtered)

    async def list_channels_with_counts(
        self,
        active_only: bool = False,
        include_filtered: bool = True,
    ) -> list[Channel]:
        """Каналы с проставленным `message_count` (число собранных сообщений)."""
        return await self.channels.get_channels_with_counts(active_only, include_filtered)

    async def get_by_pk(self, pk: int) -> Channel | None:
        """Канал по pk (первичный ключ БД), либо None."""
        return await self.channels.get_channel_by_pk(pk)

    async def get_by_channel_id(self, channel_id: int) -> Channel | None:
        """Канал по Telegram `channel_id`, либо None."""
        return await self.channels.get_channel_by_channel_id(channel_id)

    async def set_active(self, pk: int, active: bool) -> None:
        """Включить/выключить канал по pk."""
        await self.channels.set_channel_active(pk, active)

    async def set_type(self, channel_id: int, channel_type: str) -> None:
        """Задать тип канала (channel/supergroup/...) по Telegram `channel_id`."""
        await self.channels.set_channel_type(channel_id, channel_type)

    async def update_last_id(self, channel_id: int, last_id: int) -> None:
        """Обновить `last_collected_id` канала (граница инкрементального сбора)."""
        await self.channels.update_channel_last_id(channel_id, last_id)

    async def update_meta(
        self,
        channel_id: int,
        *,
        username: str | None,
        title: str | None,
    ) -> None:
        """Обновить базовые метаданные канала — username и title."""
        await self.channels.update_channel_meta(channel_id, username=username, title=title)

    async def update_channel_full_meta(
        self,
        channel_id: int,
        *,
        about: str | None,
        linked_chat_id: int | None,
        has_comments: bool,
    ) -> None:
        """Обновить расширенные метаданные: описание, привязанный чат, наличие комментариев."""
        await self.channels.update_channel_full_meta(
            channel_id, about=about, linked_chat_id=linked_chat_id, has_comments=has_comments
        )

    async def set_filtered_bulk(
        self,
        updates: list[tuple[int, str]],
        *,
        commit: bool = True,
    ) -> int:
        """Пакетно проставить флаги фильтрации каналам `(channel_id, flags_csv)`; вернуть число изменённых."""
        return await self.channels.set_filtered_bulk(updates, commit=commit)

    async def reset_all_filters(self, *, commit: bool = True) -> int:
        """Снять фильтрацию со всех каналов; вернуть число сброшенных."""
        return await self.channels.reset_all_filters(commit=commit)

    async def delete_channel(self, pk: int) -> None:
        """Удалить канал по pk."""
        await self.channels.delete_channel(pk)

    async def save_stats(self, stats: ChannelStats) -> int:
        """Сохранить снимок статистики канала; возвращает id записи."""
        return await self.channel_stats.save_channel_stats(stats)

    async def get_stats(self, channel_id: int, limit: int = 1) -> list[ChannelStats]:
        """Последние `limit` снимков статистики канала (от новых к старым)."""
        return await self.channel_stats.get_channel_stats(channel_id, limit)

    async def get_latest_stats_for_all(self) -> dict[int, ChannelStats]:
        """Свежайший снимок статистики по каждому каналу: `{channel_id: ChannelStats}`."""
        return await self.channel_stats.get_latest_stats_for_all()

    async def get_previous_subscriber_counts(self) -> dict[int, int | None]:
        """Предыдущее (не последнее) число подписчиков по каналам — для расчёта дельты роста."""
        return await self.channel_stats.get_previous_subscriber_counts()

    async def get_latest_and_previous_stats(
        self,
    ) -> tuple[dict[int, ChannelStats], dict[int, int | None]]:
        """Пара (последние снимки статистики, предыдущие счётчики подписчиков) одним проходом."""
        return await self.channel_stats.get_latest_and_previous_stats()

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
        """Создать задачу сбора для канала; возвращает её id."""
        return await self.tasks.create_collection_task(
            channel_id,
            channel_title,
            channel_username=channel_username,
            run_after=run_after,
            payload=payload,
            parent_task_id=parent_task_id,
        )

    async def create_collection_task_if_not_active(
        self,
        channel_id: int,
        channel_title: str | None,
        *,
        channel_username: str | None = None,
        run_after: datetime | None = None,
        payload: dict | None = None,
        parent_task_id: int | None = None,
    ) -> int | None:
        """Создать задачу сбора, только если у канала ещё нет активной; иначе None (анти-дубль)."""
        return await self.tasks.create_collection_task_if_not_active(
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
        run_after: datetime | None = None,
    ) -> None:
        """Обновить статус задачи сбора и сопутствующие поля (счётчик, ошибка, заметка, run_after)."""
        await self.tasks.update_collection_task(
            task_id,
            status,
            messages_collected,
            error,
            note,
            run_after,
        )

    async def reschedule_collection_task(
        self,
        task_id: int,
        *,
        run_after: datetime,
        note: str | None = None,
        messages_collected: int = 0,
    ) -> None:
        """Перенести задачу сбора на `run_after` (например после FLOOD_WAIT)."""
        await self.tasks.reschedule_collection_task(
            task_id,
            run_after=run_after,
            note=note,
            messages_collected=messages_collected,
        )

    async def reset_collection_task_to_pending(
        self,
        task_id: int,
        *,
        note: str | None = None,
    ) -> None:
        """Вернуть задачу сбора в статус PENDING (повторная постановка в очередь)."""
        await self.tasks.reset_collection_task_to_pending(task_id, note=note)

    async def update_collection_task_progress(self, task_id: int, messages_collected: int) -> None:
        """Записать прогресс задачи сбора (число собранных сообщений на текущий момент)."""
        await self.tasks.update_collection_task_progress(task_id, messages_collected)

    async def persist_stats_progress(
        self,
        task_id: int,
        *,
        payload: StatsAllTaskPayload,
        messages_collected: int,
    ) -> None:
        """Сохранить промежуточный прогресс STATS_ALL-задачи (payload + счётчик) между шагами."""
        await self.tasks.persist_stats_progress(task_id, payload=payload, messages_collected=messages_collected)

    async def get_collection_task(self, task_id: int) -> CollectionTask | None:
        """Задача сбора по id, либо None."""
        return await self.tasks.get_collection_task(task_id)

    async def get_collection_tasks(self, limit: int = 20) -> list[CollectionTask]:
        """Последние `limit` задач сбора (от новых к старым)."""
        return await self.tasks.get_collection_tasks(limit)

    async def count_collection_tasks(self, status_filter: str | None = None) -> int:
        """Число задач сбора, опционально только в статусе `status_filter`."""
        return await self.tasks.count_collection_tasks(status_filter)

    async def get_collection_tasks_paginated(
        self, limit: int = 20, offset: int = 0, status_filter: str | None = None
    ) -> tuple[list[CollectionTask], int]:
        """Страница задач сбора и общее их число: `(список, total)`."""
        return await self.tasks.get_collection_tasks_paginated(limit, offset, status_filter)

    async def get_active_collection_tasks_for_channel(
        self,
        channel_id: int,
    ) -> list[CollectionTask]:
        """Активные (pending/running) задачи сбора конкретного канала."""
        return await self.tasks.get_active_collection_tasks_for_channel(channel_id)

    async def get_channel_ids_with_active_tasks(self) -> set[int]:
        """Множество `channel_id`, у которых есть активная задача сбора."""
        return await self.tasks.get_channel_ids_with_active_tasks()

    async def get_active_stats_task(self) -> CollectionTask | None:
        """Текущая активная STATS_ALL-задача (сбор статистики по всем каналам), либо None."""
        return await self.tasks.get_active_stats_task()

    async def create_stats_task(
        self,
        payload: StatsAllTaskPayload,
        *,
        run_after: datetime | None = None,
        parent_task_id: int | None = None,
    ) -> int:
        """Создать STATS_ALL-задачу с заданным payload; возвращает её id."""
        return await self.tasks.create_stats_task(
            payload,
            run_after=run_after,
            parent_task_id=parent_task_id,
        )

    async def reschedule_stats_task(
        self,
        task_id: int,
        *,
        payload: StatsAllTaskPayload,
        run_after: datetime,
        messages_collected: int,
    ) -> None:
        """Перенести STATS_ALL-задачу на `run_after`, сохранив обновлённый payload и прогресс."""
        return await self.tasks.reschedule_stats_task(
            task_id,
            payload=payload,
            run_after=run_after,
            messages_collected=messages_collected,
        )

    async def get_pending_channel_tasks(self) -> list[CollectionTask]:
        """Задачи сбора каналов в статусе PENDING (для постановки в очередь воркером)."""
        return await self.tasks.get_pending_channel_tasks()

    async def delete_pending_channel_tasks(self) -> int:
        """Удалить все PENDING-задачи сбора каналов; вернуть число удалённых."""
        return await self.tasks.delete_pending_channel_tasks()

    async def fail_running_collection_tasks_on_startup(self) -> int:
        """Пометить «зависшие» RUNNING-задачи как FAILED при старте; вернуть число затронутых."""
        return await self.tasks.fail_running_collection_tasks_on_startup()

    async def reset_orphaned_running_tasks(self) -> int:
        """Вернуть в PENDING осиротевшие RUNNING-задачи (воркер упал); вернуть число затронутых."""
        return await self.tasks.reset_orphaned_running_tasks()

    async def cancel_collection_task(self, task_id: int, note: str | None = None) -> bool:
        """Отменить задачу сбора; True, если отмена применена."""
        return await self.tasks.cancel_collection_task(task_id, note=note)


@dataclass(frozen=True)
class CollectionBundle:
    """Фасад для процесса сбора: каналы, запись сообщений, фильтры, настройки и задачи.

    Используется `Collector`/сервисом сбора: даёт доступ к каналам (чтение/мета),
    пакетной вставке собранных сообщений, фильтрам, настройкам, нотификационным
    запросам и постановке collection-задач — всё, что нужно одному циклу сбора.
    """

    channels: ChannelsRepository
    messages: MessagesRepository
    filters: FilterRepository
    settings: SettingsRepository
    search_queries: SearchQueriesRepository
    tasks: CollectionTasksRepository
    channel_stats: ChannelStatsRepository

    @classmethod
    def from_database(cls, db: "Database") -> "CollectionBundle":
        """Собрать бандл из репозиториев, нужных циклу сбора, общего `Database`."""
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
        """Список каналов; `active_only` — только активные, `include_filtered` — с отфильтрованными."""
        return await self.channels.get_channels(active_only, include_filtered)

    async def get_by_pk(self, pk: int) -> Channel | None:
        """Канал по pk (первичный ключ БД), либо None."""
        return await self.channels.get_channel_by_pk(pk)

    async def get_by_channel_id(self, channel_id: int) -> Channel | None:
        """Канал по Telegram `channel_id`, либо None."""
        return await self.channels.get_channel_by_channel_id(channel_id)

    async def update_last_id(self, channel_id: int, last_id: int) -> None:
        """Обновить `last_collected_id` канала (граница инкрементального сбора)."""
        await self.channels.update_channel_last_id(channel_id, last_id)

    async def update_meta(
        self,
        channel_id: int,
        *,
        username: str | None,
        title: str | None,
    ) -> None:
        """Обновить базовые метаданные канала — username и title."""
        await self.channels.update_channel_meta(channel_id, username=username, title=title)

    async def set_active(self, pk: int, active: bool) -> None:
        """Включить/выключить канал по pk."""
        await self.channels.set_channel_active(pk, active)

    async def set_type(self, channel_id: int, channel_type: str) -> None:
        """Задать тип канала по Telegram `channel_id`."""
        await self.channels.set_channel_type(channel_id, channel_type)

    async def set_filtered_bulk(
        self,
        updates: list[tuple[int, str]],
        *,
        commit: bool = True,
    ) -> int:
        """Пакетно проставить флаги фильтрации каналам `(channel_id, flags_csv)`; вернуть число изменённых."""
        return await self.channels.set_filtered_bulk(updates, commit=commit)

    async def reset_all_filters(self, *, commit: bool = True) -> int:
        """Снять фильтрацию со всех каналов; вернуть число сброшенных."""
        return await self.channels.reset_all_filters(commit=commit)

    async def insert_message(self, msg: Message) -> bool:
        """Вставить одно сообщение; True, если строка добавлена (False при дубле)."""
        return await self.messages.insert_message(msg)

    async def insert_messages_batch(
        self, messages: list[Message], premium_search_query: str | None = None
    ) -> int:
        """Пакетная вставка сообщений (`INSERT OR IGNORE`); вернуть число реально добавленных."""
        return await self.messages.insert_messages_batch(messages, premium_search_query)

    async def search_messages(self, params: SearchParams) -> MessageSearchPage:
        """Поиск сообщений по фильтру `SearchParams`; возвращает страницу результатов."""
        return await self.messages.search_messages(params)

    async def delete_messages_for_channel(self, channel_id: int) -> int:
        """Удалить все сообщения канала; вернуть число удалённых строк."""
        return await self.messages.delete_messages_for_channel(channel_id)

    async def get_message_stats(self) -> dict:
        """Сводная статистика по таблице сообщений (счётчики и т.п.)."""
        return await self.messages.get_stats()

    async def count_matching_prefixes_in_other_channels(
        self,
        channel_id: int,
        prefixes: list[str],
    ) -> int:
        """Сколько раз префиксы сообщений канала встречаются в других каналах (детектор кросс-спама)."""
        return await self.filters.count_matching_prefixes_in_other_channels(channel_id, prefixes)

    async def get_setting(self, key: str) -> str | None:
        """Прочитать настройку по ключу, либо None."""
        return await self.settings.get_setting(key)

    async def set_setting(self, key: str, value: str) -> None:
        """Записать настройку по ключу."""
        await self.settings.set_setting(key, value)

    async def list_notification_queries(self, active_only: bool = True) -> list[SearchQuery]:
        """Сохранённые поисковые запросы с нотификацией при сборе (`notify_on_collect`)."""
        return await self.search_queries.get_notification_queries(active_only)

    async def get_channel_stats(self, channel_id: int, limit: int = 1) -> list[ChannelStats]:
        """Последние `limit` снимков статистики канала."""
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
        """Создать задачу сбора для канала; возвращает её id."""
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
    """Фасад нотификаций: аккаунты-отправители, настройки и персональные боты."""

    accounts: AccountsRepository
    settings: SettingsRepository
    notification_bots: NotificationBotsRepository

    @classmethod
    def from_database(cls, db: "Database") -> "NotificationBundle":
        """Собрать бандл из репозиториев аккаунтов/настроек/ботов общего `Database`."""
        repos = db.repos
        return cls(repos.accounts, repos.settings, repos.notification_bots)

    async def list_accounts(self, active_only: bool = False) -> list[Account]:
        """Список аккаунтов (с секретом сессии); `active_only` — только активные."""
        return await self.accounts.get_accounts(active_only)

    async def list_account_summaries(self, active_only: bool = False) -> list[AccountSummary]:
        """Безопасные для UI сводки аккаунтов (`AccountSummary`, без секрета сессии)."""
        return await self.accounts.get_account_summaries(active_only)

    async def get_setting(self, key: str) -> str | None:
        """Прочитать настройку по ключу, либо None."""
        return await self.settings.get_setting(key)

    async def set_setting(self, key: str, value: str) -> None:
        """Записать настройку по ключу."""
        await self.settings.set_setting(key, value)

    async def get_bot(self, tg_user_id: int) -> NotificationBot | None:
        """Персональный бот-нотификатор для пользователя Telegram, либо None."""
        return await self.notification_bots.get_bot(tg_user_id)

    async def save_bot(self, bot: NotificationBot) -> int:
        """Сохранить (upsert) бота-нотификатора; возвращает его id."""
        return await self.notification_bots.save_bot(bot)

    async def delete_bot(self, tg_user_id: int) -> None:
        """Удалить бота-нотификатора пользователя."""
        await self.notification_bots.delete_bot(tg_user_id)


@dataclass(frozen=True)
class PhotoLoaderBundle:
    """Фасад photo-loader: пакеты/элементы отправки фото и авто-загрузка из папки.

    Покрывает три сущности: батчи (`PhotoBatch`), их элементы (`PhotoBatchItem`,
    единицы отправки/планирования) и авто-задания (`PhotoAutoUploadJob`,
    периодическая отправка новых файлов из папки).
    """

    photo_loader: PhotoLoaderRepository

    @classmethod
    def from_database(cls, db: "Database") -> "PhotoLoaderBundle":
        """Собрать бандл из репозитория photo-loader общего `Database`."""
        return cls(db.repos.photo_loader)

    async def create_batch(self, batch: PhotoBatch) -> int:
        """Создать батч отправки фото; возвращает его id."""
        return await self.photo_loader.create_batch(batch)

    async def update_batch(
        self,
        batch_id: int,
        *,
        status: PhotoBatchStatus | None = None,
        error: str | None = None,
        last_run_at: datetime | None = None,
    ) -> None:
        """Обновить переданные поля батча (статус, ошибка, время последнего прогона)."""
        await self.photo_loader.update_batch(
            batch_id,
            status=status,
            error=error,
            last_run_at=last_run_at,
        )

    async def get_batch(self, batch_id: int) -> PhotoBatch | None:
        """Батч по id, либо None."""
        return await self.photo_loader.get_batch(batch_id)

    async def list_batches(self, limit: int = 50) -> list[PhotoBatch]:
        """Последние `limit` батчей."""
        return await self.photo_loader.list_batches(limit)

    async def create_item(self, item: PhotoBatchItem) -> int:
        """Создать элемент батча (единицу отправки/планирования); возвращает его id."""
        return await self.photo_loader.create_item(item)

    async def get_item(self, item_id: int) -> PhotoBatchItem | None:
        """Элемент батча по id, либо None."""
        return await self.photo_loader.get_item(item_id)

    async def list_items(self, limit: int = 100) -> list[PhotoBatchItem]:
        """Последние `limit` элементов (по всем батчам)."""
        return await self.photo_loader.list_items(limit)

    async def list_items_for_batch(self, batch_id: int, limit: int | None = None) -> list[PhotoBatchItem]:
        """Элементы конкретного батча; `limit=None` — все."""
        return await self.photo_loader.list_items_for_batch(batch_id, limit=limit)

    async def update_item(
        self,
        item_id: int,
        *,
        status: PhotoBatchStatus | None = None,
        error: str | None = None,
        telegram_message_ids: list[int] | None = None,
        started_at: datetime | None = None,
        completed_at: datetime | None = None,
    ) -> None:
        """Обновить переданные поля элемента (статус, ошибка, id отправленных сообщений, тайминги)."""
        await self.photo_loader.update_item(
            item_id,
            status=status,
            error=error,
            telegram_message_ids=telegram_message_ids,
            started_at=started_at,
            completed_at=completed_at,
        )

    async def cancel_item(self, item_id: int) -> bool:
        """Отменить элемент батча; True, если отмена применена."""
        return await self.photo_loader.cancel_item(item_id)

    async def claim_next_due_item(
        self, now: datetime, *, item_id: int | None = None
    ) -> PhotoBatchItem | None:
        """Атомарно «забрать» ближайший готовый PENDING-элемент в RUNNING.

        Без `item_id` берётся самый ранний готовый (due) элемент; с `item_id` —
        только указанный, и лишь если он сам due. None, если подходящего нет.
        """
        return await self.photo_loader.claim_next_due_item(now, item_id=item_id)

    async def requeue_running_items_on_startup(self, now: datetime) -> int:
        """Вернуть зависшие RUNNING-элементы в очередь при старте; вернуть число затронутых."""
        return await self.photo_loader.requeue_running_items_on_startup(now)

    async def create_auto_job(self, job: PhotoAutoUploadJob) -> int:
        """Создать авто-задание загрузки фото из папки; возвращает его id."""
        return await self.photo_loader.create_auto_job(job)

    async def update_auto_job(
        self,
        job_id: int,
        *,
        folder_path: str | None = None,
        send_mode: PhotoSendMode | None = None,
        caption: str | None = None,
        interval_minutes: int | None = None,
        is_active: bool | None = None,
        error: str | None = None,
        last_run_at: datetime | None = None,
        last_seen_marker: str | None = None,
    ) -> None:
        """Обновить переданные поля авто-задания (папка, режим, подпись, интервал, флаги, маркеры)."""
        await self.photo_loader.update_auto_job(
            job_id,
            folder_path=folder_path,
            send_mode=send_mode,
            caption=caption,
            interval_minutes=interval_minutes,
            is_active=is_active,
            error=error,
            last_run_at=last_run_at,
            last_seen_marker=last_seen_marker,
        )

    async def get_auto_job(self, job_id: int) -> PhotoAutoUploadJob | None:
        """Авто-задание по id, либо None."""
        return await self.photo_loader.get_auto_job(job_id)

    async def list_auto_jobs(self, active_only: bool = False) -> list[PhotoAutoUploadJob]:
        """Список авто-заданий; `active_only` — только активные."""
        return await self.photo_loader.list_auto_jobs(active_only)

    async def delete_auto_job(self, job_id: int) -> None:
        """Удалить авто-задание по id."""
        await self.photo_loader.delete_auto_job(job_id)

    async def has_sent_auto_file(self, job_id: int, file_path: str) -> bool:
        """Был ли файл уже отправлен этим авто-заданием (дедуп повторной отправки)."""
        return await self.photo_loader.has_sent_auto_file(job_id, file_path)

    async def mark_auto_file_sent(self, job_id: int, file_path: str) -> None:
        """Отметить файл как отправленный авто-заданием (чтобы не слать повторно)."""
        await self.photo_loader.mark_auto_file_sent(job_id, file_path)


@dataclass(frozen=True)
class SearchBundle:
    """Фасад поиска: сообщения, журнал поисков, каналы, настройки.

    Флаги `vec_available`/`numpy_available` (определяются в `from_database`)
    сообщают потребителю, доступен ли векторный/семантический поиск в этом
    окружении (расширение sqlite-vec и numpy).
    """

    messages: MessagesRepository
    search_log: SearchLogRepository
    channels: ChannelsRepository
    settings: SettingsRepository
    vec_available: bool = False
    numpy_available: bool = False

    @classmethod
    def from_database(cls, db: "Database") -> "SearchBundle":
        """Собрать бандл из репозиториев поиска и определить доступность vec/numpy."""
        repos = db.repos
        try:
            import numpy  # noqa: F401
            numpy_ok = True
        except ImportError:
            numpy_ok = False
        return cls(
            repos.messages,
            repos.search_log,
            repos.channels,
            repos.settings,
            vec_available=getattr(db, "vec_available", False),
            numpy_available=numpy_ok,
        )

    async def search_messages(self, params: SearchParams) -> MessageSearchPage:
        """Поиск сообщений по фильтру `SearchParams`; возвращает страницу результатов."""
        return await self.messages.search_messages(params)

    async def add_channel(self, channel: Channel) -> int:
        """Добавить канал; возвращает его pk."""
        return await self.channels.add_channel(channel)

    async def insert_messages_batch(
        self, messages: list[Message], premium_search_query: str | None = None
    ) -> int:
        """Пакетная вставка сообщений; вернуть число реально добавленных."""
        return await self.messages.insert_messages_batch(messages, premium_search_query)

    async def log_search(self, phone: str, query: str, results_count: int) -> None:
        """Записать факт поиска в журнал (телефон, запрос, число результатов)."""
        await self.search_log.log_search(phone, query, results_count)

    async def get_recent_searches(self, limit: int = 20) -> list[dict]:
        """Последние `limit` записей журнала поисков."""
        return await self.search_log.get_recent_searches(limit)

    async def get_setting(self, key: str) -> str | None:
        """Прочитать настройку по ключу, либо None."""
        return await self.settings.get_setting(key)

    async def set_setting(self, key: str, value: str) -> None:
        """Записать настройку по ключу."""
        await self.settings.set_setting(key, value)


@dataclass(frozen=True)
class SchedulerBundle:
    """Фасад шедулера: настройки, нотификационные запросы, задачи сбора и журнал поисков."""

    settings: SettingsRepository
    search_queries: SearchQueriesRepository
    tasks: CollectionTasksRepository
    search_log: SearchLogRepository

    @classmethod
    def from_database(cls, db: "Database") -> "SchedulerBundle":
        """Собрать бандл из репозиториев, нужных шедулеру, общего `Database`."""
        repos = db.repos
        return cls(repos.settings, repos.search_queries, repos.tasks, repos.search_log)

    async def get_setting(self, key: str) -> str | None:
        """Прочитать настройку по ключу, либо None."""
        return await self.settings.get_setting(key)

    async def set_setting(self, key: str, value: str) -> None:
        """Записать настройку по ключу."""
        await self.settings.set_setting(key, value)

    async def list_notification_queries(self, active_only: bool = True) -> list[SearchQuery]:
        """Поисковые запросы с нотификацией при сборе (для периодического прогона шедулером)."""
        return await self.search_queries.get_notification_queries(active_only)

    async def get_collection_tasks(self, limit: int = 20) -> list[CollectionTask]:
        """Последние `limit` задач сбора."""
        return await self.tasks.get_collection_tasks(limit)

    async def count_collection_tasks(self, status_filter: str | None = None) -> int:
        """Число задач сбора, опционально только в статусе `status_filter`."""
        return await self.tasks.count_collection_tasks(status_filter)

    async def get_collection_tasks_paginated(
        self, limit: int = 20, offset: int = 0, status_filter: str | None = None
    ) -> tuple[list[CollectionTask], int]:
        """Страница задач сбора и общее их число: `(список, total)`."""
        return await self.tasks.get_collection_tasks_paginated(limit, offset, status_filter)

    async def get_recent_searches(self, limit: int = 20) -> list[dict]:
        """Последние `limit` записей журнала поисков."""
        return await self.search_log.get_recent_searches(limit)


@dataclass(frozen=True)
class SearchQueryBundle:
    """Фасад сохранённых поисковых запросов: CRUD, статистика совпадений, FTS-метрики.

    Репозиторий `channels` опционален (`None` — пропуск): нужен лишь для
    вспомогательного `get_channels`, который иначе вернёт пустой список.
    """

    search_queries: SearchQueriesRepository
    messages: MessagesRepository
    channels: ChannelsRepository | None = None

    @classmethod
    def from_database(cls, db: "Database") -> "SearchQueryBundle":
        """Собрать бандл из репозиториев запросов/сообщений/каналов общего `Database`."""
        repos = db.repos
        return cls(repos.search_queries, repos.messages, repos.channels)

    async def add(self, sq: SearchQuery) -> int:
        """Добавить сохранённый поисковый запрос; возвращает его id."""
        return await self.search_queries.add(sq)

    async def get_all(self, active_only: bool = False) -> list[SearchQuery]:
        """Все сохранённые запросы; `active_only` — только активные."""
        return await self.search_queries.get_all(active_only)

    async def get_by_id(self, sq_id: int) -> SearchQuery | None:
        """Сохранённый запрос по id, либо None."""
        return await self.search_queries.get_by_id(sq_id)

    async def set_active(self, sq_id: int, active: bool) -> None:
        """Включить/выключить сохранённый запрос."""
        await self.search_queries.set_active(sq_id, active)

    async def update(self, sq_id: int, sq: SearchQuery) -> None:
        """Перезаписать сохранённый запрос новыми полями."""
        await self.search_queries.update(sq_id, sq)

    async def delete(self, sq_id: int) -> None:
        """Удалить сохранённый запрос по id."""
        await self.search_queries.delete(sq_id)

    async def record_stat(self, query_id: int, match_count: int) -> None:
        """Записать точку статистики запроса (число совпадений за прогон)."""
        await self.search_queries.record_stat(query_id, match_count)

    async def get_daily_stats(self, query_id: int, days: int = 30) -> list[SearchQueryDailyStat]:
        """Дневная статистика совпадений запроса за последние `days` дней."""
        return await self.search_queries.get_daily_stats(query_id, days)

    async def get_stats_for_all(self, days: int = 30) -> dict[int, list[SearchQueryDailyStat]]:
        """Дневная статистика по всем запросам: `{query_id: [SearchQueryDailyStat]}`."""
        return await self.search_queries.get_stats_for_all(days)

    async def count_fts_matches_for_query(self, sq: SearchQuery) -> int:
        """Текущее число совпадений запроса по FTS-индексу сообщений."""
        return await self.messages.count_fts_matches_for_query(sq)

    async def get_fts_daily_stats_for_query(
        self, sq: SearchQuery, days: int = 30
    ) -> list[SearchQueryDailyStat]:
        """Дневная статистика совпадений запроса, посчитанная по FTS-индексу, за `days` дней."""
        return await self.messages.get_fts_daily_stats_for_query(sq, days)

    async def get_fts_daily_stats_batch(
        self, queries: list[SearchQuery], days: int = 30
    ) -> dict[int, list[SearchQueryDailyStat]]:
        """FTS-статистика сразу для набора запросов: `{query_id: [SearchQueryDailyStat]}`."""
        return await self.messages.get_fts_daily_stats_batch(queries, days)

    async def get_last_recorded_at(self, query_id: int) -> str | None:
        """Время последней записанной точки статистики запроса, либо None."""
        return await self.search_queries.get_last_recorded_at(query_id)

    async def get_last_recorded_at_all(self) -> dict[int, str]:
        """Время последней точки статистики по всем запросам: `{query_id: ts}`."""
        return await self.search_queries.get_last_recorded_at_all()

    async def get_channels(self) -> list[Channel]:
        """Все каналы (для UI-фильтра по каналу); пустой список, если репозиторий не задан."""
        if self.channels is None:
            return []
        return await self.channels.get_channels()


@dataclass(frozen=True)
class PipelineBundle:
    """Фасад контент-пайплайнов: сами пайплайны, их источники/цели и справочники.

    Помимо CRUD пайплайнов даёт доступ к спискам каналов и аккаунтов (для выбора
    источников/целей в UI) и к кэшу диалогов (`dialog_cache`) для разрешения
    целевых диалогов публикации. `pipeline_templates` опционален.
    """

    content_pipelines: ContentPipelinesRepository
    channels: ChannelsRepository
    accounts: AccountsRepository
    dialog_cache: DialogCacheRepository
    pipeline_templates: PipelineTemplatesRepository | None = None

    @classmethod
    def from_database(cls, db: "Database") -> "PipelineBundle":
        """Собрать бандл из репозиториев пайплайнов/каналов/аккаунтов/кэша диалогов."""
        repos = db.repos
        return cls(
            repos.content_pipelines,
            repos.channels,
            repos.accounts,
            repos.dialog_cache,
            repos.pipeline_templates,
        )

    async def add(
        self,
        pipeline: ContentPipeline,
        source_channel_ids: list[int],
        targets: list[PipelineTarget],
    ) -> int:
        """Создать пайплайн вместе с его источниками и целями; возвращает id пайплайна."""
        return await self.content_pipelines.add(pipeline, source_channel_ids, targets)

    async def get_all(self, active_only: bool = False) -> list[ContentPipeline]:
        """Все пайплайны; `active_only` — только активные."""
        return await self.content_pipelines.get_all(active_only)

    async def get_by_id(self, pipeline_id: int) -> ContentPipeline | None:
        """Пайплайн по id, либо None."""
        return await self.content_pipelines.get_by_id(pipeline_id)

    async def update(
        self,
        pipeline_id: int,
        pipeline: ContentPipeline,
        source_channel_ids: list[int],
        targets: list[PipelineTarget],
    ) -> bool:
        """Обновить пайплайн и пересобрать его источники/цели; True при успехе."""
        return await self.content_pipelines.update(
            pipeline_id,
            pipeline,
            source_channel_ids,
            targets,
        )

    async def set_active(self, pipeline_id: int, active: bool) -> None:
        """Включить/выключить пайплайн."""
        await self.content_pipelines.set_active(pipeline_id, active)

    async def delete(self, pipeline_id: int) -> None:
        """Удалить пайплайн по id (вместе со связями источников/целей)."""
        await self.content_pipelines.delete(pipeline_id)

    async def list_sources(self, pipeline_id: int) -> list[PipelineSource]:
        """Каналы-источники пайплайна."""
        return await self.content_pipelines.list_sources(pipeline_id)

    async def list_targets(self, pipeline_id: int) -> list[PipelineTarget]:
        """Целевые диалоги публикации пайплайна."""
        return await self.content_pipelines.list_targets(pipeline_id)

    async def list_channels(
        self,
        active_only: bool = False,
        include_filtered: bool = True,
    ):
        """Список каналов (для выбора источников в UI пайплайна)."""
        return await self.channels.get_channels(active_only, include_filtered)

    async def list_accounts(self, active_only: bool = False):
        """Список аккаунтов (для выбора аккаунта-отправителя пайплайна)."""
        return await self.accounts.get_accounts(active_only)

    async def list_account_summaries(self, active_only: bool = False):
        """Безопасные сводки аккаунтов (`AccountSummary`, без секрета сессии)."""
        return await self.accounts.get_account_summaries(active_only)

    async def get_cached_dialog(self, phone: str, dialog_id: int) -> dict | None:
        """Закэшированный диалог `(phone, dialog_id)` для разрешения цели публикации, либо None."""
        return await self.dialog_cache.get_dialog(phone, dialog_id)

    async def list_cached_dialogs(self, phone: str) -> list[dict]:
        """Все закэшированные диалоги аккаунта (для выпадающего списка целей)."""
        return await self.dialog_cache.list_dialogs(phone)
