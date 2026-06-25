"""Pydantic-модели предметной области — фасад данных всего приложения.

Каждый класс здесь — типизированный контракт между слоями (репозитории ↔
сервисы ↔ web/CLI/agent). Репозитории мапят строки SQLite в эти модели через
`_to_<model>` хелперы; web/CLI и agent-tools читают их как единственный источник
истины о форме данных. Эти же docstring питают авто-документацию /api
(mkdocstrings, #1071), так что описания тут дают двойную выгоду — в коде и в доках.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator

from src.telegram.flood_wait import FloodWaitInfo


@dataclass(slots=True)
class SearchParams:
    """Single carrier for the message-search filter signature shared across the
    search stack (repo ↔ bundles ↔ local search). Public entry points
    (`SearchEngine.*`, `Database.search_messages`) keep their keyword signatures
    and assemble this object once, so a new search filter is added here + the
    repo body rather than re-declared at every delegating layer (#810/#807 п.4)."""

    query: str = ""
    channel_id: int | None = None
    date_from: str | None = None
    date_to: str | None = None
    limit: int = 50
    offset: int = 0
    is_fts: bool = False
    min_length: int | None = None
    max_length: int | None = None
    topic_id: int | None = None
    include_filtered: bool = False


class Account(BaseModel):
    """Telegram-аккаунт пула: телефон + StringSession и его флаги.

    `session_string` — секрет (хранится зашифрованным как `enc:v2:*`, если задан
    `SESSION_ENCRYPTION_KEY`); никогда не отдаётся наружу — для UI/agent есть
    усечённая [`AccountSummary`][src.models.AccountSummary]. `is_primary`
    выбирает аккаунт по умолчанию; `flood_wait_until` — момент, до которого
    `ClientPool` пропускает аккаунт из-за Telegram FLOOD_WAIT.
    """

    id: int | None = None
    phone: str
    session_string: str
    is_primary: bool = False
    is_active: bool = True
    is_premium: bool = False
    flood_wait_until: datetime | None = None
    created_at: datetime | None = None


class AccountSessionStatus(StrEnum):
    """Состояние StringSession аккаунта с точки зрения дешифровки/совместимости.

    Отделяет «аккаунт активен» от «сессию удалось прочитать»: при включённом
    `SESSION_ENCRYPTION_KEY` сессия может оказаться нечитаемой (нет ключа,
    повреждена, чужая версия) — UI показывает это без раскрытия секрета.
    """

    OK = "ok"
    ENCRYPTED_UNKNOWN = "encrypted_unknown"
    DECRYPT_FAILED = "decrypt_failed"
    MISSING_KEY = "missing_key"
    UNSUPPORTED_VERSION = "unsupported_version"


class AccountSummary(BaseModel):
    """Безопасная для UI/agent проекция [`Account`][src.models.Account] без `session_string`.

    Несёт те же метаданные (телефон, флаги, flood-wait) плюс
    [`session_status`][src.models.AccountSessionStatus], но без секрета сессии —
    это форма, которую отдают наружу списки аккаунтов.
    """

    id: int | None = None
    phone: str
    is_primary: bool = False
    is_active: bool = True
    is_premium: bool = False
    flood_wait_until: datetime | None = None
    created_at: datetime | None = None
    session_status: AccountSessionStatus = AccountSessionStatus.OK


class TelegramUserInfo(BaseModel):
    """Профиль пользователя Telegram, привязанного к аккаунту пула.

    Возвращается при запросе «кто этот аккаунт»: имя/username, флаги
    primary/premium и аватар в виде data-URI (`avatar_base64`) для прямого показа
    в UI без отдельного запроса картинки. В отличие от
    [`Account`][src.models.Account] — описательная карточка, а не носитель сессии.
    """

    phone: str
    first_name: str = ""
    last_name: str = ""
    username: str | None = None
    is_primary: bool = False
    is_premium: bool = False
    avatar_base64: str | None = None  # "data:image/jpeg;base64,..."


class Channel(BaseModel):
    """Telegram-канал/чат, отслеживаемый системой, со сбором и фильтрацией.

    `channel_id` — Telegram-идентификатор (на него ссылаются все сайдкар-таблицы);
    `id` — первичный ключ БД (используется только в pk-операциях). Сбор —
    инкрементальный: `last_collected_id` хранит максимальный собранный message_id.
    `is_filtered` + `filter_flags` ставит [`ChannelAnalyzer`][src.filters.analyzer]
    (низкая уникальность, спам и т.п.) — отфильтрованные каналы пропускаются при
    сборе. `needs_review`/`review_reason` помечают канал для ручной проверки.
    `created_at` — дата создания канала в Telegram, `added_at` — когда добавлен в
    систему.
    """

    id: int | None = None
    channel_id: int
    title: str | None = None
    username: str | None = None
    channel_type: str | None = None  # "channel"|"supergroup"|"gigagroup"|"group"|"unavailable"
    is_active: bool = True
    is_filtered: bool = False
    filter_flags: str = ""
    about: str | None = None
    linked_chat_id: int | None = None
    has_comments: bool = False
    last_collected_id: int = 0
    preferred_phone: str | None = None
    needs_review: bool = False
    review_reason: str | None = None
    added_at: datetime | None = None
    created_at: datetime | None = None
    message_count: int = 0
    tags: list[str] = []


class Message(BaseModel):
    """Одно сообщение канала: текст и/или медиа плюс метаданные и метрики.

    Уникально по паре `(channel_id, message_id)`. `message_kind`/`sender_kind`/
    `media_type` классифицируют содержимое и отправителя; служебные сообщения
    раскладываются в `service_action_*`. Метрики (`views`/`forwards`/
    `reply_count`/`reactions_json`) собираются вместе с текстом. `detected_lang`
    и `translation_*` заполняет слой перевода. `channel_title`/`channel_username`
    — денормализованные поля для вывода без JOIN (заполняются при чтении).
    """

    id: int | None = None
    channel_id: int
    message_id: int
    sender_id: int | None = None
    sender_name: str | None = None
    sender_first_name: str | None = None
    sender_last_name: str | None = None
    sender_username: str | None = None
    text: str | None = None
    message_kind: str | None = None
    media_type: str | None = None
    service_action_raw: str | None = None
    service_action_semantic: str | None = None
    service_action_payload_json: str | None = None
    sender_kind: str | None = None
    topic_id: int | None = None
    reactions_json: str | None = None
    views: int | None = None
    forwards: int | None = None
    reply_count: int | None = None
    date: datetime
    collected_at: datetime | None = None
    detected_lang: str | None = None
    translation_en: str | None = None
    translation_custom: str | None = None
    forward_from_channel_id: int | None = None
    channel_title: str | None = None
    channel_username: str | None = None


class CollectionTaskStatus(StrEnum):
    """Жизненный цикл фоновой задачи [`CollectionTask`][src.models.CollectionTask]:
    от `PENDING` (в очереди) через `RUNNING` к терминальным
    `COMPLETED`/`FAILED`/`CANCELLED`."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class CollectionTaskType(StrEnum):
    """Вид фоновой задачи в `collection_tasks` — определяет, какой обработчик её
    исполняет.

    Большинство типов исполняют внутренние воркеры фабрики (UnifiedDispatcher,
    CollectionQueue). Interop-типы (`DM_REPLY`, `CHAT_ANSWER`, `FETCH_DIALOGS`,
    `FETCH_HISTORY`) фабрика только создаёт — их забирает внешний воркер
    tg_messenger через REST-claim API (см.
    [`EXTERNAL_INTEROP_TASK_TYPES`][src.models.EXTERNAL_INTEROP_TASK_TYPES]).
    """

    CHANNEL_COLLECT = "channel_collect"
    STATS_ALL = "stats_all"
    SQ_STATS = "sq_stats"
    FILTER_ANALYZE = "filter_analyze"
    PHOTO_DUE = "photo_due"
    PHOTO_AUTO = "photo_auto"
    PIPELINE_RUN = "pipeline_run"
    CONTENT_GENERATE = "content_generate"
    CONTENT_PUBLISH = "content_publish"
    TRANSLATE_BATCH = "translate_batch"
    EXPORT = "export"
    # Interop types (#960): produced by tg_content_factory, executed by an
    # external tg_messenger worker via the /api/tasks REST claim API (#961).
    # The factory's own workers (UnifiedDispatcher, CollectionQueue) never claim
    # these — they are absent from HANDLED_TYPES and from the CHANNEL_COLLECT
    # pull — so they sit PENDING until the external worker claims them.
    DM_REPLY = "dm_reply"
    CHAT_ANSWER = "chat_answer"
    FETCH_DIALOGS = "fetch_dialogs"
    FETCH_HISTORY = "fetch_history"


# Task types executed by an external interop worker (tg_messenger), never by the
# factory's internal dispatchers. See #829 / #960.
EXTERNAL_INTEROP_TASK_TYPES: frozenset[CollectionTaskType] = frozenset(
    {
        CollectionTaskType.DM_REPLY,
        CollectionTaskType.CHAT_ANSWER,
        CollectionTaskType.FETCH_DIALOGS,
        CollectionTaskType.FETCH_HISTORY,
    }
)


class TelegramCommandStatus(StrEnum):
    """Жизненный цикл [`TelegramCommand`][src.models.TelegramCommand]: `PENDING`
    → `RUNNING` → терминальные `SUCCEEDED`/`FAILED`/`CANCELLED`."""

    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


class TelegramCommand(BaseModel):
    """Команда из web-слоя воркеру: единица очереди `telegram_commands`.

    Web-контейнер не держит живых Telegram-соединений — UI-действия кладутся
    сюда как команды (`command_type` + произвольный `payload`), а живой воркер их
    исполняет и пишет результат в `result_payload`/`error`. `run_after` позволяет
    отложить исполнение.
    """

    id: int | None = None
    command_type: str
    payload: dict[str, Any] = Field(default_factory=dict)
    status: TelegramCommandStatus = TelegramCommandStatus.PENDING
    requested_by: str | None = None
    created_at: datetime | None = None
    started_at: datetime | None = None
    run_after: datetime | None = None
    finished_at: datetime | None = None
    error: str | None = None
    result_payload: dict[str, Any] | None = None


class RuntimeSnapshot(BaseModel):
    """Снимок состояния воркера, публикуемый для чтения web-слоем.

    Воркер пишет в `runtime_snapshots` строки разных `snapshot_type` (heartbeat,
    accounts_status, scheduler_status, …); web-контейнер читает их, чтобы
    отрисовать статус, не открывая Telegram-соединений сам. `scope` разделяет
    глобальные снимки и привязанные к конкретной сущности.
    """

    snapshot_type: str
    scope: str = "global"
    payload: dict[str, Any] = Field(default_factory=dict)
    updated_at: datetime | None = None


# Channel rating (#966): two-axis verdict (usefulness × genre) for a channel,
# produced by ChannelAnalysisService (logic ported from the removed ai_detect_tool seed, #781).
ChannelUsefulness = Literal["useful", "useless"]
ChannelGenre = Literal["ad", "infobiz", "aggregator", "copy", "original"]


class ChannelRating(BaseModel):
    """Двухосный вердикт о канале (#966): полезность × жанр.

    Продукт `ChannelAnalysisService`: `useful` (полезен/нет) и `genre`
    (реклама/инфобизнес/агрегатор/копия/оригинал) с уверенностью `confidence` и
    текстовым обоснованием `reason`. `emoji_trash_score` и `flag_count` —
    вспомогательные сигналы шума, `n_total` — размер выборки, на которой посчитан
    вердикт.
    """

    channel_id: int
    title: str | None = None
    username: str | None = None
    useful: ChannelUsefulness
    genre: ChannelGenre
    confidence: float = 0.0
    reason: str | None = None
    emoji_trash_score: float | None = None
    flag_count: int = 0
    n_total: int = 0
    updated_at: datetime | None = None


# ---------------------------------------------------------------------------
# Unified jobs read-model (#963): one "job" view over the four heterogeneous
# background-work sources (collection_tasks, telegram_commands, photo_* tables,
# APScheduler jobs from runtime_snapshots) so the panel can show everything in
# one table. Read-only — no source rows are written through this model.
# ---------------------------------------------------------------------------
class JobSource(StrEnum):
    """Источник, из которого собрана строка единого jobs-вью
    [`JobView`][src.models.JobView]: одна из четырёх разнородных таблиц
    фоновой работы (collection_tasks, telegram_commands, photo_*, APScheduler)."""

    COLLECTION_TASK = "collection_task"
    TELEGRAM_COMMAND = "telegram_command"
    PHOTO_BATCH_ITEM = "photo_batch_item"
    PHOTO_AUTO_JOB = "photo_auto_job"
    SCHEDULER_JOB = "scheduler_job"


class JobRuntimeState(StrEnum):
    """Normalized cross-source lifecycle/runtime state."""

    RUNNING = "running"
    PENDING = "pending"
    SCHEDULED = "scheduled"  # waiting on a future run_after / interval
    PAUSE_GATE = "pause_gate"  # held by LiveRuntimePauseGate (#770)
    FLOOD_WAIT = "flood_wait"  # deferred by a Telegram FLOOD_WAIT
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    INACTIVE = "inactive"  # recurring job toggled off


class JobView(BaseModel):
    """Одна строка унифицированного read-model «работ» (#963).

    Приводит четыре разнородных источника
    ([`JobSource`][src.models.JobSource]) к общей форме, чтобы панель показывала
    всё в одной таблице: нормализованный `runtime_state`
    ([`JobRuntimeState`][src.models.JobRuntimeState]) поверх сырого `status`,
    стабильный кросс-источниковый `id` с префиксом и человекочитаемый `summary`.
    Только для чтения — записи в исходные таблицы через эту модель не идут.
    """

    source: JobSource
    id: str  # source-prefixed, e.g. "collection_task:42" (stable across sources)
    raw_id: int | None = None
    job_type: str  # task_type / command_type / job_id / "photo_*"
    status: str | None = None  # raw per-source status string
    runtime_state: JobRuntimeState
    summary: str = ""  # short human-readable payload summary
    run_after: datetime | None = None
    created_at: datetime | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    error: str | None = None
    note: str | None = None


class ContentGenerateTaskPayload(BaseModel):
    """Полезная нагрузка задачи CONTENT_GENERATE: какой пайплайн генерировать."""

    task_kind: str = "content_generate"
    pipeline_id: int


class ContentPublishTaskPayload(BaseModel):
    """Полезная нагрузка задачи CONTENT_PUBLISH: публикация результата пайплайна
    (`pipeline_id` опционален для разовых публикаций вне пайплайна)."""

    task_kind: str = "content_publish"
    pipeline_id: int | None = None


class StatsAllTaskPayload(BaseModel):
    """Нагрузка STATS_ALL: обход списка каналов для сбора статистики с курсором.

    Задача переживает рестарты: `next_index`/`remaining_channel_ids` — позиция
    обхода, `channels_ok`/`channels_err` — накопленные счётчики результата.
    """

    task_kind: str = CollectionTaskType.STATS_ALL.value
    channel_ids: list[int]
    next_index: int = 0
    channels_ok: int = 0
    channels_err: int = 0
    remaining_channel_ids: list[int] | None = None


class SqStatsTaskPayload(BaseModel):
    """Нагрузка SQ_STATS: пересчёт статистики для одного поискового запроса `sq_id`."""

    task_kind: str = CollectionTaskType.SQ_STATS.value
    sq_id: int


class FilterAnalyzeTaskPayload(BaseModel):
    """Нагрузка FILTER_ANALYZE: прогон анализатора фильтров по каналам.

    Поля-слоты для итогов (`total_channels`/`filtered_count`/`purged_count`) —
    необязательны: они зарезервированы под сводку «проанализировано/отфильтровано/
    вычищено», но текущий handler сводку пишет в `note`/`messages_collected`
    задачи, а не в этот payload, поэтому по умолчанию остаются `None`.
    """

    task_kind: str = CollectionTaskType.FILTER_ANALYZE.value
    total_channels: int | None = None
    filtered_count: int | None = None
    purged_count: int | None = None


class TranslateBatchTaskPayload(BaseModel):
    """Нагрузка TRANSLATE_BATCH: пакетный перевод сообщений с курсором.

    `last_processed_id` — позиция продолжения между батчами; `source_filter`
    ограничивает исходные языки, `batch_size` — размер пакета за проход.
    """

    task_kind: str = "translate_batch"
    target_lang: str = "en"
    source_filter: list[str] = []
    batch_size: int = 20
    last_processed_id: int = 0


class ExportTaskPayload(BaseModel):
    """Нагрузка EXPORT: выгрузка сообщений канала в файл (`fmt`: json/html/both).

    Поддерживает фильтры по датам/лимиту, опциональную выгрузку медиа
    (`with_media`) с ограничением размера и каталог назначения `out_dir`.
    """

    task_kind: str = CollectionTaskType.EXPORT.value
    channel_id: int
    fmt: Literal["json", "html", "both"] = "json"
    with_media: bool = False
    max_file_size_mb: int | None = None
    date_from: str | None = None
    date_to: str | None = None
    limit: int = 5000
    out_dir: str | None = None
    requested_by: str | None = None


# Interop payloads (#960). Produced by the factory, consumed by an external
# tg_messenger worker. ``v`` is the payload schema version so the worker can
# evolve independently; ``task_kind`` mirrors the enum value for self-describing
# rows.
#
# By design these are NOT added to CollectionTasksRepository._deserialize_payload
# / create_generic_task's typed union: the factory only produces them (the REST
# API stores the request body as an opaque JSON dict) and never executes them, so
# it never needs typed read access — the external worker owns validation/execution.
class DmReplyTaskPayload(BaseModel):
    """Interop-нагрузка DM_REPLY (#960): ответ в личку, исполняет внешний воркер.

    `peer` — @username или числовой id собеседника; `v` — версия схемы нагрузки,
    чтобы внешний воркер мог эволюционировать независимо.
    """

    v: int = 1
    task_kind: str = CollectionTaskType.DM_REPLY.value
    peer: str = Field(min_length=1)  # @username or numeric user id of the DM peer
    text: str = Field(min_length=1)
    reply_to_message_id: int | None = None


class ChatAnswerTaskPayload(BaseModel):
    """Interop-нагрузка CHAT_ANSWER (#960): ответ в чат `chat_id`, исполняет
    внешний воркер. `v` — версия схемы нагрузки."""

    v: int = 1
    task_kind: str = CollectionTaskType.CHAT_ANSWER.value
    chat_id: int
    text: str = Field(min_length=1)
    reply_to_message_id: int | None = None


class FetchDialogsTaskPayload(BaseModel):
    """Interop-нагрузка FETCH_DIALOGS (#960): запрос списка диалогов у внешнего
    воркера (`archived` включает архивные). `v` — версия схемы нагрузки."""

    v: int = 1
    task_kind: str = CollectionTaskType.FETCH_DIALOGS.value
    limit: int = 100
    archived: bool = False


class FetchHistoryTaskPayload(BaseModel):
    """Interop-нагрузка FETCH_HISTORY (#960): запрос истории диалога `peer` у
    внешнего воркера с пагинацией от `offset_id`. `v` — версия схемы нагрузки."""

    v: int = 1
    task_kind: str = CollectionTaskType.FETCH_HISTORY.value
    peer: str = Field(min_length=1)  # @username or numeric chat/channel id
    limit: int = 100
    offset_id: int = 0


class CollectionTask(BaseModel):
    """Строка таблицы `collection_tasks` — единица фоновой работы.

    Несёт тип ([`CollectionTaskType`][src.models.CollectionTaskType]) и статус
    ([`CollectionTaskStatus`][src.models.CollectionTaskStatus]) плюс
    типизированный `payload` (union нагрузок под конкретный тип задачи, обычный
    dict — для interop-типов). `last_progress_at` отличает реально застрявший
    сбор от упавшего воркера; `result_payload` пишет внешний interop-воркер.
    `parent_task_id` связывает порождённые подзадачи с родителем.
    """

    id: int | None = None
    channel_id: int | None = None
    channel_title: str | None = None
    channel_username: str | None = None
    task_type: CollectionTaskType = CollectionTaskType.CHANNEL_COLLECT
    status: CollectionTaskStatus = CollectionTaskStatus.PENDING
    messages_collected: int = 0
    error: str | None = None
    note: str | None = None
    run_after: datetime | None = None
    payload: (
        dict[str, Any]
        | StatsAllTaskPayload
        | SqStatsTaskPayload
        | FilterAnalyzeTaskPayload
        | PipelineRunTaskPayload
        | ContentGenerateTaskPayload
        | ContentPublishTaskPayload
        | TranslateBatchTaskPayload
        | ExportTaskPayload
        | None
    ) = None
    parent_task_id: int | None = None
    created_at: datetime | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None
    # When progress (messages_collected) last advanced — used by the scheduler
    # health page to tell a genuinely stuck collection apart from a downed worker.
    last_progress_at: datetime | None = None
    # Result written back by an external interop worker on completion (#961).
    result_payload: dict[str, Any] | None = None


class ChannelStats(BaseModel):
    """Снимок метрик канала на момент `collected_at`: число подписчиков и средние
    показатели по сообщениям (просмотры/реакции/пересылки). Хранится историей —
    каждый сбор добавляет новую строку."""

    id: int | None = None
    channel_id: int
    subscriber_count: int | None = None
    avg_views: float | None = None
    avg_reactions: float | None = None
    avg_forwards: float | None = None
    collected_at: datetime | None = None


class PipelinePublishMode(StrEnum):
    """Режим публикации пайплайна: `AUTO` — публиковать сразу после генерации,
    `MODERATED` — отправлять результат на ручную модерацию перед публикацией."""

    AUTO = "auto"
    MODERATED = "moderated"


class PipelineGenerationBackend(StrEnum):
    """Движок генерации контента в пайплайне: простая LLM-цепочка (`CHAIN`),
    агент (`AGENT`) или deepagents-бэкенд (`DEEP_AGENTS`)."""

    CHAIN = "chain"
    AGENT = "agent"
    DEEP_AGENTS = "deep_agents"


class PipelineNodeType(StrEnum):
    """Тип узла в графе-DAG пайплайна (#343): источник, шаги генерации/рефайна,
    генерация картинки, публикация, уведомление, ветвления и Telegram-действия —
    определяет, как узел исполняется обходчиком графа."""

    SOURCE = "source"
    RETRIEVE_CONTEXT = "retrieve_context"
    LLM_GENERATE = "llm_generate"
    LLM_REFINE = "llm_refine"
    IMAGE_GENERATE = "image_generate"
    PUBLISH = "publish"
    NOTIFY = "notify"
    FILTER = "filter"
    DELAY = "delay"
    REACT = "react"
    FORWARD = "forward"
    DELETE_MESSAGE = "delete_message"
    FETCH_MESSAGES = "fetch_messages"
    CONDITION = "condition"
    SEARCH_QUERY_TRIGGER = "search_query_trigger"
    AGENT_LOOP = "agent_loop"


class PipelineNode(BaseModel):
    """Узел графа пайплайна: тип ([`PipelineNodeType`][src.models.PipelineNodeType]),
    имя, произвольный `config` под конкретный тип и `position` для отрисовки в
    редакторе графа."""

    id: str
    type: PipelineNodeType
    name: str
    config: dict[str, Any] = Field(default_factory=dict)
    position: dict[str, float] = Field(default_factory=lambda: {"x": 0.0, "y": 0.0})


class PipelineEdge(BaseModel):
    """Направленное ребро графа пайплайна `from_node` → `to_node`.

    В JSON сериализуется как `from`/`to` (отсюда алиасы полей); опциональный
    `condition` делает ребро условным переходом из узла-ветвления.
    """

    model_config = {"populate_by_name": True}

    from_node: str = Field(alias="from")
    to_node: str = Field(alias="to")
    condition: str | None = None


class PipelineGraph(BaseModel):
    """Граф-DAG пайплайна: узлы + рёбра, с (де)сериализацией в JSON для хранения.

    Хранится строкой JSON в БД; `from`/`to` рёбер сохраняются в коротком виде.
    """

    nodes: list[PipelineNode] = Field(default_factory=list)
    edges: list[PipelineEdge] = Field(default_factory=list)

    def to_json(self) -> str:
        """Сериализовать граф в JSON-строку (рёбра — в коротком виде `from`/`to`,
        `condition` опускается, если пуст)."""
        import json
        return json.dumps(
            {
                "nodes": [n.model_dump() for n in self.nodes],
                "edges": [
                    {"from": e.from_node, "to": e.to_node, **({"condition": e.condition} if e.condition else {})}
                    for e in self.edges
                ],
            },
            ensure_ascii=False,
        )

    @classmethod
    def from_json(cls, data: str | dict) -> "PipelineGraph":
        """Собрать граф из JSON-строки или уже разобранного dict (валидирует узлы
        и рёбра через их модели)."""
        import json
        if isinstance(data, str):
            data = json.loads(data)
        nodes = [PipelineNode.model_validate(n) for n in data.get("nodes", [])]
        edges = [PipelineEdge.model_validate(e) for e in data.get("edges", [])]
        return cls(nodes=nodes, edges=edges)


class PipelineTemplate(BaseModel):
    """Шаблон графа пайплайна для быстрого старта: готовый
    [`PipelineGraph`][src.models.PipelineGraph] с именем/описанием/категорией.
    `is_builtin` помечает встроенные шаблоны (поставляются с приложением)."""

    id: int | None = None
    name: str
    description: str = ""
    category: str = ""
    template_json: PipelineGraph
    is_builtin: bool = False
    created_at: datetime | None = None


class ContentPipeline(BaseModel):
    """Конфигурация контент-пайплайна: генерация → (картинка) → черновик →
    публикация.

    Задаёт модели (`llm_model`/`image_model`), режим публикации
    ([`PipelinePublishMode`][src.models.PipelinePublishMode]) и движок
    ([`PipelineGenerationBackend`][src.models.PipelineGenerationBackend]).
    `prompt_template`/`refinement_steps` описывают LLM-цепочку, а
    `pipeline_json` — альтернативный узловой DAG (#343). `publish_times` —
    JSON-массив времён публикации; `last_generated_id` — курсор инкрементальной
    генерации. A/B-поля (`ab_num_variants`/`ab_auto_select`, #1068) включают
    генерацию N вариантов текста (по умолчанию выключено — множит расход токенов).
    """

    id: int | None = None
    name: str
    prompt_template: str = ""
    llm_model: str | None = None
    image_model: str | None = None
    publish_mode: PipelinePublishMode = PipelinePublishMode.MODERATED
    generation_backend: PipelineGenerationBackend = PipelineGenerationBackend.CHAIN
    is_active: bool = True
    last_generated_id: int = 0
    generate_interval_minutes: int = Field(60, ge=1)
    account_phone: str | None = None
    publish_times: str | None = None  # JSON array of "HH:MM" times, e.g. '["09:00", "18:00"]'
    refinement_steps: list[dict] = []  # list of {name, prompt} dicts; {text} in prompt is replaced
    pipeline_json: PipelineGraph | None = None  # node-based DAG config (issue #343)
    # A/B testing (issue #1068): generate N stylistic variants of the base text
    # and optionally auto-select the best. ab_num_variants <= 1 disables it
    # (default off — variant generation multiplies token cost ×N).
    ab_num_variants: int = Field(1, ge=1)
    ab_auto_select: bool = False
    created_at: datetime | None = None


class PipelineSource(BaseModel):
    """Привязка канала-источника к пайплайну: откуда берётся контекст/материал
    для генерации (связь `pipeline_id` ↔ `channel_id`)."""

    id: int | None = None
    pipeline_id: int
    channel_id: int
    created_at: datetime | None = None


class PipelineTarget(BaseModel):
    """Цель публикации пайплайна: диалог (`dialog_id`) у аккаунта `phone`, куда
    отправляется готовый контент. `title`/`dialog_type` — описательные поля
    диалога."""

    id: int | None = None
    pipeline_id: int
    phone: str
    dialog_id: int
    title: str | None = None
    dialog_type: str | None = None
    created_at: datetime | None = None


class PipelineRunTaskPayload(BaseModel):
    """Нагрузка PIPELINE_RUN: запуск пайплайна `pipeline_id`.

    `dry_run` — прогон без публикации; `since_hours` ограничивает окно исходных
    сообщений, из которых берётся материал.
    """

    task_kind: str = CollectionTaskType.PIPELINE_RUN.value
    pipeline_id: int
    dry_run: bool = False
    since_hours: float = 24.0


class NotificationBot(BaseModel):
    """Персональный бот уведомлений, созданный через BotFather под аккаунтом.

    Связывает Telegram-пользователя (`tg_user_id`) с ботом (`bot_username`/
    `bot_token`), через которого [`Notifier`][src.telegram.notifier] шлёт алерты.
    """

    id: int = 0
    tg_user_id: int
    tg_username: str | None = None
    bot_id: int | None = None
    bot_username: str
    bot_token: str
    created_at: datetime | None = None


class SearchQuery(BaseModel):
    """Сохранённый поисковый запрос с расписанием и опциональными уведомлениями.

    Режим поиска взаимоисключающий: `is_regex` (regex) либо `is_fts`
    (полнотекстовый), иначе — подстрочный. `notify_on_collect` шлёт алерт о новых
    совпадениях при сборе, `track_stats` копит дневную статистику попаданий,
    `interval_minutes` задаёт период периодического прогона. `exclude_patterns`
    (по строке на паттерн) и `max_length`/`chat_filter` сужают результат.
    """

    id: int | None = None
    name: str = ""
    query: str
    is_regex: bool = False
    is_fts: bool = False
    is_active: bool = True
    notify_on_collect: bool = False
    track_stats: bool = True
    interval_minutes: int = Field(60, ge=1)
    exclude_patterns: str = ""
    max_length: int | None = None
    chat_filter: str = ""
    created_at: datetime | None = None

    @model_validator(mode="after")
    def check_mode_exclusive(self) -> "SearchQuery":
        """Запретить одновременно regex и FTS — режимы поиска взаимоисключающие."""
        if self.is_regex and self.is_fts:
            raise ValueError("is_regex and is_fts are mutually exclusive")
        return self

    @model_validator(mode="after")
    def default_name_to_query(self) -> "SearchQuery":
        """Подставить текст запроса как имя, если имя не задано явно."""
        if not self.name:
            self.name = self.query
        return self

    @property
    def exclude_patterns_list(self) -> list[str]:
        if not self.exclude_patterns:
            return []
        return [p.strip() for p in self.exclude_patterns.splitlines() if p.strip()]


class SearchQueryDailyStat(BaseModel):
    """Одна точка дневной статистики сохранённого запроса: число совпадений
    (`count`) за дату `day` (формат `YYYY-MM-DD`)."""

    day: str  # "2026-03-07"
    count: int


class SearchResult(BaseModel):
    """Результат поиска: найденные сообщения плюс мета о выдаче.

    `total` — нижняя оценка числа совпадений для локального поиска по БД (точна
    только когда `has_more=False`); другие режимы трактуют его по-своему.
    `ai_summary` заполняется AI-поиском, `flood_wait` сигнализирует о Telegram
    FLOOD_WAIT, `error` — о неуспехе запроса.
    """

    messages: list[Message]
    # Lower bound for local DB search since #766 (offset + page size; exact only
    # when has_more is False); other modes keep their own semantics.
    total: int
    has_more: bool = False
    query: str
    ai_summary: str | None = None
    error: str | None = None
    flood_wait: FloodWaitInfo | None = None


class GenerationRun(BaseModel):
    """Один прогон генерации контента (строка `generation_runs`).

    Фиксирует вход (`prompt`) и выход (`generated_text`/`image_url`) с двумя
    осями состояния: `status` (жизненный цикл прогона) и `moderation_status`
    (одобрение к публикации). `quality_score`/`quality_issues` — оценка качества;
    `variants`/`selected_variant` — A/B-варианты и выбранный (#1068). `metadata`
    хранит произвольные данные прогона (например, цитаты-источники), из которых
    свойства `result_kind`/`result_count` выводят сводку результата.
    """

    id: int | None = None
    pipeline_id: int | None = None
    status: str = "pending"
    prompt: str | None = None
    generated_text: str | None = None
    metadata: dict | None = None
    image_url: str | None = None
    moderation_status: str = "pending"
    quality_score: float | None = None
    quality_issues: list[str] | None = None
    variants: list[str] | None = None
    selected_variant: int | None = None
    published_at: datetime | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None

    @property
    def result_kind(self) -> str:
        metadata = self.metadata if isinstance(self.metadata, dict) else {}
        value = metadata.get("result_kind")
        if isinstance(value, str) and value:
            return value
        if self.generated_text:
            return "generated_items"
        return "processed_messages"

    @property
    def result_count(self) -> int:
        metadata = self.metadata if isinstance(self.metadata, dict) else {}
        value = metadata.get("result_count")
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(value)
        citations = metadata.get("citations")
        if isinstance(citations, list):
            return len(citations)
        return 1 if self.generated_text else 0


class PhotoSendMode(StrEnum):
    """Способ отправки набора фото: одним альбомом (`ALBUM`) или отдельными
    сообщениями (`SEPARATE`)."""

    ALBUM = "album"
    SEPARATE = "separate"


class PhotoBatchStatus(StrEnum):
    """Жизненный цикл фото-батча/элемента: `PENDING`/`SCHEDULED` → `RUNNING` →
    терминальные `COMPLETED`/`FAILED`/`CANCELLED` (`SCHEDULED` — отложенная
    отправка по времени)."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    SCHEDULED = "scheduled"


class PhotoBatch(BaseModel):
    """Батч отправки фото в один диалог (`target_dialog_id`) под аккаунтом
    `phone`.

    Группирующая запись с режимом отправки
    ([`PhotoSendMode`][src.models.PhotoSendMode]) и статусом
    ([`PhotoBatchStatus`][src.models.PhotoBatchStatus]); конкретные файлы несут
    элементы [`PhotoBatchItem`][src.models.PhotoBatchItem].
    """

    id: int | None = None
    phone: str
    target_dialog_id: int
    target_title: str | None = None
    target_type: str | None = None
    send_mode: PhotoSendMode = PhotoSendMode.ALBUM
    caption: str | None = None
    status: PhotoBatchStatus = PhotoBatchStatus.PENDING
    error: str | None = None
    created_at: datetime | None = None
    last_run_at: datetime | None = None


class PhotoBatchItem(BaseModel):
    """Элемент фото-батча: конкретный набор файлов (`file_paths`) к отправке.

    Может выполняться отложенно (`schedule_at`); по факту отправки заполняется
    `telegram_message_ids`. `batch_id` связывает с
    [`PhotoBatch`][src.models.PhotoBatch] (может быть пуст для одиночной отправки).
    """

    id: int | None = None
    batch_id: int | None = None
    phone: str
    target_dialog_id: int
    target_title: str | None = None
    target_type: str | None = None
    file_paths: list[str]
    send_mode: PhotoSendMode = PhotoSendMode.ALBUM
    caption: str | None = None
    schedule_at: datetime | None = None
    status: PhotoBatchStatus = PhotoBatchStatus.PENDING
    error: str | None = None
    telegram_message_ids: list[int] = Field(default_factory=list)
    created_at: datetime | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None


class GeneratedImage(BaseModel):
    """Сгенерированная картинка: `prompt` и `model`, которыми создана, плюс ссылка
    `image_url` и/или локальный путь `local_path`."""

    id: int | None = None
    prompt: str
    model: str | None = None
    image_url: str | None = None
    local_path: str | None = None
    created_at: str | None = None


class PhotoAutoUploadJob(BaseModel):
    """Авто-задание: периодически отправлять новые фото из папки в диалог.

    Сканирует `folder_path` каждые `interval_minutes` и шлёт появившиеся файлы в
    `target_dialog_id` под аккаунтом `phone`. Дедуп от повторной отправки ведётся
    не здесь, а по таблице `photo_auto_upload_files` (`has_sent_auto_file` /
    `mark_auto_file_sent`); `last_seen_marker` — лишь вспомогательная отметка
    последнего просмотра (записывается, но сам по себе отправку не блокирует).
    `is_active` включает/выключает задание.
    """

    id: int | None = None
    phone: str
    target_dialog_id: int
    target_title: str | None = None
    target_type: str | None = None
    folder_path: str
    send_mode: PhotoSendMode = PhotoSendMode.ALBUM
    caption: str | None = None
    interval_minutes: int = Field(60, ge=1)
    is_active: bool = True
    error: str | None = None
    last_run_at: datetime | None = None
    last_seen_marker: str | None = None
    created_at: datetime | None = None
