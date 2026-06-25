"""Конфигурация приложения: Pydantic-модели секций + загрузчик из YAML.

[`AppConfig`][src.config.AppConfig] — корень дерева настроек; каждая вложенная
секция — отдельная `*Config`-модель с дефолтами, так что приложение запускается
и без `config.yaml`. `load_config` читает YAML, подставляет `${ENV_VAR}` и
добирает критичные значения (Telegram-креды, agent-модель) прямо из окружения.
"""

from __future__ import annotations

import logging
import os
import re
from pathlib import Path

import yaml
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


class TelegramConfig(BaseModel):
    """Учётные данные приложения Telegram (`api_id`/`api_hash`) для MTProto.

    Дефолты-пустышки заполняются из `TG_API_ID`/`TG_API_HASH`, если в YAML их нет.
    """

    api_id: int = 0
    api_hash: str = ""


class WebConfig(BaseModel):
    """Настройки веб-панели: адрес/порт прослушивания и пароль входа.

    Вход — по паролю; имя пользователя задаётся переменной `WEB_USER`
    (по умолчанию `admin`), а не захардкожено — см. `src/web/panel_auth.py`.
    """

    host: str = "0.0.0.0"
    port: int = 8080
    password: str = ""


class SchedulerConfig(BaseModel):
    """Параметры планировщика сбора: периодичность, задержки и таймауты.

    Управляет интервалом периодического сбора и паузами между каналами/запросами
    (троттлинг против FLOOD_WAIT), таймаутами потокового сбора и фоновых задач, а
    также числом параллельных воркеров сбора/статистики (0 = авто — по одному на
    подключённый аккаунт). Семантика «0/отрицательное» у таймаутов разнится —
    см. комментарии у полей.
    """

    collect_interval_minutes: int = 60
    delay_between_channels_sec: int = 2
    delay_between_requests_sec: int = 1
    # Per-post idle timeout while streaming a channel. 0/negative = disabled.
    collection_stream_timeout_sec: float = 120.0
    # asyncio.wait_for timeout for the background filter-analyze task (#793);
    # 0 or negative disables the timeout.
    filter_analyze_timeout_sec: float = 600.0
    # asyncio.wait_for timeout around runtime-snapshot publishing. 0/negative or
    # garbage is NOT "disabled" — it falls back to the safe default (a zero
    # timeout would abort every heartbeat); see resolve_snapshot_publish_timeout.
    snapshot_publish_timeout_sec: float = 30.0
    max_flood_wait_sec: int = 300
    # Parallel message-collection workers. 0 or unset = auto (one per
    # connected Telegram account, capped at 10).
    collection_worker_count: int = 0
    stats_worker_count: int = 3
    stats_all_max_channels_per_run: int = 10
    stats_all_cooldown_sec: int = 600
    stats_all_worker_count: int = 1
    stats_all_skip_fresh_hours: int = 24


class NotificationsConfig(BaseModel):
    """Настройки уведомлений: чат администратора для алертов (`admin_chat_id`) и
    префиксы имени/username при автосоздании бота уведомлений через BotFather."""

    admin_chat_id: int | None = None
    bot_name_prefix: str = "LeadHunter"
    bot_username_prefix: str = "leadhunter_"


class DatabaseConfig(BaseModel):
    """Путь к файлу SQLite и тюнинг производительности соединений (#760).

    `read_pool_size` — число read-соединений (читают параллельно, медленный
    SELECT не блокирует навигацию). `cache_size_kb` — кэш страниц на соединение
    (суммарная RAM ≈ `cache_size_kb * (read_pool_size + 1)`). `mmap_size_mb` —
    окно memory-mapped I/O (адресное пространство, не RAM; 0 отключает). Для
    `:memory:` read-пул не разворачивается (одно соединение); per-connection
    PRAGMA-тюнинг (кэш/mmap) применяется как обычно.
    """

    path: str = "data/tg_search.db"
    # Number of read connections in the pool (#760). Reads run concurrently across
    # these, so a slow SELECT never blocks navigation. Ignored for :memory:.
    read_pool_size: int = 4
    # SQLite page cache per connection in KB (#760). Total committed RAM is roughly
    # cache_size_kb * (read_pool_size + 1); raise for large (multi-GB) databases.
    # gt=0: a 0/negative value would make PRAGMA cache_size=-0 silently revert to
    # the tiny ~8 MB default (review #940).
    cache_size_kb: int = Field(64000, gt=0)
    # Memory-mapped I/O window in MB (#760). Virtual address space backed by the OS
    # page cache, not per-connection committed RAM — safe to keep large on big DBs.
    # ge=0: 0 disables mmap (valid); a negative value would mean "map the whole
    # file" to SQLite and could exhaust the address space (review #940).
    mmap_size_mb: int = Field(256, ge=0)


class LLMConfig(BaseModel):
    """Базовые LLM-настройки из конфига: провайдер, модель и ключ.

    `enabled` включает LLM-функции; на практике провайдеры чаще авторегистрируются
    из переменных окружения (`ProviderService`), а это — fallback из YAML.
    """

    enabled: bool = False
    provider: str = "openai"
    model: str = "gpt-4o-mini"
    api_key: str = ""


class AgentConfig(BaseModel):
    """Настройки агент-бэкенда: модель, deepagents-fallback и таймауты стрима.

    `model` — основная модель агента, `fallback_model`/`fallback_api_key` —
    запасной провайдер (формат `provider:model`). Группа `*_timeout` ограничивает
    стадии стримингового вызова (закрытие стрима, первое событие, простой,
    подтверждение, общий потолок) — защита от зависаний.
    """

    model: str = ""
    fallback_model: str = ""
    fallback_api_key: str = ""
    stream_close_timeout: int = 60
    first_event_timeout: int = 120
    idle_timeout: int = 90
    permission_timeout: int = 120
    total_timeout: int = 300


class SecurityConfig(BaseModel):
    """Параметры безопасности: ключ шифрования StringSession аккаунтов.

    Непустой `session_encryption_key` включает хранение сессий как `enc:v2:*`;
    см. [`resolve_session_encryption_secret`][src.config.resolve_session_encryption_secret].
    """

    session_encryption_key: str = ""


class TelegramRuntimeConfig(BaseModel):
    """Выбор Telegram-рантайма: режим бэкенда (`backend_mode`), транспорт CLI
    (`cli_transport`) и каталог кэша сессий (`session_cache_dir`). Все три можно
    переопределить переменными `TG_*` при загрузке."""

    backend_mode: str = "auto"
    cli_transport: str = "hybrid"
    session_cache_dir: str = "data/telegram_sessions"


class ProductionLimitsConfig(BaseModel):
    """Opt-in rate-limit + daily cost cap for paid LLM/image generation (#814).

    ``enabled`` defaults to False so existing deployments keep their current
    (unlimited) behavior; the limits only apply when an operator turns them on.
    """

    enabled: bool = False
    requests_per_minute: int = 60
    tokens_per_minute: int = 100000
    tokens_per_day: int = 1000000
    cost_per_1k_tokens: float = 0.002
    cost_per_image: float = 0.02
    daily_cost_cap: float = 10.0


class AppConfig(BaseModel):
    """Корень конфигурации приложения: агрегирует все секции `*Config`.

    Каждая секция со своими дефолтами, поэтому валидный `AppConfig()` собирается
    без файла. Загружается и валидируется через
    [`load_config`][src.config.load_config].
    """

    telegram: TelegramConfig = TelegramConfig()
    telegram_runtime: TelegramRuntimeConfig = TelegramRuntimeConfig()
    web: WebConfig = WebConfig()
    scheduler: SchedulerConfig = SchedulerConfig()
    notifications: NotificationsConfig = NotificationsConfig()
    database: DatabaseConfig = DatabaseConfig()
    llm: LLMConfig = LLMConfig()
    agent: AgentConfig = AgentConfig()
    security: SecurityConfig = SecurityConfig()
    production_limits: ProductionLimitsConfig = ProductionLimitsConfig()


_ENV_PATTERN = re.compile(r"\$\{(\w+)\}")


def is_provider_model_ref(value: str) -> bool:
    """True, если строка имеет вид `provider:model` с непустыми частями.

    Используется для валидации ссылок на модель (например, agent-fallback), где
    обязателен префикс провайдера.
    """
    provider, separator, model = value.partition(":")
    return bool(separator and provider.strip() and model.strip())


def _substitute_env(value: str) -> str:
    """Replace ${VAR} placeholders with environment variable values."""

    def _replace(match: re.Match) -> str:
        var_name = match.group(1)
        return os.environ.get(var_name, "")

    return _ENV_PATTERN.sub(_replace, value)


def _walk_and_substitute(obj: object) -> object:
    if isinstance(obj, str):
        return _substitute_env(obj)
    if isinstance(obj, dict):
        result = {}
        for k, v in obj.items():
            substituted = _walk_and_substitute(v)
            # Drop keys where env var resolved to empty string
            if substituted == "" and isinstance(v, str) and _ENV_PATTERN.search(v):
                continue
            result[k] = substituted
        return result
    if isinstance(obj, list):
        return [_walk_and_substitute(item) for item in obj]
    return obj


def load_config(path: str | Path = "config.yaml") -> AppConfig:
    """Load application config from YAML, substituting env variables."""
    path = Path(path)
    if not path.exists():
        config = AppConfig()
    else:
        with open(path) as f:
            raw = yaml.safe_load(f) or {}

        substituted = _walk_and_substitute(raw)
        config = AppConfig.model_validate(substituted)

    # Direct environment fallback for Telegram credentials keeps the app usable
    # even when config.yaml omits placeholders or the file is absent.
    if config.telegram.api_id == 0:
        env_api_id = os.environ.get("TG_API_ID", "").strip()
        if env_api_id.isdigit():
            config.telegram.api_id = int(env_api_id)
    if not config.telegram.api_hash:
        config.telegram.api_hash = os.environ.get("TG_API_HASH", "").strip()
    env_backend_mode = os.environ.get("TG_BACKEND_MODE", "").strip()
    if env_backend_mode:
        config.telegram_runtime.backend_mode = env_backend_mode
    env_cli_transport = os.environ.get("TG_CLI_TRANSPORT", "").strip()
    if env_cli_transport:
        config.telegram_runtime.cli_transport = env_cli_transport
    env_session_cache_dir = os.environ.get("TG_SESSION_CACHE_DIR", "").strip()
    if env_session_cache_dir:
        config.telegram_runtime.session_cache_dir = env_session_cache_dir
    if not config.agent.model:
        config.agent.model = os.environ.get("AGENT_MODEL", "").strip()
    if not config.agent.fallback_model:
        config.agent.fallback_model = os.environ.get("AGENT_FALLBACK_MODEL", "").strip()
    if not config.agent.fallback_api_key:
        config.agent.fallback_api_key = os.environ.get("AGENT_FALLBACK_API_KEY", "").strip()
    if config.agent.fallback_model and not is_provider_model_ref(config.agent.fallback_model):
        logger.warning(
            "Invalid AGENT_FALLBACK_MODEL %r. Expected provider:model; deepagents fallback "
            "will stay disabled until it is corrected.",
            config.agent.fallback_model,
        )
    return config


def resolve_session_encryption_secret(config: AppConfig) -> str | None:
    """Resolve a stable secret for account session encryption.

    Returns ``None`` when no suitable secret is available — the caller should
    skip encryption rather than use a well-known default.
    """
    if config.security.session_encryption_key:
        return config.security.session_encryption_key
    logger.warning(
        "No SESSION_ENCRYPTION_KEY configured. "
        "New account sessions will be stored in plaintext, and an encrypted DB will fail to start. "
        "Set SESSION_ENCRYPTION_KEY."
    )
    return None
