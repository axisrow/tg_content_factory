from __future__ import annotations

import logging
import os
import re
from pathlib import Path

import yaml
from pydantic import BaseModel

logger = logging.getLogger(__name__)


class TelegramConfig(BaseModel):
    api_id: int = 0
    api_hash: str = ""


class WebConfig(BaseModel):
    host: str = "0.0.0.0"
    port: int = 8080
    password: str = ""


class SchedulerConfig(BaseModel):
    collect_interval_minutes: int = 60
    delay_between_channels_sec: int = 2
    delay_between_requests_sec: int = 1
    max_flood_wait_sec: int = 300


class NotificationsConfig(BaseModel):
    admin_chat_id: int | None = None
    bot_name_prefix: str = "LeadHunter"
    bot_username_prefix: str = "leadhunter_"


class DatabaseConfig(BaseModel):
    path: str = "data/tg_search.db"


class LLMConfig(BaseModel):
    enabled: bool = False
    provider: str = "openai"
    model: str = "gpt-4o-mini"
    api_key: str = ""


class AgentConfig(BaseModel):
    model: str = ""
    fallback_model: str = ""
    fallback_api_key: str = ""
    stream_close_timeout: int = 60
    first_event_timeout: int = 120
    idle_timeout: int = 90
    permission_timeout: int = 120
    total_timeout: int = 600


class SecurityConfig(BaseModel):
    session_encryption_key: str = ""


class TelegramRuntimeConfig(BaseModel):
    backend_mode: str = "auto"
    cli_transport: str = "hybrid"
    session_cache_dir: str = "data/telegram_sessions"


class AppConfig(BaseModel):
    telegram: TelegramConfig = TelegramConfig()
    telegram_runtime: TelegramRuntimeConfig = TelegramRuntimeConfig()
    web: WebConfig = WebConfig()
    scheduler: SchedulerConfig = SchedulerConfig()
    notifications: NotificationsConfig = NotificationsConfig()
    database: DatabaseConfig = DatabaseConfig()
    llm: LLMConfig = LLMConfig()
    agent: AgentConfig = AgentConfig()
    security: SecurityConfig = SecurityConfig()


_ENV_PATTERN = re.compile(r"\$\{(\w+)\}")


def is_provider_model_ref(value: str) -> bool:
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
