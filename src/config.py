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
    search_interval_minutes: int = 60
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


class SecurityConfig(BaseModel):
    session_encryption_key: str = ""


class AppConfig(BaseModel):
    telegram: TelegramConfig = TelegramConfig()
    web: WebConfig = WebConfig()
    scheduler: SchedulerConfig = SchedulerConfig()
    notifications: NotificationsConfig = NotificationsConfig()
    database: DatabaseConfig = DatabaseConfig()
    llm: LLMConfig = LLMConfig()
    security: SecurityConfig = SecurityConfig()


_ENV_PATTERN = re.compile(r"\$\{(\w+)\}")


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
        return AppConfig()

    with open(path) as f:
        raw = yaml.safe_load(f) or {}

    substituted = _walk_and_substitute(raw)
    return AppConfig.model_validate(substituted)


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
