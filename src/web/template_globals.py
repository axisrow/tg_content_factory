from __future__ import annotations

import importlib.metadata
import logging
import os
import re
import tomllib
from datetime import datetime, timezone

from fastapi import Request
from fastapi.templating import Jinja2Templates
from markupsafe import Markup, escape

from src.config import AppConfig, is_provider_model_ref
from src.web.paths import TEMPLATES_DIR

logger = logging.getLogger(__name__)

FILTER_FLAG_EMOJI = {
    "low_uniqueness": (Markup('<i class="bi bi-phone-slash"></i>'), "Низкая уникальность контента"),
    "low_subscriber_ratio": (Markup('<i class="bi bi-bar-chart"></i>'), "Мало подписчиков"),
    "low_subscriber_manual": (Markup('<i class="bi bi-hand-index"></i>'), "Мало подписчиков (абс.)"),
    "manual": (Markup('<i class="bi bi-slash-circle"></i>'), "Ручная фильтрация"),
    "cross_channel_spam": (Markup('<i class="bi bi-megaphone"></i>'), "Кросс-канальный спам"),
    "non_cyrillic": (Markup('<i class="bi bi-globe"></i>'), "Не кириллический контент"),
    "chat_noise": (Markup('<i class="bi bi-chat-dots"></i>'), "Шум чата"),
    "username_changed": (Markup('<i class="bi bi-lightbulb"></i>'), "Сменил юзернейм"),
    "title_changed": (Markup('<i class="bi bi-pencil-square"></i>'), "Смена названия"),
    "suspicious_username": (Markup('<i class="bi bi-dice-5"></i>'), "Подозрительный юзернейм"),
}

FLASH_MESSAGES = {
    "channel_added": "Канал добавлен.",
    "channels_added": "Каналы добавлены.",
    "channel_toggled": "Статус канала изменён.",
    "channel_deleted": "Канал удалён.",
    "account_connected": "Аккаунт подключён.",
    "credentials_saved": "API-данные сохранены.",
    "filters_saved": "Фильтры сохранены.",
    "scheduler_saved": "Настройки планировщика сохранены.",
    "account_toggled": "Статус аккаунта изменён.",
    "account_deleted": "Аккаунт удалён.",
    "notification_account_saved": "Аккаунт для уведомлений сохранён.",
    "notification_bot_created": "Notification bot создан.",
    "notification_bot_deleted": "Notification bot удалён.",
    "agent_saved": "Настройки AI Agent сохранены.",
    "scheduler_started": "Планировщик запущен.",
    "scheduler_stopped": "Планировщик остановлен.",
    "collect_started": "Сбор сообщений запущен в фоне.",
    "collect_queued": "Задача добавлена в очередь. Выполнится после текущего сбора.",
    "collect_all_queued": (
        'Задачи на загрузку добавлены. Откройте <a href="/scheduler">планировщик</a>, '
        'чтобы следить за прогрессом.'
    ),
    "collect_all_noop": (
        'Новых задач не добавлено. Откройте <a href="/scheduler">планировщик</a>, '
        'чтобы проверить текущую очередь.'
    ),
    "collect_all_empty": "Нет активных каналов для загрузки.",
    "test_notification_sent": "Тестовое уведомление отправлено.",
    "test_notification_failed": "Не удалось отправить уведомление.",
    "task_cancelled": "Задача отменена.",
    "pending_collect_tasks_deleted": "Запланированные задачи на загрузку удалены из базы.",
    "pending_collect_tasks_empty": "Запланированных задач на загрузку нет.",
    "already_running": "Сбор уже запущен.",
    "triggered": "Сбор запущен вручную.",
    "filter_toggled": "Фильтр канала изменён.",
    "filter_applied": "Фильтры применены.",
    "filter_reset": "Фильтры сброшены.",
    "channel_not_found": "Канал не найден.",
    "stats_collection_started": "Сбор статистики запущен в фоне.",
    "stats_collection_queued": "Сбор статистики поставлен в очередь.",
    "search_triggered": "Поиск запущен.",
    "no_accounts": "Для начала работы добавьте Telegram-аккаунт.",
    "sq_added": "Поисковый запрос добавлен.",
    "sq_deleted": "Поисковый запрос удалён.",
    "sq_toggled": "Статус поискового запроса изменён.",
    "sq_run": "Поисковый запрос выполнен.",
    "sq_edited": "Поисковый запрос обновлён.",
    # Unified pipeline messages (deduplicated: "Пайплайн" is the preferred Russian term)
    "pipeline_added": "Пайплайн добавлен.",
    "pipeline_added_no_llm": (
        "Pipeline создан, но для его запуска требуется LLM-провайдер. "
        "Настройте провайдера в /settings."
    ),
    "pipeline_deleted": "Пайплайн удалён.",
    "pipeline_toggled": "Статус пайплайна изменён.",
    "pipeline_edited": "Пайплайн обновлён.",
    "pipeline_run_enqueued": "Запуск pipeline поставлен в очередь.",
    "pipeline_run_with_since": "Запуск поставлен в очередь.",
    "pipeline_dry_run_enqueued": "Тест pipeline поставлен в очередь.",
    "pipeline_published": "Pipeline опубликован.",
    "photo_sent": "Фото отправлены.",
    "photo_scheduled": "Фото поставлены в отложенную отправку Telegram.",
    "photo_batch_created": "Batch photo tasks созданы.",
    "photo_auto_created": "Авто-загрузка создана.",
    "photo_run_due_ok": "Due photo tasks обработаны.",
    "photo_item_cancelled": "Photo task отменён.",
    "photo_auto_toggled": "Статус auto job изменён.",
    "photo_auto_deleted": "Auto job удалён.",
    "deleted_filtered": "Каналы полностью удалены из базы.",
    "purged_selected": "Сообщения выбранных каналов очищены.",
    "purged_all_filtered": "Сообщения всех отфильтрованных каналов очищены.",
    "precheck_done": "Предпроверка подписчиков выполнена.",
    "semantic_saved": "Настройки semantic search сохранены.",
    "semantic_indexed": "Индексация semantic search завершена.",
    "translation_saved": "Настройки перевода сохранены.",
    "translation_backfill_done": "Определение языков завершено.",
    "translation_run_started": "Фоновый перевод запущен.",
    "rename_filtered": "Канал оставлен в фильтре.",
    "rename_kept": "Канал разфильтрован и оставлен активным.",
    "rename_accepted": "Переименование принято. Канал возвращён в сбор.",
    "rename_accepted_still_filtered": "Переименование принято, но канал остаётся в фильтре по другим причинам.",
    "rename_already_decided": "Решение по этому событию уже было принято.",
}

FLASH_ERRORS = {
    "resolve": "Не удалось найти канал. Проверьте username или ссылку.",
    "no_client": "Нет подключённых аккаунтов для поиска канала.",
    "collecting": "Сбор уже выполняется. Дождитесь завершения.",
    "shutting_down": "Приложение завершает работу.",
    "bot_not_configured": "Бот не подключён. Настройте уведомления в Настройках.",
    "stats_running": "Сбор статистики уже выполняется.",
    "channel_filtered": "Канал помечен как бесполезный и исключён из сбора.",
    "filter_snapshot_required": "Сначала выполните анализ фильтров и примените его из отчёта.",
    "invalid_value": "Указано некорректное значение.",
    "invalid_api_id": "API ID должен состоять только из цифр.",
    "agent_dev_mode_required": "Для управления deepagents providers включите режим разработчика.",
    "agent_prompt_template_invalid": "Шаблон промпта содержит неподдерживаемые или некорректные переменные.",
    "notification_account_invalid": "Выбран неизвестный аккаунт уведомлений.",
    "agent_provider_secret_required": "Для управления deepagents providers требуется SESSION_ENCRYPTION_KEY.",
    "agent_provider_invalid": "Выбран неизвестный провайдер deepagents.",
    "notification_account_unavailable": "Выбранный аккаунт уведомлений недоступен.",
    "notification_bot_missing": "Для выбранного аккаунта notification bot не найден.",
    "notification_action_failed": "Не удалось выполнить действие с notification bot.",
    "no_filtered_channels": "Нет отфильтрованных каналов для удаления.",
    "dev_mode_required_for_hard_delete": "Для полного удаления каналов включите режим разработчика в настройках.",
    "notification_test_failed": "Не удалось отправить тестовое уведомление.",
    "invalid_account": "Выбран неизвестный аккаунт.",
    "not_found": "Объект не найден.",
    "photo_send_failed": "Не удалось отправить фото.",
    "photo_target_required": "Сначала выберите канал, чат или личный диалог.",
    "photo_target_invalid": "Выбранная цель недоступна. Выберите её заново.",
    "photo_schedule_failed": "Не удалось создать отложенную отправку фото.",
    "photo_batch_failed": "Не удалось создать batch photo tasks.",
    "photo_auto_failed": "Не удалось создать или изменить auto job.",
    "photo_run_due_failed": "Не удалось выполнить due photo tasks.",
    "semantic_invalid_value": "Некорректные настройки semantic search.",
    "semantic_unavailable": "Текущий semantic backend недоступен в этой среде.",
    "photo_item_cancel_failed": "Не удалось отменить photo task.",
    "pipeline_invalid": "Параметры pipeline некорректны или dialog cache не заполнен.",
    "pipeline_run_failed": "Не удалось поставить запуск pipeline в очередь.",
}

PACKAGE_NAME = "tg-agent"
PROJECT_ROOT = TEMPLATES_DIR.parent.parent.parent
PYPROJECT_PATH = PROJECT_ROOT / "pyproject.toml"


def get_app_version() -> str:
    try:
        with PYPROJECT_PATH.open("rb") as fh:
            data = tomllib.load(fh)
        version = data["project"]["version"]
        if isinstance(version, str) and version:
            return version
    except Exception:
        logger.warning("Failed to read app version from %s", PYPROJECT_PATH, exc_info=True)

    try:
        return importlib.metadata.version(PACKAGE_NAME)
    except importlib.metadata.PackageNotFoundError:
        pass
    except Exception:
        logger.warning(
            "Failed to read package metadata version for %s",
            PACKAGE_NAME,
            exc_info=True,
        )

    return "unknown"


def _agent_available(config: AppConfig | None = None) -> bool:
    claude_available = bool(
        os.environ.get("ANTHROPIC_API_KEY") or os.environ.get("CLAUDE_CODE_OAUTH_TOKEN")
    )
    fallback_model = ""
    if config is not None:
        fallback_model = config.agent.fallback_model
    if not fallback_model:
        fallback_model = os.environ.get("AGENT_FALLBACK_MODEL", "").strip()
    return claude_available or is_provider_model_ref(fallback_model)


def _request_agent_manager(request: Request):
    manager = getattr(request.app.state, "agent_manager", None)
    if manager is not None:
        return manager
    container = getattr(request.app.state, "container", None)
    if container is None:
        return None
    return getattr(container, "agent_manager", None)


def _agent_available_for_request(request: Request) -> bool:
    manager = _request_agent_manager(request)
    if manager is not None:
        return bool(manager.available)
    return _agent_available(getattr(request.app.state, "config", None))


def local_dt_filter(value: datetime | str | None, fmt: str = "datetime") -> Markup:
    """Jinja2 filter: renders a UTC datetime as a client-side localised span.

    The span contains the ISO-8601 UTC string in ``data-utc`` and the desired
    format key in ``data-fmt``.  A small JS snippet in base.html converts it to
    the browser's local time on load and after every HTMX swap.
    """
    if value is None:
        return Markup("—")

    # Normalise to an ISO-8601 string with explicit UTC offset so that
    # JavaScript's Date() always interprets it as UTC.
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        iso = value.isoformat()
    else:
        s = str(value).strip()
        if not s:
            return Markup("—")
        # Append Z if there is no timezone indicator already (handles both +HH:MM and -HH:MM)
        if s[-1] != "Z" and not re.search(r"[+-]\d{2}:\d{2}$", s):
            s = s + "Z"
        iso = s

    # Server-side fallback text shown before JS runs
    if isinstance(value, datetime):
        fallback = value.strftime("%Y-%m-%d %H:%M")
    else:
        fallback = str(value)[:16]

    return Markup(f'<span class="local-dt" data-utc="{escape(iso)}" data-fmt="{escape(fmt)}">{escape(fallback)}</span>')


def configure_template_globals(
    templates: Jinja2Templates,
    config: AppConfig | None = None,
) -> Jinja2Templates:
    templates.env.globals["agent_available"] = _agent_available_for_request
    templates.env.globals["app_version"] = get_app_version()
    templates.env.globals["filter_flag_emoji"] = FILTER_FLAG_EMOJI
    templates.env.globals["flash_messages"] = FLASH_MESSAGES
    templates.env.globals["flash_errors"] = FLASH_ERRORS
    templates.env.filters["local_dt"] = local_dt_filter
    return templates
