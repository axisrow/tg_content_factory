from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

from src.config import load_config, resolve_session_encryption_secret
from src.database import Database
from src.settings_utils import parse_int_setting
from src.telegram.auth import TelegramAuth
from src.telegram.client_pool import ClientPool
from src.web.paths import PROJECT_ROOT

_DATA_ROOT = PROJECT_ROOT / "data"
_LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
_LOG_DATEFMT = "%Y-%m-%d %H:%M:%S"
_TUI_LOG_PATH = _DATA_ROOT / "agent_tui.log"
APP_LOG_PATH = _DATA_ROOT / "app.log"


def setup_logging(log_path: Path | None = None) -> None:
    log_path = log_path or APP_LOG_PATH
    logging.basicConfig(
        level=logging.INFO,
        format=_LOG_FORMAT,
        datefmt=_LOG_DATEFMT,
    )
    root = logging.getLogger()
    for h in root.handlers:
        if isinstance(h, RotatingFileHandler) and getattr(h, "baseFilename", None) == str(log_path):
            return
    log_path.parent.mkdir(parents=True, exist_ok=True)
    rfh = RotatingFileHandler(
        str(log_path),
        maxBytes=10 * 1024 * 1024,
        backupCount=3,
        encoding="utf-8",
    )
    rfh.setFormatter(logging.Formatter(_LOG_FORMAT, datefmt=_LOG_DATEFMT))
    root.addHandler(rfh)


def ensure_data_dirs() -> None:
    """Create all data subdirectories once at startup."""
    for sub in ("image", "images", "downloads", "photo_uploads", "telegram_sessions"):
        (_DATA_ROOT / sub).mkdir(parents=True, exist_ok=True)


def redirect_logging_to_file(path: str | Path = _TUI_LOG_PATH) -> list[logging.Handler]:
    """Replace console handlers with file handler for TUI mode. Returns removed handlers."""
    root = logging.getLogger()
    removed: list[logging.Handler] = []
    for h in root.handlers[:]:
        if isinstance(h, logging.StreamHandler) and not isinstance(h, logging.FileHandler):
            removed.append(h)
            root.removeHandler(h)
    fh = logging.FileHandler(str(path), encoding="utf-8")
    fh.setFormatter(logging.Formatter(_LOG_FORMAT, datefmt=_LOG_DATEFMT))
    root.addHandler(fh)
    return removed


def restore_logging(removed_handlers: list[logging.Handler] | logging.Handler | None) -> None:
    """Restore console handlers after TUI exits."""
    root = logging.getLogger()
    for h in root.handlers[:]:
        # Remove only plain FileHandlers (TUI log), not RotatingFileHandler (app.log)
        if type(h) is logging.FileHandler:
            h.close()
            root.removeHandler(h)
    if isinstance(removed_handlers, list):
        for h in removed_handlers:
            root.addHandler(h)
    elif removed_handlers:
        root.addHandler(removed_handlers)


async def init_db(config_path: str):
    config = load_config(config_path)
    db = Database(
        config.database.path,
        session_encryption_secret=resolve_session_encryption_secret(config),
    )
    await db.initialize()
    return config, db


async def init_pool(config, db: Database):
    api_id = config.telegram.api_id
    api_hash = config.telegram.api_hash
    if api_id == 0 or not api_hash:
        stored_id = await db.get_setting("tg_api_id")
        stored_hash = await db.get_setting("tg_api_hash")
        if stored_id and stored_hash:
            api_id = parse_int_setting(
                stored_id,
                setting_name="tg_api_id",
                default=0,
                logger=logging.getLogger(__name__),
            )
            api_hash = stored_hash

    auth = TelegramAuth(api_id, api_hash)
    pool = ClientPool(
        auth,
        db,
        config.scheduler.max_flood_wait_sec,
        runtime_config=config.telegram_runtime,
    )
    await pool.initialize()
    return auth, pool
