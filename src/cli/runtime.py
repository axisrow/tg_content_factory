from __future__ import annotations

import logging
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


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format=_LOG_FORMAT,
        datefmt=_LOG_DATEFMT,
    )


def ensure_data_dirs() -> None:
    """Create all data subdirectories once at startup."""
    for sub in ("image", "images", "downloads", "photo_uploads", "telegram_sessions"):
        (_DATA_ROOT / sub).mkdir(parents=True, exist_ok=True)


def redirect_logging_to_file(path: str | Path = _TUI_LOG_PATH) -> logging.Handler | None:
    """Replace console handler with file handler for TUI mode. Returns removed handler."""
    root = logging.getLogger()
    removed = None
    for h in root.handlers[:]:
        if isinstance(h, logging.StreamHandler) and not isinstance(h, logging.FileHandler):
            removed = h
            root.removeHandler(h)
    fh = logging.FileHandler(str(path), encoding="utf-8")
    fh.setFormatter(logging.Formatter(_LOG_FORMAT, datefmt=_LOG_DATEFMT))
    root.addHandler(fh)
    return removed


def restore_logging(removed_handler: logging.Handler | None) -> None:
    """Restore console handler after TUI exits."""
    root = logging.getLogger()
    for h in root.handlers[:]:
        if isinstance(h, logging.FileHandler):
            h.close()
            root.removeHandler(h)
    if removed_handler:
        root.addHandler(removed_handler)


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
