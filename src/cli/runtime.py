from __future__ import annotations

import logging

from src.config import load_config, resolve_session_encryption_secret
from src.database import Database
from src.settings_utils import parse_int_setting
from src.telegram.auth import TelegramAuth
from src.telegram.client_pool import ClientPool


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


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
    pool = ClientPool(auth, db, config.scheduler.max_flood_wait_sec)
    await pool.initialize()
    return auth, pool
