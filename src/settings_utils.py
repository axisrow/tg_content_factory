from __future__ import annotations

import logging


def parse_int_setting(
    raw_value: object,
    *,
    setting_name: str,
    default: int,
    logger: logging.Logger,
) -> int:
    if raw_value in (None, ""):
        return default
    try:
        return int(raw_value)
    except (TypeError, ValueError):
        logger.warning("Invalid %s in settings DB (%r), using %d", setting_name, raw_value, default)
        return default
