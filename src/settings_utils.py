from __future__ import annotations

import logging
import math


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
        # int() raising TypeError on unsupported objects is the handled path below.
        return int(raw_value)  # type: ignore[call-overload]
    except (TypeError, ValueError):
        logger.warning("Invalid %s in settings DB (%r), using %d", setting_name, raw_value, default)
        return default


def parse_float_setting(
    raw_value: object,
    *,
    setting_name: str,
    default: float,
    logger: logging.Logger,
) -> float:
    if raw_value in (None, ""):
        return default
    try:
        # float() raising TypeError on unsupported objects is the handled path below.
        value = float(raw_value)  # type: ignore[arg-type]
        # float("nan")/float("inf") parse without raising; reject them so callers
        # that int()-coerce or clamp the result don't crash or propagate NaN.
        if not math.isfinite(value):
            raise ValueError("non-finite value")
        return value
    except (TypeError, ValueError):
        logger.warning("Invalid %s in settings DB (%r), using %s", setting_name, raw_value, default)
        return default
