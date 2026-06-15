"""Form/query parsing and validation for the scheduler web domain (#654)."""

from __future__ import annotations

import re

VALID_STATUS_FILTERS = {"all", "active", "completed"}

_VALID_JOB_ID_RE = re.compile(
    r"^(collect_all|photo_due|photo_auto|warm_all_dialogs|sq_\d+|pipeline_run_\d+|content_generate_\d+)$"
)


def is_valid_job_id(job_id: str) -> bool:
    return bool(_VALID_JOB_ID_RE.match(job_id))


def normalize_page(page: int) -> int:
    return max(1, page)


def normalize_limit(limit: int) -> int:
    return max(10, min(limit, 100))


def normalize_status(status: str) -> str:
    return status if status in VALID_STATUS_FILTERS else "all"


def parse_interval_minutes(form) -> int | None:
    """Parse and clamp the ``interval_minutes`` form field to 1..1440.

    Returns ``None`` when the field is missing or non-integer.
    """
    try:
        minutes = int(form["interval_minutes"])
    except (KeyError, ValueError):
        return None
    return max(1, min(minutes, 1440))
