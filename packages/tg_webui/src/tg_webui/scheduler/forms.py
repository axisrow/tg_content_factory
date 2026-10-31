"""Form/query parsing and validation for the scheduler web domain (#654)."""

from __future__ import annotations

import re

VALID_STATUS_FILTERS = {"all", "active", "completed"}

_VALID_JOB_ID_RE = re.compile(
    r"^(collect_all|photo_due|photo_auto|warm_all_dialogs|sq_\d+|pipeline_run_\d+|content_generate_\d+)$"
)


def is_valid_job_id(job_id: str) -> bool:
    return bool(_VALID_JOB_ID_RE.match(job_id))


def canonical_job_id(job_id: str) -> str:
    """Map a deprecated pipeline_run_<id> job id to its live content_generate_<id> equivalent.

    pipeline_run_ is no longer a periodic scheduler job (#835/2): content_generate_ is the
    single periodic job per pipeline. Toggling/configuring the stale pipeline_run_ row must
    act on the real content_generate_ job, not a dead scheduler_job_disabled:pipeline_run_<id>
    key. Other job ids pass through unchanged.
    """
    if job_id.startswith("pipeline_run_"):
        return "content_generate_" + job_id.removeprefix("pipeline_run_")
    return job_id


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
