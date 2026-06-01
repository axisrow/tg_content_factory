"""Request/form parsing for the channels web domain."""

from __future__ import annotations

from starlette.datastructures import FormData


def parse_channel_ids(form: FormData) -> list[str]:
    """Extract the multi-value ``channel_ids`` field from a bulk-add form."""
    return form.getlist("channel_ids")


def parse_tags(raw: object) -> list[str]:
    """Split a comma-separated ``tags`` field into a clean list."""
    return [t.strip() for t in str(raw or "").split(",") if t.strip()]
