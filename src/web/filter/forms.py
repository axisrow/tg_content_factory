"""Request/form parsing and validation for the filter web domain."""

from __future__ import annotations

from typing import cast

from starlette.datastructures import FormData

from src.filters.criteria import VALID_FLAGS
from src.filters.models import ChannelFilterResult

# Confirmation phrase required by the hard-delete-all flow; defeats blind direct POSTs.
HARD_DELETE_ALL_CONFIRM_PHRASE = "DELETE_ALL_FILTERED"


def parse_snapshot(values: list[str]) -> list[ChannelFilterResult]:
    """Parse the apply-filters snapshot of ``<channel_id>|<flags_csv>`` tokens."""
    deduped: dict[int, list[str]] = {}
    for value in values:
        channel_id_str, sep, flags_csv = value.partition("|")
        if not sep:
            continue
        try:
            channel_id = int(channel_id_str)
        except ValueError:
            continue
        flags = [f for f in (f.strip() for f in flags_csv.split(",")) if f in VALID_FLAGS]
        if not flags:
            continue
        deduped[channel_id] = flags
    return [
        ChannelFilterResult(channel_id=channel_id, flags=flags, is_filtered=True)
        for channel_id, flags in deduped.items()
    ]


def parse_pks(form: FormData, field: str = "pks") -> list[int]:
    """Extract integer primary keys from a multi-value form field."""
    pks: list[int] = []
    for v in form.getlist(field):
        try:
            pks.append(int(cast(str, v)))
        except (ValueError, TypeError):
            continue
    return pks


def parse_confirm_pairs(raw: str) -> list[tuple[int, int]] | None:
    """Parse the hard-delete-all snapshot as ``pk:channel_id`` pairs.

    Each token must be ``<pk>:<channel_id>`` where both are integers.
    Duplicate ``pk`` values (or duplicate ``channel_id`` values) are rejected
    so a crafted ``"1:1001,1:1001"`` cannot smuggle a delete past the set
    comparison. Empty/whitespace input is treated as an empty snapshot so
    the no_filtered_channels branch stays reachable through the normal form
    flow. Returns ``None`` when any token is malformed.

    Binding to ``channel_id`` (the Telegram-assigned identifier, not the
    SQLite rowid) guards against PK reuse: if the rendered row is deleted
    and a new row is inserted between render and submit, the new row will
    likely have a different ``channel_id`` and the comparison will reject.
    """
    tokens = [tok.strip() for tok in (raw or "").split(",") if tok.strip()]
    pairs: list[tuple[int, int]] = []
    seen_pks: set[int] = set()
    seen_chids: set[int] = set()
    for tok in tokens:
        parts = tok.split(":")
        if len(parts) != 2:
            return None
        try:
            pk = int(parts[0])
            chid = int(parts[1])
        except ValueError:
            return None
        if pk in seen_pks or chid in seen_chids:
            return None
        seen_pks.add(pk)
        seen_chids.add(chid)
        pairs.append((pk, chid))
    return pairs
