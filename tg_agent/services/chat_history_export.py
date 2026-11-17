"""Live chat-history export to a structured JSON dataset file (#1855).

One export run turns one or more chats into a single JSON file shaped as
``{"chats": {"<marked dialog id>": {chat_id, chat_title, messages_exported,
messages}}}``. The walk goes newest→oldest in :data:`EXPORT_PAGE_SIZE`
batches through the existing ``read_history`` action — the same action the
CLI's ``dialogs read`` uses, so the native pool and the gateway's
``RemoteActionService`` serve it alike and this module adds no new Telegram
RPC surface.

The output file doubles as resume state: a re-run with ``resume=True``
skips every chat already present under its marked id, so a partial export
(flood-wait break, crash, Ctrl-C) continues instead of starting over.

The walker composes only :meth:`TelegramActionService.read_history`, which
both the native pool and the gateway's ``RemoteActionService`` already serve
as an established read operation — this module introduces no new Telegram RPC
surface. Paging by ``offset_id`` mirrors Telegram Desktop's export: each page
asks for messages older than the last id of the previous one.

Read-only by construction: nothing in this module mutates Telegram state.
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from tg_agent.models import DialogMessage

logger = logging.getLogger(__name__)

EXPORT_PAGE_SIZE = 100


@dataclass
class ChatHistoryExportReport:
    """Summary of one export run (what the CLI prints)."""

    output_path: str
    chats_processed: int
    chats_skipped: int
    messages_exported: int


def parse_export_offset_date(raw: str | None) -> datetime | None:
    """Parse ``--offset-date``: full ISO timestamp or ``YYYY-MM-DD`` (UTC).

    Returns ``None`` for an empty value; raises ``ValueError`` on anything
    else — an ignored date filter would silently widen the export.
    """
    raw = (raw or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        try:
            parsed = datetime.strptime(raw, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except ValueError as exc:
            raise ValueError(
                f"--offset-date must be ISO (YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS), got {raw!r}"
            ) from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _message_record(msg: DialogMessage) -> dict:
    return {
        "id": msg.id,
        "dialog_id": msg.dialog_id,
        "sender_id": msg.sender_id,
        "sender_name": msg.sender_name,
        "out": msg.out,
        "date": msg.date.isoformat() if msg.date else None,
        "text": msg.text,
        "media_type": msg.media_type,
        "reply_to_id": msg.reply_to_id,
        "is_forward": msg.is_forward,
    }


async def _walk_chat_history(actions, *, phone: str, chat_id, cutoff: datetime | None,
                             limit: int | None, first_page) -> list[dict]:
    """Continue a page walk newest→oldest; return exported message records.

    ``first_page`` is the already-fetched newest page (its ``dialog_id`` keys
    the chat, so resolution costs no extra request). ``cutoff`` drops the
    newer-than-cutoff prefix (the walk always starts at the newest message,
    so filtered-out messages can only precede kept ones). ``limit`` bounds
    the number of records kept, not pages fetched.
    """
    records: list[dict] = []
    page = first_page
    while True:
        for msg in page.messages:
            if cutoff is not None and msg.date >= cutoff:
                continue
            records.append(_message_record(msg))
            if limit is not None and len(records) >= limit:
                return records
        offset_id = page.messages[-1].id if page.messages else 0
        if not page.messages or len(page.messages) < EXPORT_PAGE_SIZE or offset_id == 0:
            return records
        page = await actions.read_history(
            phone=phone,
            chat_id=chat_id,
            limit=EXPORT_PAGE_SIZE,
            offset_id=offset_id,
        )


def _load_prior_export(path: Path) -> dict:
    """Load a previous export file for resume; unreadable → start fresh.

    A corrupted prior file is not silently overwritten with a bare ``{}``:
    the caller logs what it loaded, and the export overwrites the path only
    once its own first chat completes.
    """
    if not path.exists():
        return {}
    try:
        with open(path, "r", encoding="utf-8") as fh:
            prior = json.load(fh)
    except (json.JSONDecodeError, OSError):
        logger.warning("Could not read prior export at %s — starting fresh", path)
        return {}
    return prior if isinstance(prior, dict) else {}


async def _persist_export(path: Path, result: dict) -> None:
    """Write the export file atomically-enough: whole payload, one call.

    Called after every processed chat, so a crash/Ctrl-C/failing page mid-run
    keeps everything exported before the failure — the resume promise holds
    for interrupted runs, not only completed ones.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(result, indent=2, ensure_ascii=False)
    await asyncio.to_thread(path.write_text, payload, "utf-8")


async def export_chat_history(
    actions,
    *,
    phone: str,
    chat_identifiers: list[str],
    output_path: str | Path,
    resume: bool = True,
    limit: int | None = None,
    offset_date: str | None = None,
) -> ChatHistoryExportReport:
    """Export the history of each chat to one structured JSON file.

    ``actions`` is a :class:`TelegramActionService` (or the gateway's
    duck-typed ``RemoteActionService``) — anything serving ``read_history``.
    Chats are keyed in the file by their marked dialog id; ``chat_title``
    carries the caller's identifier string (no extra entity fetch).
    """
    if limit is not None and limit <= 0:
        raise ValueError("limit must be a positive integer")
    cutoff = parse_export_offset_date(offset_date)
    out = Path(output_path)

    result = _load_prior_export(out) if resume else {}
    result.setdefault("chats", {})

    stats = {"processed": 0, "skipped": 0, "messages": 0}
    for identifier in chat_identifiers:
        identifier = identifier.strip()
        if not identifier:
            continue
        # The newest page doubles as chat resolution: its dialog_id is the
        # resume key, so a skipped chat costs one page, not an extra RPC.
        newest = await actions.read_history(
            phone=phone, chat_id=identifier, limit=EXPORT_PAGE_SIZE, offset_id=0
        )
        numeric_id = str(newest.dialog_id)
        if resume and numeric_id in result["chats"]:
            stats["skipped"] += 1
            continue
        records = await _walk_chat_history(
            actions, phone=phone, chat_id=identifier, cutoff=cutoff, limit=limit,
            first_page=newest,
        )
        result["chats"][numeric_id] = {
            "chat_id": newest.dialog_id,
            "chat_title": identifier,
            "messages_exported": len(records),
            "messages": records,
        }
        stats["processed"] += 1
        stats["messages"] += len(records)
        # Persist after every chat: an interrupted run keeps its finished chats.
        await _persist_export(out, result)

    return ChatHistoryExportReport(
        output_path=str(out),
        chats_processed=stats["processed"],
        chats_skipped=stats["skipped"],
        messages_exported=stats["messages"],
    )
