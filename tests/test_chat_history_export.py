"""Telegram-Desktop-style live history export (#1855, port of telegram-mcp#384).

The export walks a chat's history newest→oldest in pages over the existing
``read_history`` action (works both for the native pool and the gateway's
RemoteActionService — no new Telegram RPC surface), keeps one JSON file with
a ``chats`` map keyed by the marked dialog id, and can resume: chats already
present in the file are skipped on a re-run.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from tg_agent.models import DialogMessage, MessagesResult
from tg_agent.services.chat_history_export import (
    export_chat_history,
    parse_export_offset_date,
)


def _msg(msg_id: int, *, dialog_id: int = 1001, date: datetime | None = None) -> DialogMessage:
    return DialogMessage(
        id=msg_id,
        dialog_id=dialog_id,
        sender_id=42,
        sender_name="Alice",
        out=False,
        date=date or datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc),
        text=f"hello {msg_id}",
        media_type=None,
        reply_to_id=None,
        is_forward=False,
    )


class FakeActions:
    """Duck-typed TelegramActionService: paginates a per-chat message store."""

    def __init__(self, messages_by_chat: dict[int, list[DialogMessage]]):
        self._store = messages_by_chat
        self.calls: list[dict] = []

    async def read_history(self, *, phone, chat_id, limit=50, offset_id=0):
        self.calls.append(
            {"chat_id": chat_id, "limit": limit, "offset_id": offset_id}
        )
        dialog_id = self.resolve[chat_id]
        messages = self._store.get(dialog_id, [])
        page = [m for m in messages if m.id < offset_id] if offset_id else list(messages)
        return MessagesResult(
            phone=phone, dialog_id=dialog_id, messages=page[:limit]
        )

    resolve: dict = {}


def _fake_actions(messages_by_chat, resolve=None):
    actions = FakeActions(messages_by_chat)
    actions.resolve = resolve or {
        f"chat-{k}": k for k in messages_by_chat
    }
    return actions


# ---------------------------------------------------------------------------
# offset_date parsing
# ---------------------------------------------------------------------------


def test_parse_offset_date_accepts_iso_and_date_only():
    assert parse_export_offset_date("2026-01-01T00:00:00Z") == datetime(
        2026, 1, 1, tzinfo=timezone.utc
    )
    assert parse_export_offset_date("2026-01-01") == datetime(
        2026, 1, 1, tzinfo=timezone.utc
    )
    assert parse_export_offset_date(None) is None
    assert parse_export_offset_date("") is None


def test_parse_offset_date_rejects_garbage():
    with pytest.raises(ValueError, match="offset-date"):
        parse_export_offset_date("not-a-date")


# ---------------------------------------------------------------------------
# Pagination and output shape
# ---------------------------------------------------------------------------


async def test_export_walks_full_history_in_pages(tmp_path):
    actions = _fake_actions({1001: [_msg(i) for i in range(250, 0, -1)]})
    out = tmp_path / "history.json"

    report = await export_chat_history(
        actions, phone="+7000", chat_identifiers=["chat-1001"], output_path=out
    )

    assert report.chats_processed == 1
    assert report.chats_skipped == 0
    assert report.messages_exported == 250
    # 250 messages at 100/page = 3 pages.
    assert len(actions.calls) == 3
    data = json.loads(out.read_text(encoding="utf-8"))
    chat = data["chats"]["1001"]
    assert chat["chat_id"] == 1001
    assert chat["messages_exported"] == 250
    assert len(chat["messages"]) == 250
    assert chat["messages"][0]["id"] == 250  # newest first
    assert chat["messages"][-1]["id"] == 1


async def test_export_limit_bounds_messages_per_chat(tmp_path):
    actions = _fake_actions({1002: [_msg(i, dialog_id=1002) for i in range(100, 0, -1)]})
    out = tmp_path / "limited.json"

    report = await export_chat_history(
        actions, phone="+7000", chat_identifiers=["chat-1002"], output_path=out, limit=25
    )

    assert report.messages_exported == 25
    data = json.loads(out.read_text(encoding="utf-8"))
    assert len(data["chats"]["1002"]["messages"]) == 25


async def test_export_invalid_limit_rejected(tmp_path):
    actions = _fake_actions({})
    with pytest.raises(ValueError, match="limit"):
        await export_chat_history(
            actions, phone="+7000", chat_identifiers=["chat-1"],
            output_path=tmp_path / "x.json", limit=0,
        )


async def test_export_offset_date_keeps_only_older_messages(tmp_path):
    msgs = [
        _msg(10, dialog_id=1003, date=datetime(2026, 3, 1, tzinfo=timezone.utc)),
        _msg(9, dialog_id=1003, date=datetime(2026, 1, 1, tzinfo=timezone.utc)),
    ]
    actions = _fake_actions({1003: msgs})
    out = tmp_path / "dated.json"

    report = await export_chat_history(
        actions, phone="+7000", chat_identifiers=["chat-1003"],
        output_path=out, offset_date="2026-02-01",
    )

    assert report.messages_exported == 1
    data = json.loads(out.read_text(encoding="utf-8"))
    assert data["chats"]["1003"]["messages"][0]["id"] == 9


async def test_export_resume_skips_already_exported_chats(tmp_path):
    actions = _fake_actions({1005: [_msg(1, dialog_id=1005)]})
    out = tmp_path / "resume.json"

    await export_chat_history(
        actions, phone="+7000", chat_identifiers=["chat-1005"], output_path=out
    )
    report = await export_chat_history(
        actions, phone="+7000", chat_identifiers=["chat-1005"], output_path=out
    )

    assert report.chats_processed == 0
    assert report.chats_skipped == 1
    assert report.messages_exported == 0
    # The previous export survived the re-run.
    data = json.loads(out.read_text(encoding="utf-8"))
    assert data["chats"]["1005"]["messages_exported"] == 1


async def test_export_no_resume_reexports(tmp_path):
    actions = _fake_actions({1006: [_msg(1, dialog_id=1006)]})
    out = tmp_path / "fresh.json"

    await export_chat_history(
        actions, phone="+7000", chat_identifiers=["chat-1006"], output_path=out
    )
    report = await export_chat_history(
        actions, phone="+7000", chat_identifiers=["chat-1006"], output_path=out, resume=False
    )

    assert report.chats_processed == 1
    assert report.chats_skipped == 0


async def test_export_empty_chat_writes_empty_list(tmp_path):
    actions = _fake_actions({1007: []})
    out = tmp_path / "empty.json"

    report = await export_chat_history(
        actions, phone="+7000", chat_identifiers=["chat-1007"], output_path=out
    )

    assert report.messages_exported == 0
    data = json.loads(out.read_text(encoding="utf-8"))
    assert data["chats"]["1007"]["messages"] == []


class _BoomOnSecondChat(FakeActions):
    """FakeActions whose walk of the second chat dies mid-run."""

    async def read_history(self, *, phone, chat_id, limit=50, offset_id=0):
        if chat_id == "chat-2002":
            raise RuntimeError("boom")
        return await super().read_history(
            phone=phone, chat_id=chat_id, limit=limit, offset_id=offset_id
        )


async def test_export_persists_each_chat_incrementally(tmp_path):
    """A crash mid-run must not discard chats exported earlier in the run —
    the resume promise holds for interrupted runs, not only completed ones
    (#1862 review)."""
    actions = _BoomOnSecondChat(
        {
            2001: [_msg(1, dialog_id=2001), _msg(2, dialog_id=2001)],
            2002: [_msg(3, dialog_id=2002)],
        }
    )
    actions.resolve = {"chat-2001": 2001, "chat-2002": 2002}
    out = tmp_path / "partial.json"

    with pytest.raises(RuntimeError, match="boom"):
        await export_chat_history(
            actions, phone="+7000", chat_identifiers=["chat-2001", "chat-2002"],
            output_path=out,
        )

    data = json.loads(out.read_text(encoding="utf-8"))
    assert "2001" in data["chats"]
    assert data["chats"]["2001"]["messages_exported"] == 2
    assert "2002" not in data["chats"]


async def test_export_multiple_chats_and_subdir_output(tmp_path):
    actions = _fake_actions(
        {
            2001: [_msg(1, dialog_id=2001)],
            2002: [_msg(2, dialog_id=2002), _msg(3, dialog_id=2002)],
        }
    )
    out = tmp_path / "datasets" / "multi.json"

    report = await export_chat_history(
        actions, phone="+7000", chat_identifiers=["chat-2001", "chat-2002"], output_path=out
    )

    assert report.chats_processed == 2
    assert report.messages_exported == 3
    data = json.loads(out.read_text(encoding="utf-8"))
    assert set(data["chats"]) == {"2001", "2002"}
