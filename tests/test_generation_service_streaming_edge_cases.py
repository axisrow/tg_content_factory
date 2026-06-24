"""Streaming edge-case coverage for ``GenerationService.generate_stream`` (issue #1034).

Tier-2 bug-hunt: SSE streaming edge cases that the happy-path tests in
``test_generation_service_streaming.py`` never exercised.

Each test here was written red-first against the pre-fix code:
  * mid-stream break (provider closes the connection / ``asyncio.TimeoutError``)
    used to propagate the raw exception and DROP the partial text;
  * ``None``/non-string chunks used to leak the literal ``"None"`` into the
    generated text and emit empty deltas;
  * citation building used to duplicate citations for a message returned twice.

Fakes deliberately mirror the real provider contract from
``generation_service.generate_stream`` (a coroutine resolving to an async
generator), so the tests stay faithful to production behaviour.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import pytest

from src.models import Message, SearchResult
from src.services.generation_service import GenerationService


class DummySearchEngine:
    """Returns a fixed message list from ``search_hybrid`` (matches existing fakes)."""

    def __init__(self, messages):
        self._messages = messages

    async def search_hybrid(self, query: str, **kwargs) -> SearchResult:
        return SearchResult(messages=self._messages, total=len(self._messages), query=query)


def _msg(message_id: int = 42, *, channel_id: int = 10, title: str | None = "C", text: str = "hi") -> Message:
    return Message(
        id=1,
        channel_id=channel_id,
        message_id=message_id,
        sender_id=None,
        sender_name="A",
        text=text,
        date=datetime.now(timezone.utc),
        collected_at=None,
        channel_title=title,
        channel_username="u",
    )


async def _drain(svc: GenerationService, **kwargs) -> list[dict]:
    updates: list[dict] = []
    async for update in svc.generate_stream(query="q", prompt_template="Use {source_messages}", **kwargs):
        updates.append(update)
    return updates


# ---------------------------------------------------------------------------
# Bug A: mid-stream break — provider closes connection / TimeoutError
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "raised",
    [ConnectionError("provider closed connection mid-stream"), asyncio.TimeoutError()],
    ids=["connection_error", "timeout_error"],
)
async def test_midstream_break_returns_partial_text(raised):
    """Provider raising mid-iteration must NOT lose the already-streamed text.

    Pre-fix the raw exception propagated out of ``generate_stream`` and the
    partial buffer (``part1 part2 ``) was lost. The fix catches the exception,
    yields a final update flagged ``partial=True``/``stream_error``, and returns.
    """

    async def provider(prompt: str = "", **kwargs):
        async def _gen():
            yield "part1 "
            yield "part2 "
            raise raised

        return _gen()

    svc = GenerationService(DummySearchEngine([_msg()]), provider_callable=provider)

    updates = await _drain(svc)

    # The stream completed gracefully (no exception escaped _drain).
    assert updates, "expected at least the partial updates"
    final = updates[-1]
    assert final["generated_text"] == "part1 part2 ", "partial text must be preserved"
    assert final["partial"] is True
    assert final["stream_error"], "stream_error must describe the failure"


async def test_midstream_break_first_chunk_yields_error_marker():
    """A provider that fails on the very first chunk still terminates gracefully."""

    async def provider(prompt: str = "", **kwargs):
        async def _gen():
            raise ConnectionError("dropped before first chunk")
            yield  # pragma: no cover - makes this an async generator

        return _gen()

    svc = GenerationService(DummySearchEngine([_msg()]), provider_callable=provider)

    updates = await _drain(svc)

    assert len(updates) == 1
    assert updates[0]["generated_text"] == ""
    assert updates[0]["partial"] is True
    assert updates[0]["stream_error"]


async def test_successful_stream_has_no_partial_flag():
    """Regression guard: a clean stream must NOT be flagged partial.

    Mutating the fix to always set ``partial=True`` would turn every successful
    generation into a "partial" one — this pins the happy path.
    """

    async def provider(prompt: str = "", **kwargs):
        async def _gen():
            yield "all "
            yield "good"

        return _gen()

    svc = GenerationService(DummySearchEngine([_msg()]), provider_callable=provider)

    updates = await _drain(svc)

    assert updates[-1]["generated_text"] == "all good"
    assert updates[-1].get("partial") in (None, False)
    assert not updates[-1].get("stream_error")


# ---------------------------------------------------------------------------
# Bug B: None / empty chunks must not pollute the generated text
# ---------------------------------------------------------------------------


async def test_none_chunks_do_not_leak_literal_none():
    """A ``None`` chunk must be skipped, not coerced to the string ``"None"``.

    Pre-fix ``str(None)`` produced ``"None"`` inside ``generated_text``
    (``"aNoneb"``) and an empty chunk produced a do-nothing delta.
    """

    async def provider(prompt: str = "", **kwargs):
        async def _gen():
            yield "a"
            yield None
            yield ""
            yield "b"

        return _gen()

    svc = GenerationService(DummySearchEngine([_msg()]), provider_callable=provider)

    updates = await _drain(svc)

    assert updates[-1]["generated_text"] == "ab", "None/empty chunks must not enter the text"
    # No update should carry an empty or "None" delta.
    deltas = [u["delta"] for u in updates]
    assert "None" not in deltas
    assert "" not in deltas
    assert deltas == ["a", "b"]


async def test_dict_chunk_with_none_text_is_skipped():
    """A dict chunk whose text fields are all ``None`` must not leak ``"None"``."""

    async def provider(prompt: str = "", **kwargs):
        async def _gen():
            yield {"text": "x"}
            yield {"text": None, "content": None, "generated_text": None}
            yield {"content": "y"}

        return _gen()

    svc = GenerationService(DummySearchEngine([_msg()]), provider_callable=provider)

    updates = await _drain(svc)

    assert updates[-1]["generated_text"] == "xy"
    assert all(u["delta"] for u in updates)
    assert "None" not in updates[-1]["generated_text"]


async def test_sync_iterator_none_chunks_skipped():
    """The synchronous-iterator branch must also drop ``None``/empty chunks."""

    def provider(prompt: str = "", **kwargs):
        return iter(["x", None, "", "y"])

    svc = GenerationService(DummySearchEngine([_msg()]), provider_callable=provider)

    updates = await _drain(svc)

    assert updates[-1]["generated_text"] == "xy"
    assert [u["delta"] for u in updates] == ["x", "y"]
