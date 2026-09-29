"""Бэкфилл DM-архива (#1453): обе стороны, свежий снимок, возобновление, гейты.

Прогоны на real Database (архив — настоящий SQL) и фейк-пуле; история стабится
на уровне `dm_archive.read_dialog_history_since` — того же транспортного шва,
что у догона #1428. Список диалогов приходит из фейка
`get_dialogs_for_phone` (сигнатура и результат зеркалят реальный
`DialogFetchResult`: list[dict]-строк снимка + флаг `partial`).
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any

from telethon_floodgate import (
    FloodWaitInfo,
    HandledFloodWaitError,
    TelegramRateLimitedError,
    TelegramRateLimitGate,
)

from src.telegram import dm_archive
from src.telegram.dm_archive import backfill_account


def _utc(**kwargs) -> datetime:
    return datetime.now(timezone.utc) + timedelta(**kwargs)


def _flood_info(seconds: int) -> FloodWaitInfo:
    return FloodWaitInfo(
        operation="history_since",
        phone="+111",
        wait_seconds=seconds,
        next_available_at_utc=_utc(seconds=seconds),
        detail="тестовый флуд",
    )


def _dialog(channel_id: int, channel_type: str = "dm", title: str = "ЧАТ") -> dict:
    """Строка снимка `get_dialogs_for_phone` — форма реального DialogFetchResult."""
    return {
        "channel_id": channel_id,
        "title": title,
        "username": None,
        "channel_type": channel_type,
        "deactivate": False,
        "is_own": False,
    }


class _FakeSnapshot(list):
    """Зеркало DialogFetchResult: список dict-строк + флаги результата."""

    def __init__(self, dialogs: list[dict], *, partial: bool = False):
        super().__init__(dialogs)
        self.partial = partial
        self.saved = len(dialogs)


class _FakeRawClient:
    def __init__(self):
        self.history_calls: list[dict] = []


class _FakePool:
    def __init__(
        self,
        client: _FakeRawClient | None = None,
        snapshot: _FakeSnapshot | None = None,
        *,
        snapshot_error: Exception | None = None,
    ):
        self.clients: dict[str, object] = {}
        if client is not None:
            self.clients["+111"] = SimpleNamespace(raw_client=client)
        self._auth = SimpleNamespace(api_id=1, api_hash="h")
        self._rate_limit_gate: Any = None
        self.snapshot = snapshot if snapshot is not None else _FakeSnapshot([])
        self.snapshot_error = snapshot_error
        self.snapshot_calls: list[dict] = []

    async def get_dialogs_for_phone(
        self,
        phone: str,
        include_dm: bool = False,
        mode: str = "channels_only",
        refresh: bool = False,
    ) -> _FakeSnapshot:
        self.snapshot_calls.append(
            {"phone": phone, "include_dm": include_dm, "mode": mode, "refresh": refresh}
        )
        if self.snapshot_error is not None:
            raise self.snapshot_error
        return self.snapshot


class _FakeMessage:
    def __init__(self, id: int, *, out: bool = False, text: str = "т"):
        self.id = id
        self.out = out
        self.date = _utc(minutes=-1)
        self.text = text


def _stub_history_since(messages_by_chat: dict[int, list[_FakeMessage]]):
    """Страница старейших выше min_id (reverse-паттерн), как реальный адаптер."""

    async def _fake(client, *, api_id, api_hash, peer, min_id=0, limit=500):
        chat_id = int(peer)
        page = [m for m in messages_by_chat.get(chat_id, []) if m.id > min_id]
        client.history_calls.append({"chat_id": chat_id, "min_id": min_id, "limit": limit})
        return page[:limit]

    return _fake


async def _make_db(tmp_path):
    from src.database import Database

    db = Database(str(tmp_path / "dm_archive.db"))
    await db.initialize()
    return db


async def test_backfill_archives_dm_bot_saved_and_skips_channels(tmp_path, monkeypatch):
    """Люди+боты+Saved — обе стороны в архив; каналы/группы мимо.
    Снимок запрашивается свежий (refresh=True, include_dm=True, mode=full).
    """
    db = await _make_db(tmp_path)
    try:
        client = _FakeRawClient()
        snapshot = _FakeSnapshot([
            _dialog(42, "dm", "ДРУГ"),
            _dialog(43, "bot", "БОТ"),
            _dialog(44, "saved", "Избранное (Saved Messages)"),
            _dialog(45, "channel", "канал"),
        ])
        stub = _stub_history_since({
            42: [_FakeMessage(10), _FakeMessage(11, out=True)],
            43: [_FakeMessage(20)],
            44: [_FakeMessage(30, out=True), _FakeMessage(31, out=True)],
        })
        monkeypatch.setattr(dm_archive, "read_dialog_history_since", stub)
        pool = _FakePool(client, snapshot)

        stats = await backfill_account(pool, db, "+111", progress=False)

        assert pool.snapshot_calls == [
            {"phone": "+111", "include_dm": True, "mode": "full", "refresh": True}
        ]
        assert stats == {"dialogs": 3, "archived": 5, "errors": 0}
        incoming, outgoing = await db.repos.dm_messages.count_by_direction("+111")
        assert (incoming, outgoing) == (2, 3)
        cur = await db.db.execute("SELECT DISTINCT chat_id FROM dm_messages ORDER BY chat_id")
        assert [row["chat_id"] for row in await cur.fetchall()] == [42, 43, 44]
    finally:
        await db.close()


async def test_backfill_resumes_from_archive_watermark(tmp_path, monkeypatch):
    """Повторный прогон дочитывает только выше MAX(message_id) архива."""
    db = await _make_db(tmp_path)
    try:
        client = _FakeRawClient()
        stub = _stub_history_since({42: [_FakeMessage(10), _FakeMessage(11), _FakeMessage(12)]})
        monkeypatch.setattr(dm_archive, "read_dialog_history_since", stub)
        pool = _FakePool(client, _FakeSnapshot([_dialog(42)]))

        await backfill_account(pool, db, "+111", progress=False)
        assert await db.repos.dm_messages.max_message_id("+111", 42) == 12

        stats = await backfill_account(pool, db, "+111", progress=False)

        # Продолжение ровно с водяного знака: выше 12 пусто — ни чтения лишнего,
        # ни дублей; курсор возобновления честный.
        assert stats["archived"] == 0
        calls_42 = [call for call in client.history_calls if call["chat_id"] == 42]
        assert calls_42[-1]["min_id"] == 12
        assert await db.repos.dm_messages.count("+111") == 3
    finally:
        await db.close()


async def test_backfill_flood_skips_dialog_continues(tmp_path, monkeypatch):
    """Флуд на одном диалоге не срывает бэкфилл остальных; счётчик ошибок."""
    db = await _make_db(tmp_path)
    try:
        client = _FakeRawClient()
        downstream = _stub_history_since({43: [_FakeMessage(20)]})

        async def _flood_then_ok(cl, *, api_id, api_hash, peer, min_id=0, limit=500):
            if peer == 42:
                raise HandledFloodWaitError(_flood_info(seconds=50000))
            return await downstream(
                cl, api_id=api_id, api_hash=api_hash, peer=peer, min_id=min_id, limit=limit
            )

        monkeypatch.setattr(dm_archive, "read_dialog_history_since", _flood_then_ok)
        pool = _FakePool(client, _FakeSnapshot([_dialog(42), _dialog(43)]))

        stats = await backfill_account(pool, db, "+111", progress=False)

        assert stats["errors"] == 1
        assert stats["archived"] == 1
    finally:
        await db.close()


class _AlwaysRefuseGate(TelegramRateLimitGate):
    """Гейт, всегда отказывающий: сервис гардится isinstance-ом (как в backends)."""

    def __init__(self):
        super().__init__(time_func=lambda: 0.0)
        self.calls = 0

    def try_acquire(self, phone: str, category: str, **kwargs) -> float:
        self.calls += 1
        return 0.01


async def test_backfill_gate_saturation_stops_before_reading(tmp_path, monkeypatch):
    """Насыщенный гейт закрывает прогон до чтения истории; курсор не тронут."""
    db = await _make_db(tmp_path)
    try:
        client = _FakeRawClient()
        stub = _stub_history_since({42: [_FakeMessage(10)]})
        monkeypatch.setattr(dm_archive, "read_dialog_history_since", stub)
        pool = _FakePool(client, _FakeSnapshot([_dialog(42)]))
        gate = _AlwaysRefuseGate()
        pool._rate_limit_gate = gate

        stats = await backfill_account(pool, db, "+111", progress=False)

        assert stats["archived"] == 0
        assert stats["incomplete"] is True  # CLI не напечатает «готово»
        assert gate.calls == 2  # выждал ожидание, второй отказ — стоп
        assert client.history_calls == []
    finally:
        await db.close()


async def test_backfill_chat_ids_filter(tmp_path, monkeypatch):
    """chat_ids сужает прогон до указанных диалогов."""
    db = await _make_db(tmp_path)
    try:
        client = _FakeRawClient()
        stub = _stub_history_since({42: [_FakeMessage(10)], 43: [_FakeMessage(20)]})
        monkeypatch.setattr(dm_archive, "read_dialog_history_since", stub)
        pool = _FakePool(client, _FakeSnapshot([_dialog(42), _dialog(43)]))

        stats = await backfill_account(pool, db, "+111", chat_ids={43}, progress=False)

        assert stats["dialogs"] == 1
        assert [call["chat_id"] for call in client.history_calls] == [43]
    finally:
        await db.close()


async def test_backfill_snapshot_failure_stops_before_reading(tmp_path, monkeypatch):
    """Нетранзиентный флуд/лимит на свежем снимке — стоп с incomplete,
    ни одного чтения истории («следующий чат» снова триггерил бы листинг,
    паттерн бана #1330).
    """
    db = await _make_db(tmp_path)
    try:
        for error in (
            HandledFloodWaitError(_flood_info(seconds=50000)),
            TelegramRateLimitedError("+111", "dialogs_page", 5.0),
        ):
            client = _FakeRawClient()
            monkeypatch.setattr(
                dm_archive, "read_dialog_history_since", _stub_history_since({42: [_FakeMessage(10)]})
            )
            pool = _FakePool(client, snapshot_error=error)

            stats = await backfill_account(pool, db, "+111", progress=False)

            assert stats == {"dialogs": 0, "archived": 0, "errors": 0, "incomplete": True}
            assert client.history_calls == []
            assert await db.repos.dm_messages.count("+111") == 0
    finally:
        await db.close()


async def test_backfill_partial_snapshot_stops_before_reading(tmp_path, monkeypatch):
    """Неполный снимок — стоп: чтение по нему промолчало бы о ненакрытых чатах."""
    db = await _make_db(tmp_path)
    try:
        client = _FakeRawClient()
        monkeypatch.setattr(
            dm_archive, "read_dialog_history_since", _stub_history_since({42: [_FakeMessage(10)]})
        )
        pool = _FakePool(client, _FakeSnapshot([_dialog(42)], partial=True))

        stats = await backfill_account(pool, db, "+111", progress=False)

        assert stats == {"dialogs": 0, "archived": 0, "errors": 0, "incomplete": True}
        assert client.history_calls == []
    finally:
        await db.close()


async def test_backfill_requires_connected_client(tmp_path):
    db = await _make_db(tmp_path)
    try:
        try:
            await backfill_account(_FakePool(None), db, "+111", progress=False)
        except RuntimeError as exc:
            assert "нет подключенного клиента" in str(exc)
        else:
            raise AssertionError("ожидали RuntimeError без клиента")
    finally:
        await db.close()
