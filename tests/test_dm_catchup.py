"""Догон пропущенного входящего DM (#1428, эпик #1416 этап 2.3).

Прогоны на real Database (журнал/политика — настоящий SQL) и фейк-пуле:
`history_since` стабится на уровне `dm_catchup.read_dialog_history_since` —
транспортного слоя tg_messenger, чьи инварианты (второе соединение,
listen-запрет) покрывают тесты `dm_history`. Приоритеты/режимы — на уровне
`DmCatchupSettings` плюс сквозные прогоны сервиса.
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any

from telethon_floodgate import (
    FloodWaitInfo,
    HandledFloodWaitError,
    TelegramRateLimitGate,
)

from src.models import (
    DM_CATCHUP_SETTING_KEY,
    DmCatchupOverride,
    DmCatchupSettings,
    IncomingDm,
)
from src.telegram import dm_catchup
from src.telegram.dm_catchup import DmCatchupService, load_dm_catchup_settings


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


class _FakeMessage:
    def __init__(self, id: int, *, out: bool = False, date: datetime | None = None, text="т"):
        self.id = id
        self.out = out
        self.date = date if date is not None else _utc(minutes=-1)
        self.text = text
        self.dialog_id = 0
        self.sender_id = None
        self.sender_name = None
        self.media_type = None
        self.reply_to_id = None
        self.is_forward = False


class _FakeRawClient:
    def __init__(self):
        self.history_calls: list[dict] = []


class _FakePool:
    """Только то, что использует догон: карта raw-клиентов, креды, резолв."""

    def __init__(self, client: _FakeRawClient | None):
        self.clients: dict[str, object] = {}
        if client is not None:
            self.clients["+111"] = SimpleNamespace(raw_client=client)
        self._auth = SimpleNamespace(api_id=1, api_hash="h")
        self._rate_limit_gate: Any = None

    async def resolve_dialog_entity(self, client, phone, dialog_id, *, target_type=None):
        return SimpleNamespace(user_id=dialog_id)


def _stub_history_since(messages_by_chat: dict[int, list[_FakeMessage]]):
    """Стаб транспортного слоя: peer.user_id → страница истории (хронологическая)."""

    async def _fake(client, *, api_id, api_hash, peer, min_id=0, limit=200):
        chat_id = peer.user_id
        page = [m for m in messages_by_chat.get(chat_id, []) if m.id > min_id]
        client.history_calls.append({"chat_id": chat_id, "min_id": min_id, "limit": limit})
        return page[-limit:]

    return _fake


async def _make_db(tmp_path):
    from src.database import Database

    db = Database(str(tmp_path / "dm_catchup.db"))
    await db.initialize()
    await db.repos.dialog_cache.replace_dialogs(
        "+111",
        [
            {"channel_id": 42, "channel_type": "dm", "title": "ДРУГ"},
            {"channel_id": 43, "channel_type": "bot", "title": "БОТ"},
            {"channel_id": 44, "channel_type": "channel", "title": "канал"},
        ],
    )
    return db


async def _journal_processed(db) -> dict[int, bool]:
    """(message_id → processed) журнала диалога 42."""
    cur = await db.execute("SELECT message_id, processed FROM incoming_dms WHERE chat_id = 42")
    return {row["message_id"]: bool(row["processed"]) for row in await cur.fetchall()}


# --- режимы ---


async def test_mode_full_journals_fresh_as_unprocessed(tmp_path, monkeypatch):
    """«Нагнать полностью»: свежие входящие в журнале, processed=0 (ждут черновик)."""
    db = await _make_db(tmp_path)
    try:
        client = _FakeRawClient()
        stub = _stub_history_since({42: [_FakeMessage(10), _FakeMessage(11, out=True)]})
        monkeypatch.setattr(dm_catchup, "read_dialog_history_since", stub)
        await db.set_setting(
            DM_CATCHUP_SETTING_KEY, DmCatchupSettings(mode="full").model_dump_json()
        )
        service = DmCatchupService(_FakePool(client), db)

        stats = await service.run_for_phone("+111")

        assert stats["stored"] == 1  # out=True не журналируется
        assert await db.repos.incoming_dms.count_unprocessed() == 1
        # канал 44 не догоняется, бот-диалог 43 — догоняется
        assert [call["chat_id"] for call in client.history_calls] == [42, 43]
    finally:
        await db.close()


async def test_mode_journal_only_marks_everything_processed(tmp_path, monkeypatch):
    """«Нагнать, но не отвечать»: строки записаны, черновики автомат не готовит."""
    db = await _make_db(tmp_path)
    try:
        stub = _stub_history_since({42: [_FakeMessage(10), _FakeMessage(11)]})
        monkeypatch.setattr(dm_catchup, "read_dialog_history_since", stub)
        await db.set_setting(
            DM_CATCHUP_SETTING_KEY,
            DmCatchupSettings(mode="journal_only").model_dump_json(),
        )
        service = DmCatchupService(_FakePool(_FakeRawClient()), db)

        stats = await service.run_for_phone("+111")

        assert stats["stored"] == 2
        assert await db.repos.incoming_dms.count_unprocessed() == 0
    finally:
        await db.close()


async def test_mode_ignore_touches_nothing(tmp_path, monkeypatch):
    """«Забыть»: журнал не растёт, история даже не читается."""
    db = await _make_db(tmp_path)
    try:
        client = _FakeRawClient()
        stub = _stub_history_since({42: [_FakeMessage(10)]})
        monkeypatch.setattr(dm_catchup, "read_dialog_history_since", stub)
        await db.set_setting(
            DM_CATCHUP_SETTING_KEY, DmCatchupSettings(mode="ignore").model_dump_json()
        )
        service = DmCatchupService(_FakePool(client), db)

        stats = await service.run_for_phone("+111")

        assert stats["stored"] == 0
        assert client.history_calls == []
        assert await db.repos.incoming_dms.count_unprocessed() == 0
    finally:
        await db.close()


# --- порог давности ---


async def test_staleness_threshold_old_gets_no_draft(tmp_path, monkeypatch):
    """Старше порога — в журнале (показ), но processed=1: черновик не готовится."""
    db = await _make_db(tmp_path)
    try:
        old = _FakeMessage(10, date=_utc(hours=-2))
        fresh = _FakeMessage(11, date=_utc(minutes=-2))
        stub = _stub_history_since({42: [old, fresh]})
        monkeypatch.setattr(dm_catchup, "read_dialog_history_since", stub)
        await db.set_setting(
            DM_CATCHUP_SETTING_KEY,
            DmCatchupSettings(mode="full", staleness_sec=3600).model_dump_json(),
        )
        service = DmCatchupService(_FakePool(_FakeRawClient()), db)

        await service.run_for_phone("+111")

        assert await db.repos.incoming_dms.count_unprocessed() == 1  # только свежее
        assert await _journal_processed(db) == {10: True, 11: False}
    finally:
        await db.close()


async def test_redelivery_does_not_flip_processed_back(tmp_path, monkeypatch):
    """Повторная доставка не откатывает разобранное (гонка со слушателем).

    Сценарий из дизайн-заметки #1440: слушатель записал сообщение, пока догон
    уже читал страницу с меньшим водяным знаком. Страница догона содержит
    дубликат — INSERT OR IGNORE молчит, processed=1 не сбрасывается.
    """
    db = await _make_db(tmp_path)
    try:
        stub = _stub_history_since({42: [_FakeMessage(12)]})
        monkeypatch.setattr(dm_catchup, "read_dialog_history_since", stub)
        await db.set_setting(
            DM_CATCHUP_SETTING_KEY,
            DmCatchupSettings(mode="full", staleness_sec=3600).model_dump_json(),
        )
        service = DmCatchupService(_FakePool(_FakeRawClient()), db)

        # Водяной знак прохода — 11; в этот же момент «слушатель» успевает
        # записать 12 как разобранное, и страница догона несёт дубликат.
        original_max = db.repos.incoming_dms.max_message_id
        raced = False

        async def _racing_max(phone: str, chat_id: int) -> int:
            nonlocal raced
            watermark = await original_max(phone, chat_id)
            if not raced:
                raced = True
                await db.repos.incoming_dms.record(
                    IncomingDm(
                        phone="+111",
                        chat_id=42,
                        message_id=12,
                        message_date=_utc(minutes=-1),
                        received_at=_utc(minutes=-1),
                    ),
                    processed=True,
                )
            return watermark

        monkeypatch.setattr(db.repos.incoming_dms, "max_message_id", _racing_max)

        stats = await service.run_for_phone("+111")

        assert stats["stored"] == 0
        assert stats["already"] == 1
        assert await _journal_processed(db) == {12: True}  # решение цело
    finally:
        await db.close()


async def test_watermark_resumes_from_journal_max(tmp_path, monkeypatch):
    """Водяной знак: повторный проход читает историю от MAX(message_id) журнала."""
    db = await _make_db(tmp_path)
    try:
        client = _FakeRawClient()
        stub = _stub_history_since({42: [_FakeMessage(10), _FakeMessage(11)]})
        monkeypatch.setattr(dm_catchup, "read_dialog_history_since", stub)
        await db.set_setting(
            DM_CATCHUP_SETTING_KEY, DmCatchupSettings(mode="full").model_dump_json()
        )
        service = DmCatchupService(_FakePool(client), db)
        await service.run_for_phone("+111")

        await service.run_for_phone("+111")

        calls_42 = [call for call in client.history_calls if call["chat_id"] == 42]
        assert calls_42[-1]["min_id"] == 11
        assert await _journal_processed(db) == {10: False, 11: False}
    finally:
        await db.close()


# --- приоритет переопределений ---


async def test_account_override_beats_global(tmp_path, monkeypatch):
    """Глобаль ignore, аккаунт +111 full: +111 догоняется, чужой аккаунт нет."""
    db = await _make_db(tmp_path)
    try:
        client = _FakeRawClient()
        stub = _stub_history_since({42: [_FakeMessage(10)]})
        monkeypatch.setattr(dm_catchup, "read_dialog_history_since", stub)
        await db.set_setting(
            DM_CATCHUP_SETTING_KEY,
            DmCatchupSettings(
                mode="ignore", accounts={"+111": DmCatchupOverride(mode="full")}
            ).model_dump_json(),
        )
        service = DmCatchupService(_FakePool(client), db)

        own = await service.run_for_phone("+111")
        other = await service.run_for_phone("+999")  # нет ни клиента, ни переопределения

        assert own["stored"] == 1
        assert other == {}
    finally:
        await db.close()


async def test_dialog_override_beats_account(tmp_path, monkeypatch):
    """Аккаунт full, диалог journal_only: у чата 42 черновика нет, у 43 ждёт."""
    db = await _make_db(tmp_path)
    try:
        stub = _stub_history_since({42: [_FakeMessage(10)], 43: [_FakeMessage(20)]})
        monkeypatch.setattr(dm_catchup, "read_dialog_history_since", stub)
        await db.set_setting(
            DM_CATCHUP_SETTING_KEY,
            DmCatchupSettings(
                mode="ignore",
                accounts={"+111": DmCatchupOverride(mode="full")},
                dialogs={"+111:42": DmCatchupOverride(mode="journal_only")},
            ).model_dump_json(),
        )
        service = DmCatchupService(_FakePool(_FakeRawClient()), db)

        await service.run_for_phone("+111")

        assert await db.repos.incoming_dms.count_unprocessed() == 1  # только чат 43
        assert (await _journal_processed(db))[10] is True
    finally:
        await db.close()


async def test_dialog_staleness_override_merges_fieldwise(tmp_path, monkeypatch):
    """Поканальное слияние: режим с уровня глобали, порог — с уровня диалога."""
    db = await _make_db(tmp_path)
    try:
        # старое для диалогового порога 60с, но ещё свежее для глобального 3600с
        msg = _FakeMessage(10, date=_utc(minutes=-5))
        stub = _stub_history_since({42: [msg]})
        monkeypatch.setattr(dm_catchup, "read_dialog_history_since", stub)
        await db.set_setting(
            DM_CATCHUP_SETTING_KEY,
            DmCatchupSettings(
                mode="full", dialogs={"+111:42": DmCatchupOverride(staleness_sec=60)}
            ).model_dump_json(),
        )
        service = DmCatchupService(_FakePool(_FakeRawClient()), db)

        await service.run_for_phone("+111")

        assert await db.repos.incoming_dms.count_unprocessed() == 0  # порог диалога вытеснил
    finally:
        await db.close()


async def test_settings_reread_every_pass_no_restart(tmp_path, monkeypatch):
    """Смена режима в БД между проходами меняет поведение без рестарта."""
    db = await _make_db(tmp_path)
    try:
        stub = _stub_history_since({42: [_FakeMessage(10)]})
        monkeypatch.setattr(dm_catchup, "read_dialog_history_since", stub)
        service = DmCatchupService(_FakePool(_FakeRawClient()), db)

        await service.run_for_phone("+111")  # дефолт journal_only
        assert await db.repos.incoming_dms.count_unprocessed() == 0

        await db.set_setting(
            DM_CATCHUP_SETTING_KEY, DmCatchupSettings(mode="full").model_dump_json()
        )
        fresh_stub = _stub_history_since({42: [_FakeMessage(11)]})
        monkeypatch.setattr(dm_catchup, "read_dialog_history_since", fresh_stub)
        await service.run_for_phone("+111")

        assert await db.repos.incoming_dms.count_unprocessed() == 1
    finally:
        await db.close()


# --- лимиты: гейт и флуд ---


class _PacedGate(TelegramRateLimitGate):
    """Гейт, отказывающий первые N обращений заданным retry_after.

    Подкласс: сервис (как и backends) гардится isinstance-ом
    TelegramRateLimitGate, фейк без наследования молча выключил бы гейтинг.
    """

    def __init__(self, refusals: int, retry_after: float = 0.01):
        super().__init__(time_func=lambda: 0.0)
        self.refusals = refusals
        self.retry_after = retry_after
        self.calls = 0

    def try_acquire(self, phone: str, category: str, **kwargs) -> float:
        self.calls += 1
        if self.refusals > 0:
            self.refusals -= 1
            return self.retry_after
        return 0.0


async def test_gate_wait_then_pass(tmp_path, monkeypatch):
    """Гейт отказал один раз → догон дождался и дочитал (ждать, а не отказывать)."""
    db = await _make_db(tmp_path)
    try:
        client = _FakeRawClient()
        stub = _stub_history_since({42: [_FakeMessage(10)]})
        monkeypatch.setattr(dm_catchup, "read_dialog_history_since", stub)
        pool = _FakePool(client)
        gate = _PacedGate(refusals=1)
        pool._rate_limit_gate = gate
        service = DmCatchupService(pool, db)

        stats = await service.run_for_phone("+111")

        assert stats["stored"] == 1
        # чат 42: отказ + резерв после ожидания; чат 43: резерв
        assert gate.calls == 3
    finally:
        await db.close()


async def test_gate_saturation_ends_pass_leaving_watermark(tmp_path, monkeypatch):
    """Второй подряд отказ гейта закрывает проход; водяные знаки не тронуты."""
    db = await _make_db(tmp_path)
    try:
        client = _FakeRawClient()
        stub = _stub_history_since({42: [_FakeMessage(10)], 43: [_FakeMessage(20)]})
        monkeypatch.setattr(dm_catchup, "read_dialog_history_since", stub)
        pool = _FakePool(client)
        pool._rate_limit_gate = _PacedGate(refusals=10)  # всегда отказ
        service = DmCatchupService(pool, db)

        stats = await service.run_for_phone("+111")

        assert stats["stored"] == 0
        assert stats["deferred"] == 2  # оба диалога отложены, не потеряны
        assert client.history_calls == []
    finally:
        await db.close()


async def test_flood_wait_defers_dialog_not_pass(tmp_path, monkeypatch):
    """Нетранзиентный флуд на одном диалоге не срывает догон остальных."""
    db = await _make_db(tmp_path)
    try:
        client = _FakeRawClient()
        downstream = _stub_history_since({43: [_FakeMessage(20)]})

        async def _flood_then_ok(cl, *, api_id, api_hash, peer, min_id=0, limit=200):
            if peer.user_id == 42:
                raise HandledFloodWaitError(_flood_info(seconds=50000))
            return await downstream(
                cl, api_id=api_id, api_hash=api_hash, peer=peer, min_id=min_id, limit=limit
            )

        monkeypatch.setattr(dm_catchup, "read_dialog_history_since", _flood_then_ok)
        service = DmCatchupService(_FakePool(client), db)

        stats = await service.run_for_phone("+111")

        assert stats["errors"] == 1
        assert stats["stored"] == 1  # диалог 43 догнан
    finally:
        await db.close()


# --- планировщик / жизненный цикл ---


async def test_schedule_is_single_flight_and_drains_pending(tmp_path, monkeypatch):
    """Повторный schedule во время прохода не плодит задачи, но доносит телефоны."""
    db = await _make_db(tmp_path)
    try:
        stub = _stub_history_since({42: [_FakeMessage(10)]})
        monkeypatch.setattr(dm_catchup, "read_dialog_history_since", stub)
        service = DmCatchupService(_FakePool(_FakeRawClient()), db)

        service.schedule(["+111"])
        service.schedule(["+111", "+222"])  # пока первый проход ещё идёт
        await asyncio.wait_for(service._task, timeout=2)

        assert service.status()["running"] is False
        assert "+111" in service.status()["last_runs"]
        assert service._pending == set()  # очередь высосана до конца
    finally:
        await db.close()


async def test_stop_cancels_running_pass(tmp_path, monkeypatch):
    db = await _make_db(tmp_path)
    try:
        service = DmCatchupService(_FakePool(_FakeRawClient()), db)

        async def _hang(phone):
            await asyncio.sleep(60)

        monkeypatch.setattr(service, "run_for_phone", _hang)
        service.schedule(["+111"])
        await asyncio.sleep(0.01)
        assert service.status()["running"] is True

        await asyncio.wait_for(service.stop(), timeout=2)

        assert service.status()["running"] is False
    finally:
        await db.close()


# --- загрузка и приоритет настроек ---


async def test_load_settings_defaults_and_corrupt_fallback(tmp_path):
    db = await _make_db(tmp_path)
    try:
        assert await load_dm_catchup_settings(db) == DmCatchupSettings()

        await db.set_setting(
            DM_CATCHUP_SETTING_KEY,
            json.dumps({"mode": "full", "staleness_sec": 60}),
        )
        settings = await load_dm_catchup_settings(db)
        assert (settings.mode, settings.staleness_sec) == ("full", 60)

        await db.set_setting(DM_CATCHUP_SETTING_KEY, "{битый json")
        assert await load_dm_catchup_settings(db) == DmCatchupSettings()
    finally:
        await db.close()


async def test_resolve_priority_chain():
    settings = DmCatchupSettings(
        mode="ignore",
        staleness_sec=100,
        accounts={"+111": DmCatchupOverride(mode="full")},
        dialogs={
            "+111:42": DmCatchupOverride(mode="journal_only", staleness_sec=7),
            "+111:43": DmCatchupOverride(staleness_sec=9),
        },
    )
    assert settings.resolve("+222", 1) == ("ignore", 100)  # глобаль
    assert settings.resolve("+111", 1) == ("full", 100)  # аккаунт побеждает глобаль
    assert settings.resolve("+111", 42) == ("journal_only", 7)  # диалог побеждает аккаунт
    assert settings.resolve("+111", 43) == ("full", 9)  # None-режим диалога наследует аккаунт
