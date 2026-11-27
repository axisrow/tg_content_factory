"""Tests for the voice-transcription module (#1545).

Fake-first: engines are fakes/monkeypatched module hooks, the cache runs on the
real in-memory project database. Live provider calls live in
tests/cli_real_provider_integration (real_provider_smoke-gated).
"""

from __future__ import annotations

import asyncio
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from telethon.tl.types import PeerChannel

from tg_agent.database import Database
from tg_agent.telegram import transcription
from tg_agent.telegram.transcription import (
    ENGINES,
    TranscribeBudget,
    _download_audio,
    _transcribe_lock,
    _upload_name,
    engine_config_error,
    format_duration,
    get_cached_transcript,
    is_transcribable,
    marked_chat_id,
    save_transcript,
    transcribe_cached,
    voice_duration,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _voice_msg(message_id: int = 42, duration: int = 23, size: int | None = 100):
    file_attrs = {"duration": duration, "mime_type": "audio/ogg"}
    if size is not None:
        file_attrs["size"] = size
    return SimpleNamespace(
        id=message_id,
        voice=SimpleNamespace(),
        file=SimpleNamespace(ext=".oga", **file_attrs),
    )


def _no_stt_env(monkeypatch):
    """Strip every engine credential so tests see the unconfigured default."""
    for var in (
        "GROQ_API_KEY",
        "TELEGRAM_TRANSCRIBE_OPENAI_URL",
        "TELEGRAM_TRANSCRIBE_OPENAI_API_KEY",
        "TELEGRAM_TRANSCRIBE_ENGINE",
        "TELEGRAM_TRANSCRIBE_LANGUAGE",
    ):
        monkeypatch.delenv(var, raising=False)


@pytest.fixture(autouse=True)
def _reset_module_state():
    transcription._INFLIGHT_LOCKS.clear()
    transcription._WHISPER_MODELS.clear()
    transcription._WHISPER_SEMAPHORE = None
    yield
    transcription._INFLIGHT_LOCKS.clear()
    transcription._WHISPER_MODELS.clear()
    transcription._WHISPER_SEMAPHORE = None


# ---------------------------------------------------------------------------
# Cache: schema shape, roundtrip, engine pinning
# ---------------------------------------------------------------------------


async def test_transcripts_table_keys_on_chat_message_source(db: Database):
    """The cache key includes the engine (#1545): no cross-engine shadowing."""
    cur = await db.execute(
        "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'transcripts'"
    )
    row = await cur.fetchone()
    assert row is not None
    assert "PRIMARY KEY (chat_id, message_id, source)" in row[0]


async def test_save_and_get_cached_transcript_roundtrip(db: Database):
    await save_transcript(db, -100123, 42, "groq", "привет мир", duration=23, lang="ru")
    hit = await get_cached_transcript(db, -100123, 42, source="groq")
    assert hit is not None
    assert hit == {
        "source": "groq",
        "text": "привет мир",
        "duration": 23,
        "lang": "ru",
        "created_at": hit["created_at"],
    }
    assert hit["created_at"]


async def test_get_cached_transcript_without_source_prefers_default_engine(
    db: Database, monkeypatch
):
    _no_stt_env(monkeypatch)
    await save_transcript(db, -100123, 42, "telegram", "с хвостом")
    await save_transcript(db, -100123, 42, "groq", "полный текст")
    hit = await get_cached_transcript(db, -100123, 42)
    # default engine is groq (upstream owner decision) → its row wins.
    assert hit is not None and hit["source"] == "groq"


async def test_get_cached_transcript_source_pin_never_crosses_engines(db: Database):
    await save_transcript(db, -100123, 42, "groq", "полный текст")
    assert await get_cached_transcript(db, -100123, 42, source="telegram") is None


# ---------------------------------------------------------------------------
# transcribe_cached: cache-first, one paid call per recording
# ---------------------------------------------------------------------------


async def test_transcribe_cached_cache_hit_never_touches_client_or_engine(
    db: Database, monkeypatch
):
    _no_stt_env(monkeypatch)
    await save_transcript(db, -100123, 42, "groq", "из кэша", duration=23)

    async def fail_download(*a, **kw):
        raise AssertionError("cache hit must not download media")

    client = SimpleNamespace(download_media=fail_download)
    engine_calls = AsyncMock()
    monkeypatch.setattr(transcription, "transcribe", engine_calls)

    msg = _voice_msg()
    result = await transcribe_cached(client, None, msg, "groq", db, -100123)
    assert result == {
        "status": "ok",
        "cached": True,
        "text": "из кэша",
        "lang": None,
        "duration": 23,
        "source": "groq",
    }
    engine_calls.assert_not_awaited()


async def test_transcribe_cached_concurrent_calls_pay_once(db: Database, monkeypatch):
    _no_stt_env(monkeypatch)
    calls = 0

    async def slow_engine(cl, entity, msg, engine):
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.05)
        return {"status": "ok", "text": f"текст {calls}", "lang": "ru"}

    monkeypatch.setattr(transcription, "transcribe", slow_engine)
    msg = _voice_msg()

    first, second = await asyncio.gather(
        transcribe_cached(None, None, msg, "groq", db, -100123),
        transcribe_cached(None, None, msg, "groq", db, -100123),
    )
    assert calls == 1
    assert first["cached"] is False
    assert first["text"] == "текст 1"
    assert second["cached"] is True
    assert second["text"] == "текст 1"
    hit = await get_cached_transcript(db, -100123, 42, source="groq")
    assert hit is not None and hit["text"] == "текст 1"


async def test_transcribe_cached_failed_engine_is_not_cached(db: Database, monkeypatch):
    _no_stt_env(monkeypatch)

    async def failing_engine(cl, entity, msg, engine):
        return {"status": "error", "error": "boom"}

    monkeypatch.setattr(transcription, "transcribe", failing_engine)
    result = await transcribe_cached(None, None, _voice_msg(), "groq", db, -100123)
    assert result["status"] == "error"
    assert await get_cached_transcript(db, -100123, 42, source="groq") is None


async def test_transcribe_cached_without_message_id_is_rejected(db: Database, monkeypatch):
    _no_stt_env(monkeypatch)
    engine_calls = AsyncMock()
    monkeypatch.setattr(transcription, "transcribe", engine_calls)
    msg = SimpleNamespace(id=None, voice=SimpleNamespace())
    result = await transcribe_cached(None, None, msg, "groq", db, -100123)
    assert result["status"] == "error"
    assert "no id" in result["error"]
    engine_calls.assert_not_awaited()


async def test_transcribe_lock_registry_cleans_up():
    async with _transcribe_lock(("chat", 1, "groq")):
        assert ("chat", 1, "groq") in transcription._INFLIGHT_LOCKS
    assert ("chat", 1, "groq") not in transcription._INFLIGHT_LOCKS


# ---------------------------------------------------------------------------
# TranscribeBudget
# ---------------------------------------------------------------------------


def test_budget_caps_voice_count():
    budget = TranscribeBudget(max_voices=2, max_seconds=10_000)
    assert budget.can_afford(100)
    budget.charge(100)
    assert budget.can_afford(100)
    budget.charge(100)
    assert not budget.can_afford(1)
    assert budget.used_voices == 2


def test_budget_caps_total_seconds():
    budget = TranscribeBudget(max_voices=100, max_seconds=100)
    assert budget.can_afford(60)
    budget.charge(60)
    assert not budget.can_afford(60)  # 60 + 60 > 100
    assert budget.can_afford(40)
    assert budget.used_seconds == 60


def test_budget_treats_unknown_duration_as_zero_seconds():
    budget = TranscribeBudget(max_voices=1, max_seconds=0)
    assert budget.can_afford(None)
    budget.charge(None)
    assert budget.used_seconds == 0
    assert not budget.can_afford(None)  # voice cap, not seconds


def test_budget_defaults_come_from_env(monkeypatch):
    monkeypatch.setenv("TELEGRAM_TRANSCRIBE_MAX_VOICES", "7")
    monkeypatch.setenv("TELEGRAM_TRANSCRIBE_MAX_SECONDS", "999")
    budget = TranscribeBudget()
    assert (budget.max_voices, budget.max_seconds) == (7, 999)
    monkeypatch.setenv("TELEGRAM_TRANSCRIBE_MAX_VOICES", "crap")
    assert TranscribeBudget().max_voices == 5


# ---------------------------------------------------------------------------
# Engine configuration errors (clear message, no traceback, nothing downloaded)
# ---------------------------------------------------------------------------


def test_groq_without_key_reports_setting(monkeypatch):
    _no_stt_env(monkeypatch)
    message = engine_config_error("groq")
    assert message is not None and "GROQ_API_KEY" in message


def test_openai_without_url_reports_setting(monkeypatch):
    _no_stt_env(monkeypatch)
    message = engine_config_error("openai")
    assert message is not None and "TELEGRAM_TRANSCRIBE_OPENAI_URL" in message


def test_whisper_without_package_reports_install_hint(monkeypatch):
    _no_stt_env(monkeypatch)
    monkeypatch.setattr(transcription, "whisper_available", lambda: False)
    message = engine_config_error("whisper")
    assert message is not None and "pip install faster-whisper" in message


def test_unknown_engine_not_in_engines():
    assert "whisper" in ENGINES and "telegram" in ENGINES
    assert engine_config_error("telegram") is None  # premium is a runtime check


async def test_whisper_engine_stays_lazy_when_not_selected(monkeypatch):
    """Engine selection must not import or load the local model (issue #1545)."""
    _no_stt_env(monkeypatch)

    def fail_load(settings):
        raise AssertionError("whisper model must be loaded only for engine='whisper'")

    monkeypatch.setattr(transcription, "_load_whisper_model", fail_load)
    result = await transcription.transcribe(None, None, _voice_msg(), "groq")
    # No GROQ_API_KEY → clear error, and the whisper loader was never touched.
    assert result["status"] == "error" and "GROQ_API_KEY" in result["error"]


def test_whisper_model_cache_is_keyed_by_settings(monkeypatch):
    monkeypatch.setattr(transcription, "whisper_available", lambda: True)

    class FakeModel:
        def __init__(self, name, device, compute_type, download_root):
            self.name = name

    fake_module = SimpleNamespace(WhisperModel=FakeModel)
    monkeypatch.setitem(sys.modules, "faster_whisper", fake_module)
    monkeypatch.setattr(transcription, "_WHISPER_MODELS", {})
    first = transcription._load_whisper_model(("small", "auto", "default", None))
    second = transcription._load_whisper_model(("small", "auto", "default", None))
    assert first is second
    assert transcription._WHISPER_MODELS[("small", "auto", "default", None)].name == "small"


# ---------------------------------------------------------------------------
# telegram engine: premium gating and polling
# ---------------------------------------------------------------------------


async def test_telegram_engine_requires_premium(monkeypatch):
    client = SimpleNamespace(get_me=AsyncMock(return_value=SimpleNamespace(premium=False)))
    result = await transcription._transcribe_via_telegram(client, None, _voice_msg())
    assert result == {"status": "premium_required"}


async def test_telegram_engine_transcribes_when_premium(monkeypatch):
    client = SimpleNamespace(
        get_me=AsyncMock(return_value=SimpleNamespace(premium=True)),
        get_input_entity=AsyncMock(return_value=PeerChannel(555)),
    )
    raw = AsyncMock(return_value=SimpleNamespace(pending=False, text="готово"))
    monkeypatch.setattr(transcription, "transcribe_audio_raw", raw)
    result = await transcription._transcribe_via_telegram(client, "@ch", _voice_msg())
    assert result == {"status": "ok", "text": "готово"}
    raw.assert_awaited_once()


async def test_telegram_engine_polls_while_pending(monkeypatch):
    client = SimpleNamespace(
        get_me=AsyncMock(return_value=SimpleNamespace(premium=True)),
        get_input_entity=AsyncMock(return_value=PeerChannel(555)),
    )
    attempts = {"n": 0}

    async def fake_raw(cl, peer, message_id):
        attempts["n"] += 1
        if attempts["n"] < 3:
            return SimpleNamespace(pending=True, text="")
        return SimpleNamespace(pending=False, text="дозрело")

    monkeypatch.setattr(transcription, "transcribe_audio_raw", fake_raw)
    monkeypatch.setattr(transcription.asyncio, "sleep", AsyncMock())
    result = await transcription._transcribe_via_telegram(client, "@ch", _voice_msg())
    assert result == {"status": "ok", "text": "дозрело"}
    assert attempts["n"] == 3


async def test_telegram_engine_premium_rpc_error_is_actionable(monkeypatch):
    class PremiumRpcError(Exception):
        message = "PREMIUM_ACCOUNT_REQUIRED"

    client = SimpleNamespace(
        get_me=AsyncMock(return_value=SimpleNamespace(premium=True)),
        get_input_entity=AsyncMock(return_value=PeerChannel(555)),
    )

    async def premium_required_rpc(cl, peer, message_id):
        raise PremiumRpcError(PremiumRpcError.message)

    monkeypatch.setattr(transcription, "transcribe_audio_raw", premium_required_rpc)
    result = await transcription._transcribe_via_telegram(client, "@ch", _voice_msg())
    assert result == {"status": "premium_required"}


async def test_telegram_engine_error_names_a_message_less_exception(monkeypatch):
    client = SimpleNamespace(
        get_me=AsyncMock(return_value=SimpleNamespace(premium=True)),
        get_input_entity=AsyncMock(return_value=PeerChannel(555)),
    )
    monkeypatch.setattr(transcription, "transcribe_audio_raw", AsyncMock(side_effect=RuntimeError()))
    result = await transcription._transcribe_via_telegram(client, "@ch", _voice_msg())
    assert result == {"status": "error", "error": "RuntimeError"}


def test_is_premium_rpc_error_matches_message():
    class PremiumRpcError(Exception):
        message = "A PREMIUM subscription is required"

    assert not transcription.is_premium_rpc_error(Exception("plain failure"))
    assert transcription.is_premium_rpc_error(PremiumRpcError("..."))


# ---------------------------------------------------------------------------
# Download guard and upload naming
# ---------------------------------------------------------------------------


async def test_download_audio_refuses_oversized_before_downloading():
    downloaded = AsyncMock()
    msg = _voice_msg(size=30 * 1048576)
    cfg = transcription.http_engine_config("groq")
    assert cfg is not None and cfg.max_bytes is not None
    data, error = await _download_audio(SimpleNamespace(download_media=downloaded), msg, cfg.max_bytes, cfg)
    assert error is not None and data is None
    assert error["reason"] == "too_large"
    assert "TELEGRAM_TRANSCRIBE_GROQ_MAX_MB" in error["error"]
    downloaded.assert_not_awaited()


async def test_download_audio_refuses_oversized_after_download():
    class BigDownload:
        async def download_media(self, msg, file=bytes):
            return b"x" * (30 * 1048576)

    cfg = transcription.http_engine_config("groq")
    assert cfg is not None and cfg.max_bytes is not None
    data, error = await _download_audio(BigDownload(), _voice_msg(size=None), cfg.max_bytes, cfg)
    assert error is not None and data is None
    assert error["reason"] == "too_large"


async def test_download_audio_reports_empty_media():
    client = SimpleNamespace(download_media=AsyncMock(return_value=None))
    data, error = await _download_audio(client, _voice_msg(), None)
    assert data is None and error is not None
    assert error["error"] == "empty media download"


def test_upload_name_renames_oga_to_ogg():
    assert _upload_name(_voice_msg()) == ("voice.ogg", "audio/ogg")
    doc = SimpleNamespace(file=SimpleNamespace(ext=".mp4", mime_type="video/mp4"))
    assert _upload_name(doc) == ("voice.mp4", "video/mp4")


def test_openai_endpoint_accepts_base_and_full_url():
    assert transcription._endpoint("https://api.openai.com/v1") == "https://api.openai.com/v1/audio/transcriptions"
    assert transcription._endpoint("https://host/v1/audio/transcriptions/") == "https://host/v1/audio/transcriptions"


# ---------------------------------------------------------------------------
# Voice helpers
# ---------------------------------------------------------------------------


def test_is_transcribable_covers_voice_and_video_note():
    assert is_transcribable(_voice_msg())
    note = SimpleNamespace(video_note=SimpleNamespace(), file=SimpleNamespace(duration=5))
    assert is_transcribable(note)
    assert not is_transcribable(SimpleNamespace(text="привет"))


def test_voice_duration_and_formatting():
    assert voice_duration(_voice_msg(duration=83)) == 83
    assert voice_duration(SimpleNamespace()) is None
    assert format_duration(83) == "1:23"
    assert format_duration(3723) == "1:02:03"
    assert format_duration(None) == "?:??"


def test_marked_chat_id_uses_telethon_convention():
    assert marked_chat_id(PeerChannel(555)) == -1000000000000 - 555


# ---------------------------------------------------------------------------
# Service orchestration (TelegramActionService.transcribe_voice)
# ---------------------------------------------------------------------------


async def _fetch_via_iter(client, entity, message):
    async def fake_iter(*a, **kw):
        yield message

    client.iter_messages = fake_iter
    return message


def _transcribe_pool(client):
    pool = AsyncMock()
    pool.clients = {"+1111": AsyncMock()}
    pool.get_native_client_by_phone = AsyncMock(return_value=(client, "+1111"))
    pool.release_client = AsyncMock()
    pool.disconnect_all = AsyncMock()
    return pool


async def test_service_transcribe_voice_ok_and_cached(db: Database, monkeypatch):
    from tg_agent.services.telegram_actions import TelegramActionService

    _no_stt_env(monkeypatch)
    monkeypatch.setenv("GROQ_API_KEY", "test-key")
    message = _voice_msg()
    client = AsyncMock()
    client.get_entity = AsyncMock(return_value=PeerChannel(555))
    client.get_input_entity = AsyncMock(return_value=PeerChannel(555))
    await _fetch_via_iter(client, None, message)

    async def fake_engine(cl, entity, msg, engine):
        return {"status": "ok", "text": "текст голосового", "lang": "ru"}

    monkeypatch.setattr(transcription, "transcribe", fake_engine)
    service = TelegramActionService(_transcribe_pool(client))

    first = await service.transcribe_voice(
        phone="+1111", chat_id="@ch", message_id=42, engine="groq", db=db
    )
    assert first["status"] == "ok" and first["cached"] is False
    assert first["text"] == "текст голосового"
    assert first["source"] == "groq"

    # Repeat call: from the cache — the engine is not invoked again.
    engine_calls = AsyncMock()
    monkeypatch.setattr(transcription, "transcribe", engine_calls)
    second = await service.transcribe_voice(
        phone="+1111", chat_id="@ch", message_id=42, engine="groq", db=db
    )
    assert second["cached"] is True and second["text"] == "текст голосового"
    engine_calls.assert_not_awaited()


async def test_service_transcribe_voice_rejects_unknown_engine_and_misconfig(
    db: Database, monkeypatch
):
    from tg_agent.services.telegram_actions import TelegramActionService

    _no_stt_env(monkeypatch)
    pool = _transcribe_pool(AsyncMock())
    service = TelegramActionService(pool)

    unknown = await service.transcribe_voice(
        phone="+1111", chat_id="@ch", message_id=42, engine="wat", db=db
    )
    assert unknown["status"] == "error" and "Unknown engine" in unknown["error"]

    missing_key = await service.transcribe_voice(
        phone="+1111", chat_id="@ch", message_id=42, engine="groq", db=db
    )
    assert missing_key["status"] == "error" and "GROQ_API_KEY" in missing_key["error"]
    # Neither call may touch the Telegram client.
    pool.get_native_client_by_phone.assert_not_awaited()


async def test_service_transcribe_voice_rejects_non_voice_message(db: Database, monkeypatch):
    from tg_agent.services.telegram_actions import TelegramActionService

    _no_stt_env(monkeypatch)
    client = AsyncMock()
    client.get_entity = AsyncMock(return_value=PeerChannel(555))
    client.get_input_entity = AsyncMock(return_value=PeerChannel(555))
    await _fetch_via_iter(client, None, SimpleNamespace(id=42, text="просто текст"))
    service = TelegramActionService(_transcribe_pool(client))
    result = await service.transcribe_voice(
        phone="+1111", chat_id="@ch", message_id=42, engine="telegram", db=db
    )
    assert result["status"] == "error"
    assert "голосов" in result["error"]


async def test_service_transcribe_voice_runs_under_flood_wait_wrapper(
    db: Database, monkeypatch
):
    """The engine call is wrapped in run_with_flood_wait: a native-RPC flood is
    reported to the pool and re-raised as HandledFloodWaitError (review #1561)."""
    import tg_agent.services.telegram_actions as ta

    _no_stt_env(monkeypatch)
    monkeypatch.setenv("GROQ_API_KEY", "test-key")
    message = _voice_msg()
    client = AsyncMock()
    client.get_entity = AsyncMock(return_value=PeerChannel(555))
    client.get_input_entity = AsyncMock(return_value=PeerChannel(555))
    await _fetch_via_iter(client, None, message)

    async def fake_engine(cl, entity, msg, engine):
        return {"status": "ok", "text": "текст", "lang": "ru"}

    monkeypatch.setattr(transcription, "transcribe", fake_engine)

    wrapped_kwargs: dict = {}

    async def fake_flood_wait(coro, **kwargs):
        wrapped_kwargs.update(kwargs)
        return await coro

    monkeypatch.setattr(ta, "run_with_flood_wait", fake_flood_wait)
    pool = _transcribe_pool(client)
    service = ta.TelegramActionService(pool)

    result = await service.transcribe_voice(
        phone="+1111", chat_id="@ch", message_id=42, engine="groq", db=db
    )
    assert result["status"] == "ok"
    assert wrapped_kwargs["operation"] == "telegram_action_transcribe"
    assert wrapped_kwargs["phone"] == "+1111"
    assert wrapped_kwargs["pool"] is pool


async def test_service_transcribe_voice_telegram_picks_premium_lease(db: Database, monkeypatch):
    from tg_agent.services.telegram_actions import TelegramActionService

    _no_stt_env(monkeypatch)
    message = _voice_msg()
    client = AsyncMock()
    client.get_entity = AsyncMock(return_value=PeerChannel(555))
    client.get_input_entity = AsyncMock(return_value=PeerChannel(555))
    await _fetch_via_iter(client, None, message)
    client.get_me = AsyncMock(return_value=SimpleNamespace(premium=True))
    client.get_input_entity = AsyncMock(return_value=PeerChannel(555))
    raw = AsyncMock(return_value=SimpleNamespace(pending=False, text="нативно"))
    monkeypatch.setattr(transcription, "transcribe_audio_raw", raw)

    pool = _transcribe_pool(client)
    pool.get_premium_client = AsyncMock(return_value=(AsyncMock(), "+7777"))
    pool.get_premium_unavailability_reason = AsyncMock(return_value="недоступен")
    service = TelegramActionService(pool)

    result = await service.transcribe_voice(
        phone=None, chat_id="@ch", message_id=42, engine="telegram", db=db
    )
    assert result["status"] == "ok" and result["text"] == "нативно"
    pool.get_premium_client.assert_awaited_once()
    # Released once after the lease pick and once by the _client context manager.
    pool.release_client.assert_any_call("+7777")
    # The transcription ran on the leased premium account.
    pool.get_native_client_by_phone.assert_awaited_once_with("+7777", wait_for_flood=True)


async def test_service_transcribe_voice_telegram_without_premium_account(db: Database, monkeypatch):
    from tg_agent.services.telegram_actions import TelegramActionService

    _no_stt_env(monkeypatch)
    pool = _transcribe_pool(AsyncMock())
    pool.get_premium_client = AsyncMock(return_value=None)
    pool.get_premium_unavailability_reason = AsyncMock(return_value="Нет Premium-аккаунтов.")
    service = TelegramActionService(pool)

    result = await service.transcribe_voice(
        phone=None, chat_id="@ch", message_id=42, engine="telegram", db=db
    )
    assert result["status"] == "premium_required"
    assert result["error"] == "Нет Premium-аккаунтов."
    pool.get_native_client_by_phone.assert_not_awaited()


async def test_service_transcribe_voice_missing_message(db: Database, monkeypatch):
    from tg_agent.services.telegram_actions import (
        TelegramActionMessageNotFoundError,
        TelegramActionService,
    )

    _no_stt_env(monkeypatch)
    monkeypatch.setenv("GROQ_API_KEY", "test-key")
    client = AsyncMock()
    client.get_entity = AsyncMock(return_value=PeerChannel(555))
    client.get_input_entity = AsyncMock(return_value=PeerChannel(555))
    await _fetch_via_iter(client, None, None)  # id no longer exists
    service = TelegramActionService(_transcribe_pool(client))
    with pytest.raises(TelegramActionMessageNotFoundError):
        await service.transcribe_voice(
            phone="+1111", chat_id="@ch", message_id=999, engine="groq", db=db
        )


# ---------------------------------------------------------------------------
# CLI rendering lives in src/cli/commands/dialogs.py (covered in
# test_regression_dialogs_cli.py); here only the shared status-dict contract.
# ---------------------------------------------------------------------------


def test_status_dict_contract_is_stable():
    """The CLI renders these keys; renames are a breaking CLI contract change."""
    row = {"source": "groq", "text": "t", "lang": "ru", "duration": 5}
    out = transcription._cached_result(row)
    assert {"status", "cached", "text", "lang", "duration", "source"} <= set(out)
