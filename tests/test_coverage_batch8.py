"""Coverage batch 8 — targeted tests for remaining gaps in:
- cli/commands/test.py (benchmark, telegram checks, format helpers)
- telegram/client_pool.py (cache, flood, disconnect, premium)
- agent/manager.py (DeepagentsBackend, ClaudeSdkBackend, AgentManager)
- telegram/collector.py (media types, cancellation, auto-delete, precheck)
- services/agent_provider_service.py (refresh, save, export, form parsing)
- agent/tools/deepagents_sync.py (remaining sync tools)
- scheduler/manager.py (jobs, sync, pipeline jobs)
- services/unified_dispatcher.py (dispatch, handlers)
"""

from __future__ import annotations

import asyncio
import time
from datetime import UTC, datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.config import AppConfig, SchedulerConfig
from src.database import Database
from src.models import (
    CollectionTask,
    CollectionTaskStatus,
    CollectionTaskType,
    ContentGenerateTaskPayload,
    PipelineRunTaskPayload,
    SqStatsTaskPayload,
    StatsAllTaskPayload,
)
from src.telegram.collector import Collector

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_db():
    db = MagicMock(spec=Database)
    db.repos = MagicMock()
    return db


def _make_config(**overrides):
    return AppConfig(**overrides)


# ===========================================================================
# 1. cli/commands/test.py — remaining branches
# ===========================================================================


class TestFormatException:
    def test_with_message(self):
        from src.cli.commands.test import _format_exception

        exc = RuntimeError("something broke")
        assert _format_exception(exc) == "something broke"

    def test_empty_message(self):
        from src.cli.commands.test import _format_exception

        exc = RuntimeError("")
        assert _format_exception(exc) == "RuntimeError"

    def test_whitespace_only(self):
        from src.cli.commands.test import _format_exception

        exc = RuntimeError("   ")
        assert _format_exception(exc) == "RuntimeError"


class TestFormatAllFloodedDetail:
    def test_no_retry_no_datetime(self):
        from src.cli.commands.test import _format_all_flooded_detail

        result = _format_all_flooded_detail(
            "base", retry_after_sec=None, next_available_at_utc=None
        )
        assert "all clients are flood-waited" in result

    def test_retry_sec_only(self):
        from src.cli.commands.test import _format_all_flooded_detail

        result = _format_all_flooded_detail(
            "base", retry_after_sec=10, next_available_at_utc=None
        )
        assert "retry after about 10s" in result
        assert "until" not in result

    def test_retry_sec_with_utc(self):
        from src.cli.commands.test import _format_all_flooded_detail

        dt = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        result = _format_all_flooded_detail(
            "base", retry_after_sec=10, next_available_at_utc=dt
        )
        assert "retry after about 10s" in result
        assert "until" in result


class TestIsPremiumFlood:
    def test_premium_operations(self):
        from src.cli.commands.test import _is_premium_flood
        from src.telegram.flood_wait import FloodWaitInfo

        for op in ("check_search_quota", "search_telegram_check_quota", "search_telegram"):
            info = FloodWaitInfo(
                operation=op,
                wait_seconds=10,
                next_available_at_utc=datetime.now(timezone.utc),
                detail="test",
            )
            assert _is_premium_flood(info) is True

    def test_non_premium_operation(self):
        from src.cli.commands.test import _is_premium_flood
        from src.telegram.flood_wait import FloodWaitInfo

        info = FloodWaitInfo(
            operation="get_entity",
            wait_seconds=10,
            next_available_at_utc=datetime.now(timezone.utc),
            detail="test",
        )
        assert _is_premium_flood(info) is False


class TestGetSearchResultFloodWait:
    def test_returns_flood_info(self):
        from src.cli.commands.test import _get_search_result_flood_wait
        from src.telegram.flood_wait import FloodWaitInfo

        info = FloodWaitInfo(
            operation="search",
            wait_seconds=10,
            next_available_at_utc=datetime.now(timezone.utc),
            detail="test",
        )
        result = SimpleNamespace(flood_wait=info)
        assert _get_search_result_flood_wait(result) is info

    def test_returns_none_not_flood_info(self):
        from src.cli.commands.test import _get_search_result_flood_wait

        result = SimpleNamespace(flood_wait="not a flood info")
        assert _get_search_result_flood_wait(result) is None

    def test_returns_none_no_attr(self):
        from src.cli.commands.test import _get_search_result_flood_wait

        assert _get_search_result_flood_wait(SimpleNamespace()) is None


class TestRunBenchmarkStep:
    def test_success(self):
        import sys

        from src.cli.commands.test import BenchmarkStep, _run_benchmark_step

        step = BenchmarkStep("test", (sys.executable, "-c", "pass"))
        elapsed = _run_benchmark_step(step)
        assert elapsed >= 0

    def test_failure_exits(self):
        import sys

        from src.cli.commands.test import BenchmarkStep, _run_benchmark_step

        step = BenchmarkStep("fail", (sys.executable, "-c", "import sys; sys.exit(1)"))
        with pytest.raises(SystemExit):
            _run_benchmark_step(step)


class TestPrintResult:
    def test_pass(self, capsys):
        from src.cli.commands.test import CheckResult, Status, _print_result

        _print_result(CheckResult("test", Status.PASS, "ok"))
        out = capsys.readouterr().out
        assert "PASS" in out
        assert "test" in out

    def test_fail(self, capsys):
        from src.cli.commands.test import CheckResult, Status, _print_result

        _print_result(CheckResult("test", Status.FAIL, "bad"))
        out = capsys.readouterr().out
        assert "FAIL" in out

    def test_skip(self, capsys):
        from src.cli.commands.test import CheckResult, Status, _print_result

        _print_result(CheckResult("test", Status.SKIP, "skipped"))
        out = capsys.readouterr().out
        assert "SKIP" in out


class TestDecideLiveFloodAction:
    @pytest.mark.asyncio
    async def test_no_availability_returns_skip(self):
        from src.cli.commands.test import _decide_live_test_flood_action
        from src.telegram.flood_wait import FloodWaitInfo

        pool = MagicMock()
        pool.get_stats_availability = None
        pool.get_premium_stats_availability = None
        info = FloodWaitInfo(
            operation="test",
            wait_seconds=10,
            next_available_at_utc=datetime.now(timezone.utc),
            detail="detail",
        )
        decision = await _decide_live_test_flood_action(pool, info)
        assert decision.action == "skip"

    @pytest.mark.asyncio
    async def test_not_flooded_returns_rotate(self):
        from src.cli.commands.test import _decide_live_test_flood_action
        from src.telegram.client_pool import StatsClientAvailability
        from src.telegram.flood_wait import FloodWaitInfo

        pool = MagicMock()
        pool.get_stats_availability = AsyncMock(
            return_value=StatsClientAvailability(state="available")
        )
        info = FloodWaitInfo(
            operation="test",
            wait_seconds=10,
            next_available_at_utc=datetime.now(timezone.utc),
            detail="detail",
        )
        decision = await _decide_live_test_flood_action(pool, info)
        assert decision.action == "rotate"


class TestTgCallWrapper:
    @pytest.mark.asyncio
    async def test_timeout_raises(self):
        from src.cli.commands.test import _tg_call

        async def _hang():
            await asyncio.sleep(100)

        with pytest.raises(TimeoutError, match="Timed out"):
            await _tg_call(_hang(), timeout=0)


class TestIsRegularSearchUnavailable:
    def test_match(self):
        from src.cli.commands.test import _is_regular_search_client_unavailable_error

        assert _is_regular_search_client_unavailable_error(
            "Нет доступных Telegram-аккаунтов. Проверьте подключение."
        )

    def test_no_match(self):
        from src.cli.commands.test import _is_regular_search_client_unavailable_error

        assert not _is_regular_search_client_unavailable_error("something else")


class TestIsPremiumFloodUnavailable:
    def test_match(self):
        from src.cli.commands.test import _is_premium_flood_unavailable_error

        assert _is_premium_flood_unavailable_error(
            "Premium-аккаунты временно недоступны из-за Flood Wait."
        )

    def test_no_match(self):
        from src.cli.commands.test import _is_premium_flood_unavailable_error

        assert not _is_premium_flood_unavailable_error("other")


class TestTelegramLiveStepSkipError:
    def test_is_runtime_error(self):
        from src.cli.commands.test import TelegramLiveStepSkipError

        err = TelegramLiveStepSkipError("detail")
        assert isinstance(err, RuntimeError)
        assert str(err) == "detail"


class TestTelegramLiveFloodDecision:
    def test_defaults(self):
        from src.cli.commands.test import TelegramLiveFloodDecision

        d = TelegramLiveFloodDecision(action="skip", detail="d")
        assert d.retry_after_sec is None
        assert d.next_available_at_utc is None


# ===========================================================================
# 2. telegram/client_pool.py — cache, flood, disconnect, premium
# ===========================================================================


class TestClientPoolDialogsCache:
    @pytest.mark.asyncio
    async def test_invalidate_all(self, db):
        from src.telegram.auth import TelegramAuth
        from src.telegram.client_pool import ClientPool

        pool = ClientPool(TelegramAuth(0, ""), db)
        pool._store_cached_dialogs("+7", "full", [{"channel_id": 1}])
        pool._store_cached_dialogs("+8", "full", [{"channel_id": 2}])
        assert len(pool._dialogs_cache) == 2
        pool.invalidate_dialogs_cache()
        assert len(pool._dialogs_cache) == 0

    @pytest.mark.asyncio
    async def test_invalidate_by_phone(self, db):
        from src.telegram.auth import TelegramAuth
        from src.telegram.client_pool import ClientPool

        pool = ClientPool(TelegramAuth(0, ""), db)
        pool._store_cached_dialogs("+7", "full", [{"channel_id": 1}])
        pool._store_cached_dialogs("+8", "full", [{"channel_id": 2}])
        pool.invalidate_dialogs_cache("+7")
        assert ("+7", "full") not in pool._dialogs_cache
        assert ("+8", "full") in pool._dialogs_cache

    @pytest.mark.asyncio
    async def test_get_cached_expired(self, db):
        from src.telegram.auth import TelegramAuth
        from src.telegram.client_pool import ClientPool

        pool = ClientPool(TelegramAuth(0, ""), db)
        pool._store_cached_dialogs("+7", "full", [{"channel_id": 1}])
        # Backdate the cache entry
        pool._dialogs_cache[("+7", "full")].fetched_at_monotonic = time.monotonic() - 9999
        result = pool._get_cached_dialogs("+7", "full")
        assert result is None

    @pytest.mark.asyncio
    async def test_get_cached_channels_only_from_full(self, db):
        from src.telegram.auth import TelegramAuth
        from src.telegram.client_pool import ClientPool

        pool = ClientPool(TelegramAuth(0, ""), db)
        pool._store_cached_dialogs("+7", "full", [
            {"channel_id": 1, "channel_type": "channel"},
            {"channel_id": 2, "channel_type": "dm"},
        ])
        result = pool._get_cached_dialogs("+7", "channels_only")
        assert len(result) == 1
        assert result[0]["channel_id"] == 1

    @pytest.mark.asyncio
    async def test_get_cached_channels_only_full_expired(self, db):
        from src.telegram.auth import TelegramAuth
        from src.telegram.client_pool import ClientPool

        pool = ClientPool(TelegramAuth(0, ""), db)
        pool._store_cached_dialogs("+7", "full", [{"channel_id": 1}])
        pool._dialogs_cache[("+7", "full")].fetched_at_monotonic = time.monotonic() - 9999
        result = pool._get_cached_dialogs("+7", "channels_only")
        assert result is None


class TestClientPoolPremiumFlood:
    @pytest.mark.asyncio
    async def test_report_premium_flood(self, db):
        from src.telegram.auth import TelegramAuth
        from src.telegram.client_pool import ClientPool

        pool = ClientPool(TelegramAuth(0, ""), db)
        await pool.report_premium_flood("+7", 60)
        assert "+7" in pool._premium_flood_wait_until

    @pytest.mark.asyncio
    async def test_clear_premium_flood(self, db):
        from src.telegram.auth import TelegramAuth
        from src.telegram.client_pool import ClientPool

        pool = ClientPool(TelegramAuth(0, ""), db)
        await pool.report_premium_flood("+7", 60)
        pool.clear_premium_flood("+7")
        assert "+7" not in pool._premium_flood_wait_until

    @pytest.mark.asyncio
    async def test_premium_flood_expires_stale_entries(self, db):
        from src.telegram.auth import TelegramAuth
        from src.telegram.client_pool import ClientPool

        pool = ClientPool(TelegramAuth(0, ""), db)
        # Add an expired entry
        pool._premium_flood_wait_until["+old"] = datetime.now(timezone.utc) - timedelta(seconds=10)
        # Report new flood to trigger cleanup
        await pool.report_premium_flood("+new", 60)
        assert "+old" not in pool._premium_flood_wait_until
        assert "+new" in pool._premium_flood_wait_until


class TestClientPoolStatsAvailability:
    @pytest.mark.asyncio
    async def test_get_stats_availability(self, db):
        from src.telegram.auth import TelegramAuth
        from src.telegram.client_pool import ClientPool

        pool = ClientPool(TelegramAuth(0, ""), db)
        result = await pool.get_stats_availability()
        assert result.state in ("available", "no_connected_active", "all_flooded")

    @pytest.mark.asyncio
    async def test_get_premium_stats_no_premium(self, db):
        from src.telegram.auth import TelegramAuth
        from src.telegram.client_pool import ClientPool

        pool = ClientPool(TelegramAuth(0, ""), db)
        result = await pool.get_premium_stats_availability()
        assert result.state == "no_connected_active"


class TestClientPoolDialogsFetched:
    @pytest.mark.asyncio
    async def test_mark_and_check(self, db):
        from src.telegram.auth import TelegramAuth
        from src.telegram.client_pool import ClientPool

        pool = ClientPool(TelegramAuth(0, ""), db)
        assert not pool.is_dialogs_fetched("+7")
        pool.mark_dialogs_fetched("+7")
        assert pool.is_dialogs_fetched("+7")


class TestClientPoolPremiumUnavailabilityReason:
    @pytest.mark.asyncio
    async def test_no_premium_accounts(self, db):
        from src.telegram.auth import TelegramAuth
        from src.telegram.client_pool import ClientPool

        pool = ClientPool(TelegramAuth(0, ""), db)
        reason = await pool.get_premium_unavailability_reason()
        assert "Premium" in reason or "Нет аккаунтов" in reason


# ===========================================================================
# 3. agent/manager.py — backends
# ===========================================================================


class TestEmbedHistoryInPrompt:
    def test_basic(self):
        from src.agent.manager import _embed_history_in_prompt

        result = _embed_history_in_prompt(
            [{"role": "user", "content": "hi"}, {"role": "assistant", "content": "hello"}],
            "new msg",
        )
        assert "<user>\nhi\n</user>" in result
        assert "<assistant>\nhello\n</assistant>" in result
        assert "<user>\nnew msg\n</user>" in result

    def test_empty_history(self):
        from src.agent.manager import _embed_history_in_prompt

        result = _embed_history_in_prompt([], "msg")
        assert result == "<user>\nmsg\n</user>"


class TestDeepagentsBackendProperties:
    def test_legacy_fallback_model(self):
        from src.agent.manager import DeepagentsBackend

        config = AppConfig()
        config.agent.fallback_model = "openai:gpt-4"
        db = MagicMock(spec=Database)
        backend = DeepagentsBackend(db, config)
        assert backend.legacy_fallback_model == "openai:gpt-4"

    def test_fallback_model_returns_last_used(self):
        from src.agent.manager import DeepagentsBackend

        db = MagicMock(spec=Database)
        backend = DeepagentsBackend(db, AppConfig())
        backend._last_used_model = "openai:gpt-4"
        assert backend.fallback_model == "openai:gpt-4"

    def test_fallback_provider_returns_last_used(self):
        from src.agent.manager import DeepagentsBackend

        db = MagicMock(spec=Database)
        backend = DeepagentsBackend(db, AppConfig())
        backend._last_used_provider = "openai"
        assert backend.fallback_provider == "openai"

    def test_configured_with_legacy_model(self):
        from src.agent.manager import DeepagentsBackend

        config = AppConfig()
        config.agent.fallback_model = "openai:gpt-4"
        db = MagicMock(spec=Database)
        backend = DeepagentsBackend(db, config)
        assert backend.configured is True

    def test_not_configured(self):
        from src.agent.manager import DeepagentsBackend

        config = AppConfig()
        config.agent.fallback_model = ""
        db = MagicMock(spec=Database)
        backend = DeepagentsBackend(db, config)
        # No cached configs, no legacy model
        assert backend.configured is False

    def test_provider_from_model_no_colon(self):
        from src.agent.manager import DeepagentsBackend

        db = MagicMock(spec=Database)
        backend = DeepagentsBackend(db, AppConfig())
        assert backend._provider_from_model("gpt-4") is None

    def test_provider_from_model_with_colon(self):
        from src.agent.manager import DeepagentsBackend

        db = MagicMock(spec=Database)
        backend = DeepagentsBackend(db, AppConfig())
        assert backend._provider_from_model("openai:gpt-4") == "openai"

    def test_init_error_property(self):
        from src.agent.manager import DeepagentsBackend

        db = MagicMock(spec=Database)
        backend = DeepagentsBackend(db, AppConfig())
        assert backend.init_error is None
        backend._init_error = "some error"
        assert backend.init_error == "some error"


class TestDeepagentsBackendExtractResult:
    def test_dict_with_messages(self):
        from src.agent.manager import DeepagentsBackend

        db = MagicMock(spec=Database)
        backend = DeepagentsBackend(db, AppConfig())
        msg = SimpleNamespace(content="hello world")
        result = backend._extract_result_text({"messages": [msg]})
        assert result == "hello world"

    def test_dict_with_list_content(self):
        from src.agent.manager import DeepagentsBackend

        db = MagicMock(spec=Database)
        backend = DeepagentsBackend(db, AppConfig())
        msg = SimpleNamespace(content=[{"text": "a"}, {"text": "b"}])
        result = backend._extract_result_text({"messages": [msg]})
        assert "a" in result and "b" in result

    def test_dict_empty_messages(self):
        from src.agent.manager import DeepagentsBackend

        db = MagicMock(spec=Database)
        backend = DeepagentsBackend(db, AppConfig())
        result = backend._extract_result_text({"messages": []})
        assert "messages" in result

    def test_non_dict(self):
        from src.agent.manager import DeepagentsBackend

        db = MagicMock(spec=Database)
        backend = DeepagentsBackend(db, AppConfig())
        assert backend._extract_result_text("plain text") == "plain text"


class TestDeepagentsClassifyProbeFailure:
    def test_timeout_returns_unknown(self):
        from src.agent.manager import DeepagentsBackend

        db = MagicMock(spec=Database)
        backend = DeepagentsBackend(db, AppConfig())
        status, reason = backend._classify_probe_failure(RuntimeError("Timed out waiting"))
        assert status == "unknown"

    def test_auth_error_returns_unknown(self):
        from src.agent.manager import DeepagentsBackend

        db = MagicMock(spec=Database)
        backend = DeepagentsBackend(db, AppConfig())
        status, reason = backend._classify_probe_failure(RuntimeError("unauthorized access"))
        assert status == "unknown"

    def test_generic_error_returns_unsupported(self):
        from src.agent.manager import DeepagentsBackend

        db = MagicMock(spec=Database)
        backend = DeepagentsBackend(db, AppConfig())
        status, reason = backend._classify_probe_failure(RuntimeError("model not available"))
        assert status == "unsupported"


class TestDeepagentsBackendValidation:
    def test_legacy_validation_no_model(self):
        from src.agent.manager import DeepagentsBackend
        from src.agent.provider_registry import ProviderRuntimeConfig

        db = MagicMock(spec=Database)
        backend = DeepagentsBackend(db, AppConfig())
        cfg = ProviderRuntimeConfig(
            provider="openai", enabled=True, priority=0,
            selected_model="", plain_fields={}, secret_fields={},
        )
        assert backend._legacy_validation_error(cfg) != ""

    def test_legacy_validation_anthropic_no_key(self):
        from src.agent.manager import DeepagentsBackend
        from src.agent.provider_registry import ProviderRuntimeConfig

        db = MagicMock(spec=Database)
        backend = DeepagentsBackend(db, AppConfig())
        cfg = ProviderRuntimeConfig(
            provider="anthropic", enabled=True, priority=0,
            selected_model="anthropic:claude-3", plain_fields={}, secret_fields={},
        )
        assert "AGENT_FALLBACK_API_KEY" in backend._legacy_validation_error(cfg)


class TestClaudeSdkBackendAvailable:
    def test_available_with_api_key(self):
        from src.agent.manager import ClaudeSdkBackend

        db = MagicMock(spec=Database)
        backend = ClaudeSdkBackend(db, AppConfig())
        with patch.dict("os.environ", {"ANTHROPIC_API_KEY": "sk-test"}):
            assert backend.available is True

    def test_not_available_without_key(self):
        from src.agent.manager import ClaudeSdkBackend

        db = MagicMock(spec=Database)
        backend = ClaudeSdkBackend(db, AppConfig())
        with patch.dict("os.environ", {}, clear=True):
            # Remove both vars
            import os
            old_api = os.environ.pop("ANTHROPIC_API_KEY", None)
            old_oauth = os.environ.pop("CLAUDE_CODE_OAUTH_TOKEN", None)
            try:
                assert backend.available is False
            finally:
                if old_api:
                    os.environ["ANTHROPIC_API_KEY"] = old_api
                if old_oauth:
                    os.environ["CLAUDE_CODE_OAUTH_TOKEN"] = old_oauth


class TestAgentManager:
    def test_initialize_calls_backends(self):
        from src.agent.manager import AgentManager

        db = MagicMock(spec=Database)
        manager = AgentManager(db, AppConfig())
        manager._claude_backend = MagicMock()
        manager._deepagents_backend = MagicMock()
        manager._deepagents_backend.configured = False
        manager._deepagents_backend.preflight_available = None
        manager.initialize()
        manager._claude_backend.initialize.assert_called_once()

    @pytest.mark.asyncio
    async def test_refresh_settings_cache(self):
        from src.agent.manager import AgentManager

        db = MagicMock(spec=Database)
        manager = AgentManager(db, AppConfig())
        manager._deepagents_backend = MagicMock()
        manager._deepagents_backend.refresh_settings_cache = AsyncMock()
        manager._deepagents_backend.configured = False
        await manager.refresh_settings_cache()
        manager._deepagents_backend.refresh_settings_cache.assert_awaited_once()


class TestAgentRuntimeStatus:
    def test_fields(self):
        from src.agent.manager import AgentRuntimeStatus

        status = AgentRuntimeStatus(
            claude_available=True,
            deepagents_available=False,
            dev_mode_enabled=False,
            backend_override="",
            selected_backend="claude",
            fallback_model="",
            fallback_provider="",
            using_override=False,
        )
        assert status.claude_available is True
        assert status.error is None


# ===========================================================================
# 4. telegram/collector.py — media types, cancellation, auto-delete
# ===========================================================================


class TestCollectorGetMediaType:
    def _get_media_type(self, media):
        from src.telegram.collector import Collector as _Collector

        return _Collector._get_media_type(SimpleNamespace(media=media))

    def test_none_media(self):
        assert self._get_media_type(None) is None

    def test_photo(self):
        from telethon.tl.types import MessageMediaPhoto

        media = MagicMock(spec=MessageMediaPhoto)
        media.__class__ = MessageMediaPhoto
        assert self._get_media_type(media) == "photo"

    def test_document_sticker(self):
        from telethon.tl.types import (
            DocumentAttributeSticker,
            MessageMediaDocument,
        )

        doc = MagicMock()
        doc.attributes = [MagicMock(spec=DocumentAttributeSticker)]
        doc.attributes[0].__class__ = DocumentAttributeSticker
        media = MagicMock(spec=MessageMediaDocument)
        media.__class__ = MessageMediaDocument
        media.document = doc
        assert self._get_media_type(media) == "sticker"

    def test_document_video(self):
        from telethon.tl.types import (
            DocumentAttributeVideo,
            MessageMediaDocument,
        )

        attr = MagicMock(spec=DocumentAttributeVideo)
        attr.__class__ = DocumentAttributeVideo
        attr.round_message = False
        doc = MagicMock()
        doc.attributes = [attr]
        media = MagicMock(spec=MessageMediaDocument)
        media.__class__ = MessageMediaDocument
        media.document = doc
        assert self._get_media_type(media) == "video"

    def test_document_video_note(self):
        from telethon.tl.types import (
            DocumentAttributeVideo,
            MessageMediaDocument,
        )

        attr = MagicMock(spec=DocumentAttributeVideo)
        attr.__class__ = DocumentAttributeVideo
        attr.round_message = True
        doc = MagicMock()
        doc.attributes = [attr]
        media = MagicMock(spec=MessageMediaDocument)
        media.__class__ = MessageMediaDocument
        media.document = doc
        assert self._get_media_type(media) == "video_note"

    def test_document_audio(self):
        from telethon.tl.types import (
            DocumentAttributeAudio,
            MessageMediaDocument,
        )

        attr = MagicMock(spec=DocumentAttributeAudio)
        attr.__class__ = DocumentAttributeAudio
        attr.voice = False
        doc = MagicMock()
        doc.attributes = [attr]
        media = MagicMock(spec=MessageMediaDocument)
        media.__class__ = MessageMediaDocument
        media.document = doc
        assert self._get_media_type(media) == "audio"

    def test_document_voice(self):
        from telethon.tl.types import (
            DocumentAttributeAudio,
            MessageMediaDocument,
        )

        attr = MagicMock(spec=DocumentAttributeAudio)
        attr.__class__ = DocumentAttributeAudio
        attr.voice = True
        doc = MagicMock()
        doc.attributes = [attr]
        media = MagicMock(spec=MessageMediaDocument)
        media.__class__ = MessageMediaDocument
        media.document = doc
        assert self._get_media_type(media) == "voice"

    def test_document_gif(self):
        from telethon.tl.types import (
            DocumentAttributeAnimated,
            MessageMediaDocument,
        )

        attr = MagicMock(spec=DocumentAttributeAnimated)
        attr.__class__ = DocumentAttributeAnimated
        doc = MagicMock()
        doc.attributes = [attr]
        media = MagicMock(spec=MessageMediaDocument)
        media.__class__ = MessageMediaDocument
        media.document = doc
        assert self._get_media_type(media) == "gif"

    def test_document_plain(self):
        from telethon.tl.types import MessageMediaDocument

        doc = MagicMock()
        doc.attributes = []
        media = MagicMock(spec=MessageMediaDocument)
        media.__class__ = MessageMediaDocument
        media.document = doc
        assert self._get_media_type(media) == "document"

    def test_web_page(self):
        from telethon.tl.types import MessageMediaWebPage

        media = MagicMock(spec=MessageMediaWebPage)
        media.__class__ = MessageMediaWebPage
        assert self._get_media_type(media) == "web_page"

    def test_geo(self):
        from telethon.tl.types import MessageMediaGeo

        media = MagicMock(spec=MessageMediaGeo)
        media.__class__ = MessageMediaGeo
        assert self._get_media_type(media) == "location"

    def test_geo_live(self):
        from telethon.tl.types import MessageMediaGeoLive

        media = MagicMock(spec=MessageMediaGeoLive)
        media.__class__ = MessageMediaGeoLive
        assert self._get_media_type(media) == "geo_live"

    def test_contact(self):
        from telethon.tl.types import MessageMediaContact

        media = MagicMock(spec=MessageMediaContact)
        media.__class__ = MessageMediaContact
        assert self._get_media_type(media) == "contact"

    def test_poll(self):
        from telethon.tl.types import MessageMediaPoll

        media = MagicMock(spec=MessageMediaPoll)
        media.__class__ = MessageMediaPoll
        assert self._get_media_type(media) == "poll"

    def test_dice(self):
        from telethon.tl.types import MessageMediaDice

        media = MagicMock(spec=MessageMediaDice)
        media.__class__ = MessageMediaDice
        assert self._get_media_type(media) == "dice"

    def test_game(self):
        from telethon.tl.types import MessageMediaGame

        media = MagicMock(spec=MessageMediaGame)
        media.__class__ = MessageMediaGame
        assert self._get_media_type(media) == "game"

    def test_unknown(self):
        media = SimpleNamespace()
        assert self._get_media_type(media) == "unknown"


class TestCollectorProperties:
    @pytest.mark.asyncio
    async def test_is_cancelled(self, db):
        from tests.helpers import make_mock_pool

        pool = make_mock_pool()
        collector = Collector(pool, db, SchedulerConfig())
        assert not collector.is_cancelled
        await collector.cancel()
        assert collector.is_cancelled

    @pytest.mark.asyncio
    async def test_delay_between_channels(self, db):
        from tests.helpers import make_mock_pool

        pool = make_mock_pool()
        config = SchedulerConfig(delay_between_channels_sec=5)
        collector = Collector(pool, db, config)
        assert collector.delay_between_channels_sec == 5


class TestCollectorAutoDelete:
    @pytest.mark.asyncio
    async def test_auto_delete_not_enabled(self, db):
        from tests.helpers import make_mock_pool

        pool = make_mock_pool()
        collector = Collector(pool, db, SchedulerConfig())
        result = await collector._maybe_auto_delete(100)
        assert result is False

    @pytest.mark.asyncio
    async def test_auto_delete_enabled(self, db):
        from tests.helpers import make_mock_pool

        pool = make_mock_pool()
        collector = Collector(pool, db, SchedulerConfig())
        await db.set_setting("auto_delete_on_collect", "1")
        collector._auto_delete_cached = None
        result = await collector._maybe_auto_delete(100)
        assert result is True

    @pytest.mark.asyncio
    async def test_auto_delete_cached(self, db):
        from tests.helpers import make_mock_pool

        pool = make_mock_pool()
        collector = Collector(pool, db, SchedulerConfig())
        collector._auto_delete_cached = True
        assert await collector._is_auto_delete_enabled() is True


# ===========================================================================
# 5. services/agent_provider_service.py — remaining paths
# ===========================================================================


class TestAgentProviderServiceRefresh:
    @pytest.mark.asyncio
    async def test_refresh_models_for_provider_live_fail(self, db):
        from src.services.agent_provider_service import AgentProviderService

        config = AppConfig()
        svc = AgentProviderService(db, config)
        # openai is a known provider
        with patch.object(svc, "_fetch_live_models", AsyncMock(side_effect=RuntimeError("network"))):
            entry = await svc.refresh_models_for_provider("openai")
        assert entry.error == "network"
        assert entry.source == "static cache"

    @pytest.mark.asyncio
    async def test_refresh_all_models(self, db):
        from src.agent.provider_registry import ProviderRuntimeConfig
        from src.services.agent_provider_service import AgentProviderService

        config = AppConfig()
        svc = AgentProviderService(db, config)
        cfg = ProviderRuntimeConfig(
            provider="openai", enabled=True, priority=0,
            selected_model="gpt-4", plain_fields={}, secret_fields={},
        )
        with patch.object(svc, "_fetch_live_models", AsyncMock(return_value=["gpt-4", "gpt-3.5"])):
            results = await svc.refresh_all_models(configs=[cfg])
        assert "openai" in results
        assert "gpt-4" in results["openai"].models


class TestAgentProviderServiceSaveLoad:
    @pytest.mark.asyncio
    async def test_save_and_load_round_trip(self, db):
        from src.agent.provider_registry import ProviderRuntimeConfig
        from src.security import SessionCipher
        from src.services.agent_provider_service import AgentProviderService

        config = AppConfig()
        svc = AgentProviderService(db, config)
        svc._cipher = SessionCipher("test-secret-key-123456")

        cfg = ProviderRuntimeConfig(
            provider="openai", enabled=True, priority=0,
            selected_model="gpt-4", plain_fields={},
            secret_fields={"api_key": "sk-test"},
        )
        await svc.save_provider_configs([cfg])
        loaded = await svc.load_provider_configs()
        assert len(loaded) == 1
        assert loaded[0].provider == "openai"
        assert loaded[0].secret_fields["api_key"] == "sk-test"


class TestAgentProviderServiceValidation:
    def test_validate_missing_model(self):
        from src.agent.provider_registry import ProviderRuntimeConfig
        from src.services.agent_provider_service import AgentProviderService

        svc = AgentProviderService(MagicMock(), AppConfig())
        cfg = ProviderRuntimeConfig(
            provider="openai", enabled=True, priority=0,
            selected_model="", plain_fields={}, secret_fields={"api_key": "x"},
        )
        assert "Model is required" in svc.validate_provider_config(cfg)

    def test_validate_unknown_provider(self):
        from src.agent.provider_registry import ProviderRuntimeConfig
        from src.services.agent_provider_service import AgentProviderService

        svc = AgentProviderService(MagicMock(), AppConfig())
        cfg = ProviderRuntimeConfig(
            provider="unknown_xyz", enabled=True, priority=0,
            selected_model="m", plain_fields={}, secret_fields={},
        )
        assert "Unknown provider" in svc.validate_provider_config(cfg)

    def test_create_empty_config_unknown_provider(self):
        from src.services.agent_provider_service import AgentProviderService

        svc = AgentProviderService(MagicMock(), AppConfig())
        with pytest.raises(RuntimeError, match="Unknown provider"):
            svc.create_empty_config("zzz_unknown", 0)


class TestAgentProviderServiceCompat:
    def test_compatibility_error_unsupported(self, db):
        from src.agent.provider_registry import ProviderRuntimeConfig
        from src.services.agent_provider_service import (
            AgentProviderService,
            ProviderModelCacheEntry,
            ProviderModelCompatibilityRecord,
        )

        svc = AgentProviderService(db, AppConfig())
        cfg = ProviderRuntimeConfig(
            provider="openai", enabled=True, priority=0,
            selected_model="gpt-4", plain_fields={}, secret_fields={"api_key": "x"},
        )
        fingerprint = svc.config_fingerprint(cfg)
        record = ProviderModelCompatibilityRecord(
            model="gpt-4",
            status="unsupported",
            reason="not compatible",
            tested_at=datetime.now(UTC).isoformat(),
            config_fingerprint=fingerprint,
        )
        cache_entry = ProviderModelCacheEntry(
            provider="openai", models=["gpt-4"], source="static",
            compatibility={fingerprint: record},
        )
        error = svc.compatibility_error_for_config(cfg, cache_entry)
        assert error == "not compatible"

    def test_compatibility_warning_unknown(self, db):
        from src.agent.provider_registry import ProviderRuntimeConfig
        from src.services.agent_provider_service import (
            AgentProviderService,
            ProviderModelCacheEntry,
            ProviderModelCompatibilityRecord,
        )

        svc = AgentProviderService(db, AppConfig())
        cfg = ProviderRuntimeConfig(
            provider="openai", enabled=True, priority=0,
            selected_model="gpt-4", plain_fields={}, secret_fields={"api_key": "x"},
        )
        fingerprint = svc.config_fingerprint(cfg)
        record = ProviderModelCompatibilityRecord(
            model="gpt-4",
            status="unknown",
            reason="dunno",
            tested_at=datetime.now(UTC).isoformat(),
            config_fingerprint=fingerprint,
        )
        cache_entry = ProviderModelCacheEntry(
            provider="openai", models=["gpt-4"], source="static",
            compatibility={fingerprint: record},
        )
        warning = svc.compatibility_warning_for_config(cfg, cache_entry)
        assert "не подтверждена" in warning or "dunno" in warning


# ===========================================================================
# 6. deepagents_sync.py — remaining sync tools
# ===========================================================================


class TestRunSync:
    def test_outside_event_loop(self):
        from src.agent.tools.deepagents_sync import _run_sync

        async def _op():
            return 42

        result = _run_sync("test_tool", _op)
        assert result == 42

    @pytest.mark.asyncio
    async def test_inside_event_loop_raises(self):
        from src.agent.tools.deepagents_sync import _run_sync

        async def _op():
            return 42

        with pytest.raises(RuntimeError, match="cannot run inside"):
            _run_sync("test_tool", _op)


class TestDeepagentsSyncTools:
    @pytest.fixture
    def sync_tools(self, mock_db):
        from src.agent.tools.deepagents_sync import build_deepagents_tools

        return {t.__name__: t for t in build_deepagents_tools(mock_db)}

    def test_toggle_pipeline(self, sync_tools, mock_db):
        with patch("src.services.pipeline_service.PipelineService") as svc_cls:
            svc_cls.return_value.toggle = AsyncMock()
            result = sync_tools["toggle_pipeline"](1)
        assert "переключён" in result

    def test_toggle_pipeline_error(self, sync_tools):
        with patch("src.services.pipeline_service.PipelineService", side_effect=RuntimeError("err")):
            result = sync_tools["toggle_pipeline"](1)
        assert "Ошибка" in result

    def test_delete_pipeline(self, sync_tools, mock_db):
        with patch("src.services.pipeline_service.PipelineService") as svc_cls:
            svc_cls.return_value.delete = AsyncMock()
            result = sync_tools["delete_pipeline"](1)
        assert "удалён" in result

    def test_toggle_search_query(self, sync_tools, mock_db):
        with patch("src.services.search_query_service.SearchQueryService") as svc_cls:
            svc_cls.return_value.toggle = AsyncMock()
            result = sync_tools["toggle_search_query"](1)
        assert "переключён" in result

    def test_delete_search_query(self, sync_tools, mock_db):
        with patch("src.services.search_query_service.SearchQueryService") as svc_cls:
            svc_cls.return_value.delete = AsyncMock()
            result = sync_tools["delete_search_query"](1)
        assert "удалён" in result

    def test_list_accounts_empty(self, sync_tools, mock_db):
        mock_db.get_accounts = AsyncMock(return_value=[])
        result = sync_tools["list_accounts"]()
        assert "не найдены" in result

    def test_list_accounts_with_data(self, sync_tools, mock_db):
        acc = SimpleNamespace(id=1, phone="+7", is_active=True)
        mock_db.get_accounts = AsyncMock(return_value=[acc])
        result = sync_tools["list_accounts"]()
        assert "+7" in result

    def test_toggle_account_not_found(self, sync_tools, mock_db):
        mock_db.get_accounts = AsyncMock(return_value=[])
        result = sync_tools["toggle_account"](999)
        assert "не найден" in result

    def test_delete_account(self, sync_tools, mock_db):
        mock_db.delete_account = AsyncMock()
        result = sync_tools["delete_account"](1)
        assert "удалён" in result

    def test_get_flood_status(self, sync_tools, mock_db):
        acc = SimpleNamespace(phone="+7", flood_wait_until=None)
        mock_db.get_accounts = AsyncMock(return_value=[acc])
        result = sync_tools["get_flood_status"]()
        assert "+7" in result

    def test_get_flood_status_empty(self, sync_tools, mock_db):
        mock_db.get_accounts = AsyncMock(return_value=[])
        result = sync_tools["get_flood_status"]()
        assert "не найдены" in result

    def test_analyze_filters(self, sync_tools, mock_db):
        with patch("src.filters.analyzer.ChannelAnalyzer") as cls:
            report = SimpleNamespace(results=[])
            cls.return_value.analyze_all = AsyncMock(return_value=report)
            result = sync_tools["analyze_filters"]()
        assert "0" in result or "проверено" in result

    def test_apply_filters(self, sync_tools, mock_db):
        with patch("src.filters.analyzer.ChannelAnalyzer") as cls:
            report = SimpleNamespace(results=[])
            cls.return_value.analyze_all = AsyncMock(return_value=report)
            cls.return_value.apply_filters = AsyncMock(return_value=0)
            result = sync_tools["apply_filters"]()
        assert "помечены" in result

    def test_reset_filters(self, sync_tools, mock_db):
        with patch("src.filters.analyzer.ChannelAnalyzer") as cls:
            cls.return_value.reset_filters = AsyncMock(return_value=3)
            result = sync_tools["reset_filters"]()
        assert "3" in result

    def test_toggle_channel_filter_not_found(self, sync_tools, mock_db):
        mock_db.get_channel_by_pk = AsyncMock(return_value=None)
        result = sync_tools["toggle_channel_filter"](999)
        assert "не найден" in result


# ===========================================================================
# 7. scheduler/manager.py — jobs, sync, pipeline
# ===========================================================================


class TestSchedulerManager:
    @pytest.mark.asyncio
    async def test_is_running_false(self):
        from src.scheduler.manager import SchedulerManager

        sm = SchedulerManager(SchedulerConfig())
        assert sm.is_running is False

    @pytest.mark.asyncio
    async def test_interval_minutes(self):
        from src.scheduler.manager import SchedulerManager

        sm = SchedulerManager(SchedulerConfig(collect_interval_minutes=30))
        assert sm.interval_minutes == 30

    @pytest.mark.asyncio
    async def test_is_job_enabled_no_bundle(self):
        from src.scheduler.manager import SchedulerManager

        sm = SchedulerManager(SchedulerConfig())
        assert await sm.is_job_enabled("collect_all") is True

    @pytest.mark.asyncio
    async def test_update_interval_no_scheduler(self):
        from src.scheduler.manager import SchedulerManager

        sm = SchedulerManager(SchedulerConfig())
        sm.update_interval(15)
        assert sm._current_interval_minutes == 15

    @pytest.mark.asyncio
    async def test_get_job_next_run_no_scheduler(self):
        from src.scheduler.manager import SchedulerManager

        sm = SchedulerManager(SchedulerConfig())
        assert sm.get_job_next_run("collect_all") is None

    @pytest.mark.asyncio
    async def test_get_all_jobs_no_scheduler(self):
        from src.scheduler.manager import SchedulerManager

        sm = SchedulerManager(SchedulerConfig())
        assert sm.get_all_jobs_next_run() == {}

    @pytest.mark.asyncio
    async def test_stop_not_running(self):
        from src.scheduler.manager import SchedulerManager

        sm = SchedulerManager(SchedulerConfig())
        await sm.stop()  # Should not raise

    @pytest.mark.asyncio
    async def test_trigger_now_no_enqueuer(self):
        from src.scheduler.manager import SchedulerManager

        sm = SchedulerManager(SchedulerConfig())
        result = await sm.trigger_now()
        assert result["enqueued"] == 0

    @pytest.mark.asyncio
    async def test_trigger_background_no_enqueuer(self):
        from src.scheduler.manager import SchedulerManager

        sm = SchedulerManager(SchedulerConfig())
        await sm.trigger_background()
        # bg task should complete quickly
        if sm._bg_task:
            await sm._bg_task

    @pytest.mark.asyncio
    async def test_run_photo_due_no_enqueuer(self):
        from src.scheduler.manager import SchedulerManager

        sm = SchedulerManager(SchedulerConfig())
        result = await sm._run_photo_due()
        assert result == {"processed": 0}

    @pytest.mark.asyncio
    async def test_run_photo_auto_no_enqueuer(self):
        from src.scheduler.manager import SchedulerManager

        sm = SchedulerManager(SchedulerConfig())
        result = await sm._run_photo_auto()
        assert result == {"jobs": 0}

    @pytest.mark.asyncio
    async def test_run_pipeline_job_no_enqueuer(self):
        from src.scheduler.manager import SchedulerManager

        sm = SchedulerManager(SchedulerConfig())
        await sm._run_pipeline_job(1)  # Should not raise

    @pytest.mark.asyncio
    async def test_run_content_generate_no_enqueuer(self):
        from src.scheduler.manager import SchedulerManager

        sm = SchedulerManager(SchedulerConfig())
        await sm._run_content_generate_job(1)  # Should not raise

    @pytest.mark.asyncio
    async def test_run_search_query_no_enqueuer(self):
        from src.scheduler.manager import SchedulerManager

        sm = SchedulerManager(SchedulerConfig())
        await sm._run_search_query(1)  # Should not raise

    @pytest.mark.asyncio
    async def test_load_settings_no_bundle(self):
        from src.scheduler.manager import SchedulerManager

        sm = SchedulerManager(SchedulerConfig())
        await sm.load_settings()  # Should not raise

    @pytest.mark.asyncio
    async def test_sync_job_state_not_running(self):
        from src.scheduler.manager import SchedulerManager

        sm = SchedulerManager(SchedulerConfig())
        await sm.sync_job_state("collect_all", True)  # Should not raise

    @pytest.mark.asyncio
    async def test_get_potential_jobs(self):
        from src.scheduler.manager import SchedulerManager

        sm = SchedulerManager(SchedulerConfig())
        jobs = await sm.get_potential_jobs()
        assert any(j["job_id"] == "collect_all" for j in jobs)


# ===========================================================================
# 8. services/unified_dispatcher.py — dispatch, handlers
# ===========================================================================


def _make_dispatcher(**overrides):
    from src.services.unified_dispatcher import UnifiedDispatcher

    collector = MagicMock()
    collector.is_running = False
    collector.delay_between_channels_sec = 0
    collector.get_stats_availability = AsyncMock(
        return_value=SimpleNamespace(state="available")
    )
    channel_bundle = MagicMock()
    tasks_repo = MagicMock()
    tasks_repo.requeue_running_generic_tasks_on_startup = AsyncMock(return_value=0)
    tasks_repo.claim_next_due_generic_task = AsyncMock(return_value=None)
    tasks_repo.update_collection_task = AsyncMock()
    tasks_repo.update_collection_task_progress = AsyncMock()
    tasks_repo.get_collection_task = AsyncMock(return_value=None)

    kwargs = {
        "collector": collector,
        "channel_bundle": channel_bundle,
        "tasks_repo": tasks_repo,
        "poll_interval_sec": 0.01,
    }
    kwargs.update(overrides)
    return UnifiedDispatcher(**kwargs), tasks_repo, collector


class TestUnifiedDispatcherDispatch:
    @pytest.mark.asyncio
    async def test_unknown_task_type(self):
        dispatcher, tasks_repo, _ = _make_dispatcher()
        task = CollectionTask(
            id=1,
            task_type=CollectionTaskType.CHANNEL_COLLECT,
            status=CollectionTaskStatus.RUNNING,
        )
        await dispatcher._dispatch(task)
        tasks_repo.update_collection_task.assert_awaited()
        call_args = tasks_repo.update_collection_task.call_args
        assert call_args[0][1] == CollectionTaskStatus.FAILED

    @pytest.mark.asyncio
    async def test_photo_due_no_service(self):
        dispatcher, tasks_repo, _ = _make_dispatcher()
        task = CollectionTask(
            id=1,
            task_type=CollectionTaskType.PHOTO_DUE,
            status=CollectionTaskStatus.RUNNING,
        )
        await dispatcher._handle_photo_due(task)
        tasks_repo.update_collection_task.assert_awaited()
        call_args = tasks_repo.update_collection_task.call_args
        assert call_args[0][1] == CollectionTaskStatus.COMPLETED

    @pytest.mark.asyncio
    async def test_photo_auto_no_service(self):
        dispatcher, tasks_repo, _ = _make_dispatcher()
        task = CollectionTask(
            id=1,
            task_type=CollectionTaskType.PHOTO_AUTO,
            status=CollectionTaskStatus.RUNNING,
        )
        await dispatcher._handle_photo_auto(task)
        tasks_repo.update_collection_task.assert_awaited()

    @pytest.mark.asyncio
    async def test_sq_stats_no_bundle(self):
        dispatcher, tasks_repo, _ = _make_dispatcher()
        task = CollectionTask(
            id=1,
            task_type=CollectionTaskType.SQ_STATS,
            status=CollectionTaskStatus.RUNNING,
            payload=SqStatsTaskPayload(sq_id=1),
        )
        await dispatcher._handle_sq_stats(task)
        tasks_repo.update_collection_task.assert_awaited()

    @pytest.mark.asyncio
    async def test_sq_stats_invalid_payload(self):
        dispatcher, tasks_repo, _ = _make_dispatcher(sq_bundle=MagicMock())
        task = CollectionTask.model_construct(
            id=1,
            task_type=CollectionTaskType.SQ_STATS,
            status=CollectionTaskStatus.RUNNING,
            payload={"wrong": "dict"},
        )
        await dispatcher._handle_sq_stats(task)
        call_args = tasks_repo.update_collection_task.call_args
        assert call_args[0][1] == CollectionTaskStatus.FAILED

    @pytest.mark.asyncio
    async def test_pipeline_run_no_env(self):
        dispatcher, tasks_repo, _ = _make_dispatcher()
        task = CollectionTask(
            id=1,
            task_type=CollectionTaskType.PIPELINE_RUN,
            status=CollectionTaskStatus.RUNNING,
            payload=PipelineRunTaskPayload(pipeline_id=1),
        )
        await dispatcher._handle_pipeline_run(task)
        call_args = tasks_repo.update_collection_task.call_args
        assert call_args[0][1] == CollectionTaskStatus.FAILED
        assert "not configured" in str(call_args)

    @pytest.mark.asyncio
    async def test_pipeline_run_invalid_payload(self):
        dispatcher, tasks_repo, _ = _make_dispatcher(
            pipeline_bundle=MagicMock(), search_engine=MagicMock(), db=MagicMock()
        )
        task = CollectionTask.model_construct(
            id=1,
            task_type=CollectionTaskType.PIPELINE_RUN,
            status=CollectionTaskStatus.RUNNING,
            payload={"wrong": "dict"},
        )
        await dispatcher._handle_pipeline_run(task)
        call_args = tasks_repo.update_collection_task.call_args
        assert call_args[0][1] == CollectionTaskStatus.FAILED

    @pytest.mark.asyncio
    async def test_content_generate_no_env(self):
        dispatcher, tasks_repo, _ = _make_dispatcher()
        task = CollectionTask(
            id=1,
            task_type=CollectionTaskType.CONTENT_GENERATE,
            status=CollectionTaskStatus.RUNNING,
            payload=ContentGenerateTaskPayload(pipeline_id=1),
        )
        await dispatcher._handle_content_generate(task)
        call_args = tasks_repo.update_collection_task.call_args
        assert call_args[0][1] == CollectionTaskStatus.FAILED

    @pytest.mark.asyncio
    async def test_content_generate_invalid_payload(self):
        dispatcher, tasks_repo, _ = _make_dispatcher(
            pipeline_bundle=MagicMock(), search_engine=MagicMock(), db=MagicMock()
        )
        task = CollectionTask.model_construct(
            id=1,
            task_type=CollectionTaskType.CONTENT_GENERATE,
            status=CollectionTaskStatus.RUNNING,
            payload={"wrong": "dict"},
        )
        await dispatcher._handle_content_generate(task)
        call_args = tasks_repo.update_collection_task.call_args
        assert call_args[0][1] == CollectionTaskStatus.FAILED

    @pytest.mark.asyncio
    async def test_content_publish_no_id(self):
        dispatcher, tasks_repo, _ = _make_dispatcher()
        task = CollectionTask(
            id=None,
            task_type=CollectionTaskType.CONTENT_PUBLISH,
            status=CollectionTaskStatus.RUNNING,
        )
        await dispatcher._handle_content_publish(task)
        tasks_repo.update_collection_task.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_stats_all_no_id(self):
        dispatcher, tasks_repo, _ = _make_dispatcher()
        task = CollectionTask(
            id=None,
            task_type=CollectionTaskType.STATS_ALL,
            status=CollectionTaskStatus.RUNNING,
        )
        await dispatcher._handle_stats_all(task)
        tasks_repo.update_collection_task.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_stats_all_invalid_payload(self):
        dispatcher, tasks_repo, _ = _make_dispatcher()
        task = CollectionTask.model_construct(
            id=1,
            task_type=CollectionTaskType.STATS_ALL,
            status=CollectionTaskStatus.RUNNING,
            payload={"wrong": "dict"},
        )
        await dispatcher._handle_stats_all(task)
        call_args = tasks_repo.update_collection_task.call_args
        assert call_args[0][1] == CollectionTaskStatus.FAILED

    @pytest.mark.asyncio
    async def test_stats_all_completed_all_done(self):
        dispatcher, tasks_repo, _ = _make_dispatcher()
        payload = StatsAllTaskPayload(
            channel_ids=[-100, -200],
            next_index=2,  # >= len(channel_ids), so done
            batch_size=20,
        )
        task = CollectionTask(
            id=1,
            task_type=CollectionTaskType.STATS_ALL,
            status=CollectionTaskStatus.RUNNING,
            payload=payload,
        )
        await dispatcher._handle_stats_all(task)
        call_args = tasks_repo.update_collection_task.call_args
        assert call_args[0][1] == CollectionTaskStatus.COMPLETED


class TestUnifiedDispatcherStartStop:
    @pytest.mark.asyncio
    async def test_start_and_stop(self):
        dispatcher, tasks_repo, _ = _make_dispatcher()
        await dispatcher.start()
        assert dispatcher._task is not None
        await dispatcher.stop()
        assert dispatcher._task is None

    @pytest.mark.asyncio
    async def test_start_already_running(self):
        dispatcher, tasks_repo, _ = _make_dispatcher()
        await dispatcher.start()
        task1 = dispatcher._task
        await dispatcher.start()  # Should not create a new task
        assert dispatcher._task is task1
        await dispatcher.stop()
