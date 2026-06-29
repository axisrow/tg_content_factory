"""Coverage tests for business logic services."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.config import AppConfig
from src.database import Database

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_mock_db():
    db = MagicMock(spec=Database)
    db.repos = MagicMock()
    db._db_path = ":memory:"
    db._session_encryption_secret = None
    return db










# ===========================================================================
# 1. cli/commands/test.py — read/write check functions
# ===========================================================================




class TestProviderConfigServiceExtended:
    async def test_normalize_urlish_empty(self, db):
        from src.services.agent_provider_service import ProviderConfigService
        config = AppConfig()
        svc = ProviderConfigService(db, config)
        assert svc._normalize_urlish("") == ""

    async def test_normalize_urlish_no_scheme(self, db):
        from src.services.agent_provider_service import ProviderConfigService
        config = AppConfig()
        svc = ProviderConfigService(db, config)
        result = svc._normalize_urlish("example.com/api")
        assert result == "example.com/api"

    async def test_normalize_urlish_with_scheme(self, db):
        from src.services.agent_provider_service import ProviderConfigService
        config = AppConfig()
        svc = ProviderConfigService(db, config)
        result = svc._normalize_urlish("https://example.com/api/")
        assert result.endswith("/api")
        assert not result.endswith("/")

    async def test_config_sort_key_unknown_provider(self, db):
        from src.services.agent_provider_service import ProviderConfigService, ProviderRuntimeConfig
        config = AppConfig()
        svc = ProviderConfigService(db, config)
        cfg = ProviderRuntimeConfig(
            provider="unknown_provider", enabled=True, priority=5,
            selected_model="m", plain_fields={}, secret_fields={},
        )
        key = svc._config_sort_key(cfg)
        assert key[0] == 5

    async def test_empty_model_cache_entry_unknown(self, db):
        from src.services.agent_provider_service import ProviderConfigService
        config = AppConfig()
        svc = ProviderConfigService(db, config)
        with pytest.raises(RuntimeError, match="Unknown provider"):
            svc._empty_model_cache_entry("totally_unknown")

    async def test_compatibility_view_none(self, db):
        from src.services.agent_provider_service import ProviderConfigService
        config = AppConfig()
        svc = ProviderConfigService(db, config)
        assert svc._compatibility_view(None) is None

    async def test_compatibility_view_with_record(self, db):
        from src.services.agent_provider_service import (
            ProviderConfigService,
            ProviderModelCompatibilityRecord,
        )
        config = AppConfig()
        svc = ProviderConfigService(db, config)
        record = ProviderModelCompatibilityRecord(
            model="test", status="ok", reason="",
            config_fingerprint="fp", probe_kind="dev",
        )
        result = svc._compatibility_view(record)
        assert result is not None
        assert result["model"] == "test"

    async def test_decrypt_no_cipher(self, db):
        import pytest

        from src.services.agent_provider_service import ProviderConfigService, provider_spec
        config = AppConfig()
        svc = ProviderConfigService(db, config)
        svc._cipher = None
        spec = provider_spec("openai")
        if spec and spec.secret_fields:
            with pytest.raises(ValueError, match="SESSION_ENCRYPTION_KEY"):
                svc._decrypt_secret_fields({"api_key": "secret"}, spec)

    async def test_app_version(self, db):
        from src.services.agent_provider_service import ProviderConfigService
        config = AppConfig()
        svc = ProviderConfigService(db, config)
        version = svc._app_version()
        assert isinstance(version, str)


# ===========================================================================
# 7. telegram/client_pool.py — dialog cache
# ===========================================================================




class TestCollectionQueueExtraCoverage:
    """Cover remaining gaps in collection_queue.py."""

    async def test_cancel_task_marks_only_matching_active_task(self):
        """Cancel marks the matching active task without cancelling the whole collector."""
        from src.collection_queue import CollectionQueue

        channels = MagicMock()
        channels.cancel_collection_task = AsyncMock(return_value=True)
        collector = MagicMock()
        collector.cancel = AsyncMock()

        queue = CollectionQueue(collector, channels)
        cancel_event = asyncio.Event()
        other_event = asyncio.Event()
        queue._active_task_ids[42] = cancel_event
        queue._active_task_ids[43] = other_event

        result = await queue.cancel_task(42, note="test")

        assert cancel_event.is_set()
        assert not other_event.is_set()
        collector.cancel.assert_not_awaited()
        assert result is True

    async def test_worker_skips_cancelled_task(self):
        """Lines 96-98: task with CANCELLED status is skipped."""
        from src.collection_queue import CollectionQueue
        from src.models import Channel, CollectionTask, CollectionTaskStatus

        channels = MagicMock()
        task = CollectionTask(
            id=1,
            channel_id=100,
            title="ch",
            status=CollectionTaskStatus.CANCELLED,
        )
        channels.get_collection_task = AsyncMock(return_value=task)
        collector = MagicMock()

        queue = CollectionQueue(collector, channels)
        ch = Channel(id=1, channel_id=100, title="test")
        queue._queue.put_nowait((1, ch, False, True))

        # Run worker for a short time
        worker = asyncio.create_task(queue._run_worker())
        await asyncio.sleep(0.2)
        worker.cancel()
        try:
            await worker
        except asyncio.CancelledError:
            pass
        # The task was not collected
        collector.collect_single_channel.assert_not_called()

    async def test_worker_handles_deleted_channel(self):
        """Lines 104-115: channel deleted before collection."""
        from src.collection_queue import CollectionQueue
        from src.models import Channel, CollectionTask, CollectionTaskStatus

        channels = MagicMock()
        task = CollectionTask(
            id=1,
            channel_id=100,
            title="ch",
            status=CollectionTaskStatus.PENDING,
        )
        channels.get_collection_task = AsyncMock(return_value=task)
        channels.get_by_pk = AsyncMock(return_value=None)  # deleted
        channels.cancel_collection_task = AsyncMock()
        collector = MagicMock()

        queue = CollectionQueue(collector, channels)
        ch = Channel(id=1, channel_id=100, title="test")
        queue._queue.put_nowait((1, ch, False, True))

        worker = asyncio.create_task(queue._run_worker())
        await asyncio.sleep(0.2)
        worker.cancel()
        try:
            await worker
        except asyncio.CancelledError:
            pass
        channels.cancel_collection_task.assert_awaited_once()

    async def test_worker_handles_filtered_channel(self):
        """Lines 118-129: channel filtered before collection."""
        from src.collection_queue import CollectionQueue
        from src.models import Channel, CollectionTask, CollectionTaskStatus

        channels = MagicMock()
        task = CollectionTask(
            id=1,
            channel_id=100,
            title="ch",
            status=CollectionTaskStatus.PENDING,
        )
        channels.get_collection_task = AsyncMock(return_value=task)
        filtered_ch = Channel(
            id=1, channel_id=100, title="test", is_filtered=True
        )
        channels.get_by_pk = AsyncMock(return_value=filtered_ch)
        channels.cancel_collection_task = AsyncMock()
        collector = MagicMock()

        queue = CollectionQueue(collector, channels)
        ch = Channel(id=1, channel_id=100, title="test")
        queue._queue.put_nowait((1, ch, False, True))  # force=False

        worker = asyncio.create_task(queue._run_worker())
        await asyncio.sleep(0.2)
        worker.cancel()
        try:
            await worker
        except asyncio.CancelledError:
            pass
        channels.cancel_collection_task.assert_awaited_once()

    async def test_worker_progress_callback(self):
        """Lines 135-136: progress callback during collection."""
        from src.collection_queue import CollectionQueue
        from src.models import Channel, CollectionTask, CollectionTaskStatus

        channels = MagicMock()
        task = CollectionTask(
            id=1,
            channel_id=100,
            title="ch",
            status=CollectionTaskStatus.PENDING,
        )
        channels.get_collection_task = AsyncMock(return_value=task)
        fresh_ch = Channel(id=1, channel_id=100, title="test", is_filtered=False)
        channels.get_by_pk = AsyncMock(return_value=fresh_ch)
        channels.update_collection_task = AsyncMock()
        channels.update_collection_task_progress = AsyncMock()
        collector = MagicMock()
        collector.is_cancelled = False
        collector.collect_single_channel = AsyncMock(return_value=5)

        queue = CollectionQueue(collector, channels)
        ch = Channel(id=1, channel_id=100, title="test")
        queue._queue.put_nowait((1, ch, False, True))

        worker = asyncio.create_task(queue._run_worker())
        await asyncio.sleep(0.3)
        worker.cancel()
        try:
            await worker
        except asyncio.CancelledError:
            pass
        # Collector was called
        collector.collect_single_channel.assert_awaited_once()

    async def test_worker_cancelled_during_collection(self):
        """Lines 141-146: collector is_cancelled during collection."""
        from src.collection_queue import CollectionQueue
        from src.models import Channel, CollectionTask, CollectionTaskStatus

        channels = MagicMock()
        task = CollectionTask(
            id=1,
            channel_id=100,
            title="ch",
            status=CollectionTaskStatus.PENDING,
        )
        channels.get_collection_task = AsyncMock(return_value=task)
        fresh_ch = Channel(id=1, channel_id=100, title="test", is_filtered=False)
        channels.get_by_pk = AsyncMock(return_value=fresh_ch)
        channels.update_collection_task = AsyncMock()
        channels.cancel_collection_task = AsyncMock()
        collector = MagicMock()
        collector.is_cancelled = True
        collector.collect_single_channel = AsyncMock(return_value=0)

        queue = CollectionQueue(collector, channels)
        ch = Channel(id=1, channel_id=100, title="test")
        queue._queue.put_nowait((1, ch, False, True))

        worker = asyncio.create_task(queue._run_worker())
        await asyncio.sleep(0.3)
        worker.cancel()
        try:
            await worker
        except asyncio.CancelledError:
            pass
        channels.cancel_collection_task.assert_awaited_once()

    async def test_worker_honors_db_cancel_after_collector_clears_flag(self):
        """#633 bug #30: cancel in the task-startup window must win.

        The collector clears its in-memory cancel flag at the start of
        collection, so a cancel that arrived in the startup window leaves
        is_cancelled False. The persisted CANCELLED status must still be
        honored — the task must NOT be marked COMPLETED.
        """
        from src.collection_queue import CollectionQueue
        from src.models import Channel, CollectionTask, CollectionTaskStatus

        channels = MagicMock()
        running_task = CollectionTask(
            id=1,
            channel_id=100,
            title="ch",
            status=CollectionTaskStatus.RUNNING,
        )
        cancelled_task = CollectionTask(
            id=1,
            channel_id=100,
            title="ch",
            status=CollectionTaskStatus.CANCELLED,
        )
        # Startup check sees a live task; the post-collection re-read sees the
        # CANCELLED row written by cancel_task during the startup window.
        channels.get_collection_task = AsyncMock(
            side_effect=[running_task, cancelled_task]
        )
        fresh_ch = Channel(id=1, channel_id=100, title="test", is_filtered=False)
        channels.get_by_pk = AsyncMock(return_value=fresh_ch)
        channels.update_collection_task = AsyncMock()
        channels.cancel_collection_task = AsyncMock()
        collector = MagicMock()
        # Flag was cleared by collect_single_channel — looks "not cancelled".
        collector.is_cancelled = False
        collector.collect_single_channel = AsyncMock(return_value=7)

        queue = CollectionQueue(collector, channels)
        ch = Channel(id=1, channel_id=100, title="test")
        queue._queue.put_nowait((1, ch, False, True))

        worker = asyncio.create_task(queue._run_worker())
        await asyncio.sleep(0.3)
        worker.cancel()
        try:
            await worker
        except asyncio.CancelledError:
            pass

        # Task must be re-cancelled, never transitioned to COMPLETED.
        channels.cancel_collection_task.assert_awaited_once()
        completed_calls = [
            c
            for c in channels.update_collection_task.call_args_list
            if CollectionTaskStatus.COMPLETED in c.args
            or c.kwargs.get("status") == CollectionTaskStatus.COMPLETED
        ]
        assert completed_calls == []

    async def test_worker_does_not_requeue_user_cancel_during_shutdown(self):
        """A persisted user CANCELLED must not be reset to PENDING by shutdown."""
        from src.collection_queue import CollectionQueue
        from src.models import Channel, CollectionTask, CollectionTaskStatus

        channels = MagicMock()
        running_task = CollectionTask(
            id=1,
            channel_id=100,
            title="ch",
            status=CollectionTaskStatus.RUNNING,
        )
        cancelled_task = CollectionTask(
            id=1,
            channel_id=100,
            title="ch",
            status=CollectionTaskStatus.CANCELLED,
        )
        channels.get_collection_task = AsyncMock(side_effect=[running_task, cancelled_task])
        channels.get_by_pk = AsyncMock(return_value=Channel(id=1, channel_id=100, title="test"))
        channels.update_collection_task = AsyncMock()
        channels.cancel_collection_task = AsyncMock()
        channels.reset_collection_task_to_pending = AsyncMock()
        collector = MagicMock()
        collector.is_cancelled = False

        queue = CollectionQueue(collector, channels)

        async def collect_then_shutdown(*args, **kwargs):
            queue._shutdown_requested = True
            return 7

        collector.collect_single_channel = AsyncMock(side_effect=collect_then_shutdown)
        queue._queue.put_nowait((1, Channel(id=1, channel_id=100, title="test"), False, True))

        worker = asyncio.create_task(queue._run_worker())
        await asyncio.sleep(0.3)
        worker.cancel()
        try:
            await worker
        except asyncio.CancelledError:
            pass

        channels.cancel_collection_task.assert_awaited_once()
        channels.reset_collection_task_to_pending.assert_not_awaited()
        completed_calls = [
            c
            for c in channels.update_collection_task.call_args_list
            if CollectionTaskStatus.COMPLETED in c.args
            or c.kwargs.get("status") == CollectionTaskStatus.COMPLETED
        ]
        assert completed_calls == []

    async def test_worker_generic_exception(self):
        """Lines 174-181: generic exception during collection."""
        from src.collection_queue import CollectionQueue
        from src.models import Channel, CollectionTask, CollectionTaskStatus

        channels = MagicMock()
        task = CollectionTask(
            id=1,
            channel_id=100,
            title="ch",
            status=CollectionTaskStatus.PENDING,
        )
        channels.get_collection_task = AsyncMock(return_value=task)
        fresh_ch = Channel(id=1, channel_id=100, title="test", is_filtered=False)
        channels.get_by_pk = AsyncMock(return_value=fresh_ch)
        channels.update_collection_task = AsyncMock()
        collector = MagicMock()
        collector.collect_single_channel = AsyncMock(
            side_effect=RuntimeError("boom")
        )

        queue = CollectionQueue(collector, channels)
        ch = Channel(id=1, channel_id=100, title="test")
        queue._queue.put_nowait((1, ch, False, True))

        worker = asyncio.create_task(queue._run_worker())
        await asyncio.sleep(0.3)
        worker.cancel()
        try:
            await worker
        except asyncio.CancelledError:
            pass
        # Should have marked task as FAILED
        calls = channels.update_collection_task.call_args_list
        assert any(
            c.args[1] == CollectionTaskStatus.FAILED
            for c in calls
            if len(c.args) > 1
        )

    async def test_requeue_startup_tasks_skips_none_channel_id(self):
        """Lines 225-227: skip task with channel_id=None."""
        from src.collection_queue import CollectionQueue
        from src.models import CollectionTask, CollectionTaskStatus

        channels = MagicMock()
        channels.reset_orphaned_running_tasks = AsyncMock(return_value=0)
        task = CollectionTask(
            id=1,
            channel_id=None,
            title="ch",
            status=CollectionTaskStatus.PENDING,
        )
        channels.get_pending_channel_tasks = AsyncMock(return_value=[task])
        collector = MagicMock()

        queue = CollectionQueue(collector, channels)
        count = await queue.requeue_startup_tasks()
        assert count == 0


# ---- services/production_limits_service.py coverage ----




class TestProductionLimitsServiceCoverage:
    """Cover remaining lines in production_limits_service.py."""

    async def test_rate_limiter_minute_limit(self):
        """Lines 79-81: minute request limit reached."""
        from src.services.production_limits_service import RateLimitConfig, RateLimiter

        config = RateLimitConfig(requests_per_minute=1)
        limiter = RateLimiter(config)
        allowed, _ = await limiter.check_and_acquire(tokens=0)
        assert allowed is True
        allowed2, wait = await limiter.check_and_acquire(tokens=0)
        assert allowed2 is False
        assert wait > 0

    async def test_rate_limiter_token_limit(self):
        """Lines 83-85: minute token limit."""
        from src.services.production_limits_service import RateLimitConfig, RateLimiter

        config = RateLimitConfig(tokens_per_minute=100)
        limiter = RateLimiter(config)
        allowed, _ = await limiter.check_and_acquire(tokens=50)
        assert allowed is True
        allowed2, wait = await limiter.check_and_acquire(tokens=60)
        assert allowed2 is False

    async def test_rate_limiter_day_token_limit(self):
        """Lines 87-89: day token limit."""
        from src.services.production_limits_service import RateLimitConfig, RateLimiter

        config = RateLimitConfig(tokens_per_day=100)
        limiter = RateLimiter(config)
        allowed, _ = await limiter.check_and_acquire(tokens=50)
        assert allowed is True
        allowed2, wait = await limiter.check_and_acquire(tokens=60)
        assert allowed2 is False

    async def test_rate_limiter_image_count(self):
        """Lines 95-97: image count tracking."""
        from src.services.production_limits_service import RateLimitConfig, RateLimiter

        config = RateLimitConfig()
        limiter = RateLimiter(config)
        await limiter.check_and_acquire(tokens=0, is_image=True)
        usage = limiter.get_usage()
        assert usage["minute"]["images"] == 1
        assert usage["day"]["images"] == 1

    async def test_wait_and_acquire_timeout(self):
        """Lines 122-127: wait_and_acquire with timeout."""
        from src.services.production_limits_service import RateLimitConfig, RateLimiter

        config = RateLimitConfig(requests_per_minute=1)
        limiter = RateLimiter(config)
        await limiter.check_and_acquire(tokens=0)
        result = await limiter.wait_and_acquire(tokens=0, max_wait=0.1)
        assert result is False

    async def test_cost_tracker_estimate_image(self):
        """Line 171: cost estimation for images."""
        from src.services.production_limits_service import CostConfig, CostTracker

        config = CostConfig(cost_per_image=0.05)
        tracker = CostTracker(config)
        cost = await tracker.estimate_cost(is_image=True)
        assert cost == 0.05

    async def test_cost_tracker_check_cost_cap_exceeded(self):
        """Lines 191-192, 196-197: cost cap exceeded."""
        from src.services.production_limits_service import CostConfig, CostTracker

        config = CostConfig(daily_cost_cap=0.01)
        tracker = CostTracker(config)
        await tracker.record_cost(tokens=10000)
        allowed, _ = await tracker.check_cost_cap(tokens=10000)
        assert allowed is False

    async def test_cost_tracker_record_cost_day_reset(self):
        """Lines 205-207: day reset in record_cost."""
        from src.services.production_limits_service import CostConfig, CostTracker

        config = CostConfig()
        tracker = CostTracker(config)
        tracker._day_start = 0  # force reset
        cost = await tracker.record_cost(tokens=1000)
        assert cost > 0

    async def test_production_limits_service_execute_with_retry(self):
        """Lines 320-322: execute_with_retry exhausting retries."""
        from src.services.production_limits_service import ProductionLimitsService

        db = _make_mock_db()
        db.get_setting = AsyncMock(return_value=None)
        svc = ProductionLimitsService(db)

        call_count = 0

        async def failing_func():
            nonlocal call_count
            call_count += 1
            raise ValueError("boom")

        with pytest.raises(ValueError, match="boom"):
            await svc.execute_with_retry(
                failing_func, max_retries=1, base_delay=0.01
            )
        assert call_count == 2  # 1 initial + 1 retry


# ---- services/image_generation_service.py coverage ----




class TestImageGenerationServiceCoverage:
    """Cover remaining lines in image_generation_service.py."""

    async def test_register_from_env_together(self):
        """Lines 72-73: Together adapter registration."""
        from src.services.image_generation_service import ImageGenerationService

        with patch.dict("os.environ", {"TOGETHER_API_KEY": "test_key"}, clear=False):
            svc = ImageGenerationService()
            assert "together" in svc.adapter_names

    async def test_register_from_env_huggingface(self):
        """Lines 75-77: HuggingFace adapter registration."""
        from src.services.image_generation_service import ImageGenerationService

        with patch.dict(
            "os.environ",
            {"HUGGINGFACE_API_KEY": "test_key"},
            clear=False,
        ):
            svc = ImageGenerationService()
            assert "huggingface" in svc.adapter_names

    async def test_register_from_env_openai(self):
        """Lines 79-81: OpenAI adapter registration."""
        from src.services.image_generation_service import ImageGenerationService

        with patch.dict("os.environ", {"OPENAI_API_KEY": "test_key"}, clear=False):
            svc = ImageGenerationService()
            assert "openai" in svc.adapter_names

    async def test_register_from_env_replicate(self):
        """Lines 83-85: Replicate adapter registration."""
        from src.services.image_generation_service import ImageGenerationService

        with patch.dict("os.environ", {"REPLICATE_API_TOKEN": "test_key"}, clear=False):
            svc = ImageGenerationService()
            assert "replicate" in svc.adapter_names

    async def test_generate_adapter_timeout(self):
        """Lines 43-45: adapter timeout."""
        from src.services.image_generation_service import ImageGenerationService

        svc = ImageGenerationService()

        async def timeout_adapter(prompt, model_id):
            raise asyncio.TimeoutError()

        svc.register_adapter("test", timeout_adapter)
        result = await svc.generate("test:model", "prompt")
        assert result is None

    async def test_generate_adapter_unexpected_error(self):
        """Lines 46-48: adapter unexpected error."""
        from src.services.image_generation_service import ImageGenerationService

        svc = ImageGenerationService()

        async def error_adapter(prompt, model_id):
            raise RuntimeError("unexpected")

        svc.register_adapter("test", error_adapter)
        result = await svc.generate("test:model", "prompt")
        assert result is None

    async def test_search_models_static_catalogs(self):
        """Lines 118-145: search_models for non-replicate provider."""
        from src.services.image_generation_service import ImageGenerationService

        svc = ImageGenerationService()
        result = await svc.search_models("together")
        # Static catalogs should return list of dicts
        assert isinstance(result, list)

    async def test_no_adapter_warning(self):
        """Lines 38-40: no adapter available."""
        from src.services.image_generation_service import ImageGenerationService

        svc = ImageGenerationService()
        svc._adapters = {}
        result = await svc.generate("unknown:model", "prompt")
        assert result is None


# ---- services/provider_adapters.py coverage ----




class TestProviderAdaptersCoverage:
    """Cover remaining lines in provider_adapters.py."""

    async def test_parse_json_cohere_style(self):
        """Lines 36-40: Cohere-style response."""
        from src.services.provider_adapters import _parse_json_for_text

        data = {"generations": [{"text": "hello"}]}
        result = await _parse_json_for_text(data)
        assert result == "hello"

    async def test_parse_json_huggingface_style(self):
        """Lines 42-43: HuggingFace style."""
        from src.services.provider_adapters import _parse_json_for_text

        data = {"generated_text": "hello world"}
        result = await _parse_json_for_text(data)
        assert result == "hello world"

    async def test_parse_json_outputs_dict(self):
        """Lines 44-51: outputs style with dict items."""
        from src.services.provider_adapters import _parse_json_for_text

        data = {"outputs": [{"content": "hello"}]}
        result = await _parse_json_for_text(data)
        assert result == "hello"

    async def test_parse_json_outputs_string(self):
        """Lines 44-49: outputs style with string items."""
        from src.services.provider_adapters import _parse_json_for_text

        data = {"outputs": ["hello"]}
        result = await _parse_json_for_text(data)
        assert result == "hello"

    async def test_parse_json_result_string(self):
        """Lines 52-55: result as string."""
        from src.services.provider_adapters import _parse_json_for_text

        data = {"result": "hello"}
        result = await _parse_json_for_text(data)
        assert result == "hello"

    async def test_parse_json_result_dict(self):
        """Lines 56-60: result as dict."""
        from src.services.provider_adapters import _parse_json_for_text

        data = {"result": {"text": "hello"}}
        result = await _parse_json_for_text(data)
        assert result == "hello"

    async def test_parse_json_results_nested_content(self):
        """Lines 62-71: results with nested content dict."""
        from src.services.provider_adapters import _parse_json_for_text

        data = {"results": [{"content": {"text": "hello"}}]}
        result = await _parse_json_for_text(data)
        assert result == "hello"

    async def test_parse_json_fallback_string_field(self):
        """Lines 73-76: fallback to first string field."""
        from src.services.provider_adapters import _parse_json_for_text

        data = {"custom_field": "hello", "number": 42}
        result = await _parse_json_for_text(data)
        assert result == "hello"

    async def test_parse_json_list_input(self):
        """Lines 77-83: list input."""
        from src.services.provider_adapters import _parse_json_for_text

        data = [{"choices": [{"message": {"content": "hello"}}]}]
        result = await _parse_json_for_text(data)
        assert result == "hello"

    async def test_parse_json_non_dict_non_list(self):
        """Line 84: non-dict, non-list input."""
        from src.services.provider_adapters import _parse_json_for_text

        result = await _parse_json_for_text("plain text")
        assert result == "plain text"


# ---- services/quality_scoring_service.py coverage ----




class TestQualityScoringServiceCoverage:
    """Cover remaining lines in quality_scoring_service.py."""

    async def test_score_content_parse_json_no_braces(self):
        """Lines 101-106: JSON parsing with no braces."""
        from src.services.quality_scoring_service import QualityScoringService

        db = _make_mock_db()
        svc = QualityScoringService(db)

        mock_provider = AsyncMock(return_value="no json here")
        with patch(
            "src.services.provider_service.RuntimeProviderRegistry"
        ) as mock_aps:
            mock_aps.return_value.get_provider_callable.return_value = mock_provider
            score = await svc.score_content("test text", model="test")
            # Should get defaults when no JSON found
            assert score.relevance == 0.5

    async def test_score_content_os_error(self):
        """Lines 117-119: OSError during scoring."""
        from src.services.quality_scoring_service import QualityScoringService
        from tests.helpers import fast_llm_error_recovery

        db = _make_mock_db()
        svc = QualityScoringService(db, error_recovery=fast_llm_error_recovery())

        mock_provider = AsyncMock(side_effect=OSError("network"))
        with patch(
            "src.services.provider_service.RuntimeProviderRegistry"
        ) as mock_aps:
            mock_aps.return_value.get_provider_callable.return_value = mock_provider
            score = await svc.score_content("test text", model="test")
            assert score.overall == 0.5

    async def test_score_content_unexpected_error(self):
        """Lines 120-122: unexpected error during scoring."""
        from src.services.quality_scoring_service import QualityScoringService
        from tests.helpers import fast_llm_error_recovery

        db = _make_mock_db()
        svc = QualityScoringService(db, error_recovery=fast_llm_error_recovery())

        mock_provider = AsyncMock(side_effect=RuntimeError("unexpected"))
        with patch(
            "src.services.provider_service.RuntimeProviderRegistry"
        ) as mock_aps:
            mock_aps.return_value.get_provider_callable.return_value = mock_provider
            score = await svc.score_content("test text", model="test")
            assert score.overall == 0.5


# ---- services/photo_auto_upload_service.py coverage ----




class TestPhotoAutoUploadServiceCoverage:
    """Cover remaining lines in photo_auto_upload_service.py."""

    async def test_update_job_validates_folder(self, tmp_path):
        """Line 40-41: update_job validates folder_path."""
        from src.services.photo_auto_upload_service import PhotoAutoUploadService

        bundle = MagicMock()
        publish = MagicMock()
        svc = PhotoAutoUploadService(bundle, publish)

        with pytest.raises(ValueError, match="Folder not found"):
            await svc.update_job(1, folder_path="/nonexistent/folder")

    async def test_run_due_processes_due_jobs(self, tmp_path):
        """Lines 55-63: run_due processes due jobs."""

        from src.models import PhotoAutoUploadJob
        from src.services.photo_auto_upload_service import PhotoAutoUploadService

        bundle = MagicMock()
        job = PhotoAutoUploadJob(
            id=1,
            phone="+1",
            target_dialog_id=1,
            folder_path=str(tmp_path),
            interval_minutes=1,
            is_active=True,
            last_run_at=None,  # never run = due
        )
        bundle.list_auto_jobs = AsyncMock(return_value=[job])
        bundle.get_auto_job = AsyncMock(return_value=job)
        bundle.update_auto_job = AsyncMock()
        bundle.has_sent_auto_file = AsyncMock(return_value=False)
        publish = MagicMock()
        publish.send_now = AsyncMock()

        svc = PhotoAutoUploadService(bundle, publish)
        result = await svc.run_due()
        assert result == 1

    async def test_run_job_no_files(self, tmp_path):
        """Lines 72-74: run_job with no new files."""
        from src.models import PhotoAutoUploadJob
        from src.services.photo_auto_upload_service import PhotoAutoUploadService

        bundle = MagicMock()
        job = PhotoAutoUploadJob(
            id=1,
            phone="+1",
            target_dialog_id=1,
            folder_path=str(tmp_path),
        )
        bundle.get_auto_job = AsyncMock(return_value=job)
        bundle.has_sent_auto_file = AsyncMock(return_value=True)
        bundle.update_auto_job = AsyncMock()
        publish = MagicMock()

        svc = PhotoAutoUploadService(bundle, publish)
        count = await svc.run_job(1)
        assert count == 0

    async def test_run_job_send_failure(self, tmp_path):
        """Lines 96-107: run_job send failure."""
        from src.models import PhotoAutoUploadJob
        from src.services.photo_auto_upload_service import PhotoAutoUploadService

        # Create a test image
        img = tmp_path / "test.jpg"
        img.write_bytes(b"fake_image")

        bundle = MagicMock()
        job = PhotoAutoUploadJob(
            id=1,
            phone="+1",
            target_dialog_id=1,
            folder_path=str(tmp_path),
        )
        bundle.get_auto_job = AsyncMock(return_value=job)
        bundle.has_sent_auto_file = AsyncMock(return_value=False)
        bundle.update_auto_job = AsyncMock()
        publish = MagicMock()
        publish.send_now = AsyncMock(side_effect=RuntimeError("send failed"))

        svc = PhotoAutoUploadService(bundle, publish)
        with pytest.raises(RuntimeError, match="send failed"):
            await svc.run_job(1)
        bundle.update_auto_job.assert_awaited()

    async def test_is_due_not_active(self):
        """Line 125-126: inactive job is not due."""

        from src.models import PhotoAutoUploadJob
        from src.services.photo_auto_upload_service import PhotoAutoUploadService

        job = PhotoAutoUploadJob(
            id=1,
            phone="+1",
            target_dialog_id=1,
            folder_path="/tmp",
            is_active=False,
        )
        assert PhotoAutoUploadService._is_due(job, datetime.now(timezone.utc)) is False


# ---- services/content_generation_service.py coverage ----




class TestContentGenerationServiceCoverage:
    """Cover remaining lines in content_generation_service.py."""

    async def test_generate_set_status_fails(self, db):
        """Lines 69-71: set_status to running fails."""
        from src.models import ContentPipeline
        from src.services.content_generation_service import ContentGenerationService

        engine = MagicMock()
        svc = ContentGenerationService(db, engine)

        pipeline = ContentPipeline(
            id=1,
            name="test",
            prompt_template="write something",
        )

        # Create the run, but make set_status raise on "running"
        original_set_status = db.repos.generation_runs.set_status

        call_count = 0

        async def failing_set_status(run_id, status):
            nonlocal call_count
            call_count += 1
            if call_count == 1 and status == "running":
                raise RuntimeError("DB error")
            return await original_set_status(run_id, status)

        db.repos.generation_runs.set_status = failing_set_status
        with pytest.raises(RuntimeError, match="DB error"):
            await svc.generate(pipeline=pipeline)

    async def test_run_deep_agents_no_manager(self):
        """Line 174-175: deep_agents without manager."""
        from src.models import ContentPipeline, PipelineGenerationBackend
        from src.services.content_generation_service import ContentGenerationService

        db = _make_mock_db()
        db.repos.generation_runs.create_run = AsyncMock(return_value=1)
        db.repos.generation_runs.set_status = AsyncMock()
        engine = MagicMock()

        svc = ContentGenerationService(db, engine, agent_manager=None)

        pipeline = ContentPipeline(
            id=1,
            name="test",
            prompt_template="write",
            generation_backend=PipelineGenerationBackend.DEEP_AGENTS,
        )

        with pytest.raises(RuntimeError, match="AgentManager not configured"):
            await svc._run_deep_agents(pipeline, None, 256, 0.0)

    async def test_run_deep_agents_stream(self):
        """Lines 177-204: deep_agents streaming."""

        from src.models import ContentPipeline, PipelineGenerationBackend
        from src.services.content_generation_service import ContentGenerationService

        db = _make_mock_db()
        engine = MagicMock()

        agent_manager = MagicMock()

        async def fake_stream(*args, **kwargs):
            yield 'data: {"text": "hello"}'
            yield 'data: {"full_text": "hello world"}'
            yield "not a data line"
            yield 'data: {invalid json}'

        agent_manager.chat_stream = fake_stream

        svc = ContentGenerationService(db, engine, agent_manager=agent_manager)

        pipeline = ContentPipeline(
            id=1,
            name="test",
            prompt_template="write",
            generation_backend=PipelineGenerationBackend.DEEP_AGENTS,
        )

        result = await svc._run_deep_agents(pipeline, None, 256, 0.0)
        assert result["generated_text"] == "hello world"


# ---- services/publish_service.py coverage ----




class TestPublishServiceCoverage:
    """Cover remaining lines in publish_service.py."""

    async def test_publish_run_missing_ids(self):
        """Line 41: missing run or pipeline id."""
        from src.models import ContentPipeline, GenerationRun
        from src.services.publish_service import PublishService

        db = _make_mock_db()
        pool = MagicMock()
        svc = PublishService(db, pool)

        run = GenerationRun(id=None)
        pipeline = ContentPipeline(id=1, name="p", prompt_template="t")
        results = await svc.publish_run(run, pipeline)
        assert not results[0].success

    async def test_publish_run_no_text(self):
        """Lines 43-45: no generated text."""
        from src.models import ContentPipeline, GenerationRun, PipelinePublishMode
        from src.services.publish_service import PublishService

        db = _make_mock_db()
        pool = MagicMock()
        svc = PublishService(db, pool)

        run = GenerationRun(
            id=1,
            pipeline_id=1,
            generated_text="",
            moderation_status="approved",
        )
        pipeline = ContentPipeline(
            id=1,
            name="p",
            prompt_template="t",
            publish_mode=PipelinePublishMode.AUTO,
        )
        results = await svc.publish_run(run, pipeline)
        assert not results[0].success

    async def test_publish_run_not_approved(self):
        """Lines 47-57: moderated but not approved."""
        from src.models import ContentPipeline, GenerationRun, PipelinePublishMode
        from src.services.publish_service import PublishService

        db = _make_mock_db()
        pool = MagicMock()
        svc = PublishService(db, pool)

        run = GenerationRun(
            id=1,
            pipeline_id=1,
            generated_text="test",
            moderation_status="pending",
            status="completed",  # completed but unapproved → blocked on eligibility, not status (#1036)
        )
        pipeline = ContentPipeline(
            id=1,
            name="p",
            prompt_template="t",
            publish_mode=PipelinePublishMode.MODERATED,
        )
        results = await svc.publish_run(run, pipeline)
        assert "not approved" in results[0].error

    async def test_publish_to_target_no_client(self):
        """Lines 84-89: no client for phone."""
        from src.models import ContentPipeline, GenerationRun, PipelinePublishMode, PipelineTarget
        from src.services.publish_service import PublishService

        db = _make_mock_db()
        db.repos.content_pipelines.list_targets = AsyncMock(
            return_value=[
                PipelineTarget(
                    id=1, pipeline_id=1, phone="+1", dialog_id=123
                )
            ]
        )
        pool = MagicMock()
        pool.get_client_by_phone = AsyncMock(return_value=None)
        svc = PublishService(db, pool)

        run = GenerationRun(
            id=1,
            pipeline_id=1,
            generated_text="test",
            moderation_status="approved",
        )
        pipeline = ContentPipeline(
            id=1,
            name="p",
            prompt_template="t",
            publish_mode=PipelinePublishMode.AUTO,
        )
        results = await svc.publish_run(run, pipeline)
        assert not results[0].success


# ---- services/ab_testing_service.py coverage ----




class TestABTestingServiceCoverage:
    """Cover remaining lines in ab_testing_service.py."""

    async def test_select_variant_invalid_index(self):
        """Lines 117-120: invalid variant index."""
        from src.services.ab_testing_service import ABTestingService

        db = _make_mock_db()
        svc = ABTestingService(db)

        from src.models import GenerationRun

        run = GenerationRun(
            id=1,
            pipeline_id=1,
            variants=["v1", "v2"],
        )
        db.repos.generation_runs.get = AsyncMock(return_value=run)

        with pytest.raises(ValueError, match="Invalid variant index"):
            await svc.select_variant(1, 5)

    async def test_select_variant_no_variants(self):
        """Lines 113-117: no variants available."""
        from src.services.ab_testing_service import ABTestingService

        db = _make_mock_db()
        svc = ABTestingService(db)

        from src.models import GenerationRun

        run = GenerationRun(id=1, pipeline_id=1, variants=None)
        db.repos.generation_runs.get = AsyncMock(return_value=run)

        with pytest.raises(ValueError, match="no variants"):
            await svc.select_variant(1, 0)

    async def test_get_variants_no_data(self):
        """Lines 137-144: run with no variants data."""
        from src.services.ab_testing_service import ABTestingService

        db = _make_mock_db()
        svc = ABTestingService(db)

        from src.models import GenerationRun

        run = GenerationRun(id=1, pipeline_id=1, generated_text="hello")
        db.repos.generation_runs.get = AsyncMock(return_value=run)

        result = await svc.get_variants(1)
        assert result is not None
        assert len(result.variants) == 1
        assert result.variants[0].text == "hello"

    async def test_generate_variants_provider_error(self):
        """Lines 59-61, 82-83: variant generation error."""
        from src.models import ContentPipeline
        from src.services.ab_testing_service import ABTestingService

        db = _make_mock_db()
        svc = ABTestingService(db)
        pipeline = ContentPipeline(id=1, name="p", prompt_template="t")

        with patch(
            "src.services.provider_service.RuntimeProviderRegistry"
        ) as mock_aps:
            mock_aps.return_value.get_provider_callable.side_effect = RuntimeError("no provider")
            variants = await svc.generate_variants(pipeline, "base text", num_variants=2)
            # Should return just the base text
            assert variants == ["base text"]


# ---- services/channel_service.py coverage ----




class TestChannelServiceCoverage:
    """Cover remaining lines in channel_service.py."""

    async def test_toggle(self, db):
        """Lines 110-114: toggle channel active state."""
        from src.database.bundles import ChannelBundle
        from src.services.channel_service import ChannelService

        pool = MagicMock()
        bundle = ChannelBundle.from_database(db)
        svc = ChannelService(bundle, pool, queue=None)

        from src.models import Channel

        ch = Channel(channel_id=1, title="test")
        await bundle.add_channel(ch)
        channels = await bundle.list_channels()
        pk = channels[0].id
        await svc.toggle(pk)
        refreshed = await bundle.get_by_pk(pk)
        assert refreshed.is_active is False

    async def test_delete_with_active_tasks(self, db):
        """Lines 117-126: delete channel with active tasks."""
        from src.database.bundles import ChannelBundle
        from src.services.channel_service import ChannelService

        pool = MagicMock()
        queue = MagicMock()
        queue.cancel_task = AsyncMock()
        bundle = ChannelBundle.from_database(db)
        svc = ChannelService(bundle, pool, queue=queue)

        from src.models import Channel

        ch = Channel(channel_id=1, title="test")
        await bundle.add_channel(ch)
        channels = await bundle.list_channels()
        pk = channels[0].id
        await svc.delete(pk)
        result = await bundle.get_by_pk(pk)
        assert result is None

    async def test_refresh_channel_meta_no_channel(self, db):
        """Lines 136-138: refresh meta for nonexistent channel."""
        from src.database.bundles import ChannelBundle
        from src.services.channel_service import ChannelService

        pool = MagicMock()
        bundle = ChannelBundle.from_database(db)
        svc = ChannelService(bundle, pool, queue=None)

        result = await svc.refresh_channel_meta(9999)
        assert result is False

    async def test_refresh_channel_meta_no_meta(self, db):
        """Lines 139-141: refresh meta returns no data."""
        from src.database.bundles import ChannelBundle
        from src.services.channel_service import ChannelService

        pool = MagicMock()
        pool.fetch_channel_meta = AsyncMock(return_value=None)
        bundle = ChannelBundle.from_database(db)
        svc = ChannelService(bundle, pool, queue=None)

        from src.models import Channel

        ch = Channel(channel_id=1, title="test")
        await bundle.add_channel(ch)
        channels = await bundle.list_channels()
        pk = channels[0].id
        result = await svc.refresh_channel_meta(pk)
        assert result is False

    async def test_refresh_all_channel_meta(self, db):
        """Lines 150-160: refresh all channel meta."""
        from src.database.bundles import ChannelBundle
        from src.services.channel_service import ChannelService

        pool = MagicMock()
        pool.fetch_channel_meta = AsyncMock(
            return_value={
                "about": "test about",
                "linked_chat_id": None,
                "has_comments": False,
            }
        )
        bundle = ChannelBundle.from_database(db)
        svc = ChannelService(bundle, pool, queue=None)

        from src.models import Channel

        ch = Channel(channel_id=1, title="test")
        await bundle.add_channel(ch)
        ok, failed = await svc.refresh_all_channel_meta()
        assert ok == 1
        assert failed == 0


# ---- services/collection_service.py coverage ----




class TestCollectionServiceCoverage:
    """Cover remaining lines in collection_service.py."""

    async def test_enqueue_channel_without_queue(self, db):
        """Lines 48-59: enqueue without queue uses direct DB insert."""
        from src.database.bundles import ChannelBundle
        from src.models import Channel
        from src.services.collection_service import CollectionService

        collector = MagicMock()
        bundle = ChannelBundle.from_database(db)
        svc = CollectionService(bundle, collector, collection_queue=None)

        ch = Channel(channel_id=100, title="test")
        await bundle.add_channel(ch)
        channels = await bundle.list_channels()
        assert len(channels) == 1

        result = await svc._enqueue_channel(channels[0], force=True, full=False)
        assert result is True

    async def test_enqueue_channel_by_pk_not_found(self, db):
        """Line 64: not found."""
        from src.database.bundles import ChannelBundle
        from src.services.collection_service import CollectionService

        collector = MagicMock()
        bundle = ChannelBundle.from_database(db)
        svc = CollectionService(bundle, collector, collection_queue=None)

        result = await svc.enqueue_channel_by_pk(9999)
        assert result == "not_found"

    async def test_enqueue_channel_by_pk_filtered(self, db):
        """Lines 65-66: filtered channel."""
        from src.database.bundles import ChannelBundle
        from src.models import Channel
        from src.services.collection_service import CollectionService

        collector = MagicMock()
        bundle = ChannelBundle.from_database(db)
        svc = CollectionService(bundle, collector, collection_queue=None)

        ch = Channel(channel_id=100, title="test", is_filtered=True)
        await bundle.add_channel(ch)
        channels = await bundle.list_channels(include_filtered=True)
        pk = channels[0].id
        # Channel was inserted as is_filtered=True, but DB default might be False.
        # Mark it explicitly via DB
        await db.execute(
            "UPDATE channels SET is_filtered = 1 WHERE id = ?", (pk,)
        )

        result = await svc.enqueue_channel_by_pk(pk)
        assert result == "filtered"

    async def test_collect_all_stats(self, db):
        """Lines 93-94: collect all stats."""
        from src.database.bundles import ChannelBundle
        from src.services.collection_service import CollectionService

        collector = MagicMock()
        collector.collect_all_stats = AsyncMock()
        bundle = ChannelBundle.from_database(db)
        svc = CollectionService(bundle, collector, collection_queue=None)

        await svc.collect_all_stats()
        collector.collect_all_stats.assert_awaited_once()


# ---- services/embedding_service.py coverage ----




class TestEmbeddingServiceCoverage:
    """Cover remaining lines in embedding_service.py."""

    async def test_get_embeddings_no_vec_no_numpy(self):
        """Lines 78-82: no vec and no numpy."""
        from src.services.embedding_service import EmbeddingService

        search = MagicMock()
        search.vec_available = False
        search.numpy_available = False
        search.get_setting = AsyncMock(return_value=None)
        search.settings = MagicMock()
        search.settings.get_setting = AsyncMock(return_value=None)
        svc = EmbeddingService(search)

        with pytest.raises(RuntimeError, match="unavailable"):
            await svc._get_embeddings()

    async def test_get_embeddings_langchain_import_error(self):
        """Lines 88-90: langchain import error."""
        from src.services.embedding_service import EmbeddingService

        search = MagicMock()
        search.vec_available = True
        search.numpy_available = True
        search.get_setting = AsyncMock(return_value=None)
        search.settings = MagicMock()
        search.settings.get_setting = AsyncMock(return_value=None)
        svc = EmbeddingService(search)

        with patch.dict("sys.modules", {"langchain": None, "langchain.embeddings": None}):
            with pytest.raises((RuntimeError, ImportError)):
                await svc._get_embeddings()


# ---- services/provider_service.py coverage ----




class TestProviderServiceCoverage:
    """Cover remaining lines in provider_service.py."""

    def test_get_provider_callable_openai_model(self):
        """Lines 135-142: OpenAI model routing for GPT models."""
        from src.services.provider_service import RuntimeProviderRegistry

        db = _make_mock_db()
        db.get_setting = AsyncMock(return_value=None)

        with patch.dict("os.environ", {"OPENAI_API_KEY": "test"}, clear=False):
            svc = RuntimeProviderRegistry(db)

        func = svc.get_provider_callable("gpt-4")
        assert func is not None

    def test_get_provider_callable_unknown_fallback(self):
        """Lines 144-145: unknown provider fallback."""
        from src.services.provider_service import RuntimeProviderRegistry

        db = _make_mock_db()
        db.get_setting = AsyncMock(return_value=None)
        svc = RuntimeProviderRegistry(db)

        func = svc.get_provider_callable("nonexistent_provider")
        assert func is not None


# ---- search/local_search.py coverage ----




class TestLocalSearchCoverage:
    """Cover remaining lines in local_search.py."""

    async def test_numpy_fallback_no_vec_no_numpy(self):
        """Lines 88-91: no sqlite-vec and no numpy."""
        from src.search.local_search import LocalSearch

        search_engine = MagicMock()
        search_engine.vec_available = False
        search_engine.numpy_available = False

        embedding_svc = MagicMock()
        embedding_svc.embed_query = AsyncMock(return_value=[0.1, 0.2])
        svc = LocalSearch(search_engine, embedding_service=embedding_svc)

        with pytest.raises(RuntimeError, match="unavailable"):
            await svc.search_semantic(query="test")


# ---- search/numpy_semantic.py coverage ----




class TestNumpySemanticCoverage:
    """Cover remaining lines in numpy_semantic.py."""

    def test_load_empty(self):
        """Lines 25-28: loading empty embeddings."""
        from src.search.numpy_semantic import NumpySemanticIndex

        idx = NumpySemanticIndex()
        idx.load([])
        assert idx.size == 0

    def test_search_empty(self):
        """Lines 46-47: search on empty index."""
        from src.search.numpy_semantic import NumpySemanticIndex

        idx = NumpySemanticIndex()
        result = idx.search([0.1, 0.2])
        assert result == []

    def test_load_and_search(self):
        """Lines 30-51: load and search."""
        from src.search.numpy_semantic import NumpySemanticIndex

        idx = NumpySemanticIndex()
        embeddings = [
            (1, [1.0, 0.0, 0.0]),
            (2, [0.0, 1.0, 0.0]),
            (3, [0.0, 0.0, 1.0]),
        ]
        idx.load(embeddings)
        assert idx.size == 3

        results = idx.search([1.0, 0.0, 0.0], k=2)
        assert len(results) == 2
        assert results[0][0] == 1  # most similar


# ---- telegram/auth.py coverage ----
