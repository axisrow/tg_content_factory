"""Coverage tests for CLI commands: scheduler, notification, analytics, filter, pipeline."""

from __future__ import annotations

import argparse
import time
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

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


def _make_pool_with_clients(phones=None):
    phones = phones or ["+1111"]
    pool = MagicMock()
    pool.clients = {p: MagicMock() for p in phones}
    pool.get_native_client_by_phone = AsyncMock(return_value=None)
    pool.get_available_client = AsyncMock(return_value=None)
    pool.get_forum_topics = AsyncMock(return_value=[])
    pool.invalidate_dialogs_cache = MagicMock()
    pool.disconnect_all = AsyncMock()
    pool._dialogs_cache = {}
    pool._dialogs_cache_ttl_sec = 300
    return pool


def _get_messaging_handlers(mock_db, client_pool=None):
    """Build MCP tools and return messaging handlers keyed by name."""
    get_setting = getattr(mock_db, "get_setting", None)
    if isinstance(get_setting, AsyncMock) and get_setting.side_effect is None and isinstance(
        get_setting.return_value, (AsyncMock, MagicMock)
    ):
        get_setting.return_value = None
    captured_tools = []

    with patch(
        "src.agent.tools.create_sdk_mcp_server",
        side_effect=lambda **kw: captured_tools.extend(kw.get("tools", [])),
    ):
        from src.agent.tools import make_mcp_server
        make_mcp_server(mock_db, client_pool=client_pool)

    return {t.name: t.handler for t in captured_tools if hasattr(t, "handler")}


def _text(result) -> str:
    """Extract text from tool result payload."""
    if isinstance(result, dict):
        return result["content"][0]["text"]
    if hasattr(result, "content"):
        return result.content[0].text if hasattr(result.content[0], "text") else str(result.content[0])
    return str(result)


def _make_args(**kwargs):
    defaults = {
        "config": "config.yaml",
        "dialogs_action": "list",
        "phone": None,
        "yes": True,
    }
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


# ===========================================================================
# 1. cli/commands/test.py — read/write check functions
# ===========================================================================




class TestSchedulerSyncJobState:
    async def test_sync_job_disable(self):
        from src.scheduler.service import SchedulerManager
        mgr = SchedulerManager()
        mgr._scheduler = MagicMock()
        mgr._scheduler.running = True
        mgr._scheduler.remove_job = MagicMock()
        await mgr.sync_job_state("collect", False)
        mgr._scheduler.remove_job.assert_called_once_with("collect")

    async def test_sync_job_enable_collection(self):
        from src.scheduler.service import SchedulerManager
        mgr = SchedulerManager()
        mgr._scheduler = MagicMock()
        mgr._scheduler.running = True
        mgr._job_id = "collect_all"
        mgr._current_interval_minutes = 10
        await mgr.sync_job_state("collect_all", True)
        mgr._scheduler.add_job.assert_called_once()

    async def test_sync_job_enable_photo_due(self):
        from src.scheduler.service import SchedulerManager
        mgr = SchedulerManager()
        mgr._scheduler = MagicMock()
        mgr._scheduler.running = True
        mgr._task_enqueuer = MagicMock()
        await mgr.sync_job_state("photo_due", True)
        mgr._scheduler.add_job.assert_called_once()

    async def test_sync_job_enable_photo_auto(self):
        from src.scheduler.service import SchedulerManager
        mgr = SchedulerManager()
        mgr._scheduler = MagicMock()
        mgr._scheduler.running = True
        mgr._task_enqueuer = MagicMock()
        await mgr.sync_job_state("photo_auto", True)
        mgr._scheduler.add_job.assert_called_once()

    async def test_sync_job_enable_sq_prefix(self):
        from src.scheduler.service import SchedulerManager
        mgr = SchedulerManager()
        mgr._scheduler = MagicMock()
        mgr._scheduler.running = True
        with patch.object(mgr, "sync_search_query_jobs", new_callable=AsyncMock):
            await mgr.sync_job_state("sq_1", True)
            mgr.sync_search_query_jobs.assert_called_once()

    async def test_sync_job_enable_pipeline_prefix(self):
        from src.scheduler.service import SchedulerManager
        mgr = SchedulerManager()
        mgr._scheduler = MagicMock()
        mgr._scheduler.running = True
        with patch.object(mgr, "sync_pipeline_jobs", new_callable=AsyncMock):
            await mgr.sync_job_state("pipeline_run_1", True)
            mgr.sync_pipeline_jobs.assert_called_once()

    async def test_get_job_next_run_fallback(self):
        from src.scheduler.service import SchedulerManager
        mgr = SchedulerManager()
        job = SimpleNamespace(id="test_job", next_run_time="2025-01-01")
        mgr._scheduler = MagicMock()
        mgr._scheduler.get_job = MagicMock(side_effect=Exception("boom"))
        mgr._scheduler.get_jobs = MagicMock(return_value=[job])
        result = mgr.get_job_next_run("test_job")
        assert result == "2025-01-01"

    async def test_get_all_jobs_cache(self):
        from src.scheduler.service import SchedulerManager
        mgr = SchedulerManager()
        mgr._scheduler = MagicMock()
        mgr._scheduler.get_jobs = MagicMock(return_value=[])
        mgr._jobs_cache = {"cached": "data"}
        mgr._jobs_cache_ts = time.monotonic()
        result = mgr.get_all_jobs_next_run()
        assert result == {"cached": "data"}


# ===========================================================================
# 6. services/agent_provider_service.py — remaining paths
# ===========================================================================




class TestCLINotificationCoverage:
    """Cover remaining lines in cli/commands/notification.py."""

    def test_notification_run_setup(self, cli_env):
        """Lines 23-29: setup action."""
        import argparse

        from src.models import NotificationBot

        bot = NotificationBot(
            tg_user_id=1,
            bot_username="test_bot",
            bot_token="token",
        )

        with patch(
            "src.cli.commands.notification.runtime.init_pool",
            new=AsyncMock(return_value=(None, _make_pool_with_clients())),
        ), patch(
            "src.cli.commands.notification.NotificationService"
        ) as mock_ns:
            mock_ns.return_value.setup_bot = AsyncMock(return_value=bot)
            from src.cli.commands.notification import run

            args = argparse.Namespace(
                config="config.yaml",
                notification_action="setup",
            )
            run(args)

    def test_notification_run_status_none(self, cli_env):
        """Lines 31-38: status action with no bot."""
        import argparse

        with patch(
            "src.cli.commands.notification.runtime.init_pool",
            new=AsyncMock(return_value=(None, _make_pool_with_clients())),
        ), patch(
            "src.cli.commands.notification.NotificationService"
        ) as mock_ns:
            mock_ns.return_value.get_status = AsyncMock(return_value=None)
            from src.cli.commands.notification import run

            args = argparse.Namespace(
                config="config.yaml",
                notification_action="status",
            )
            run(args)

    def test_notification_run_delete(self, cli_env):
        """Lines 40-43: delete action."""
        import argparse

        with patch(
            "src.cli.commands.notification.runtime.init_pool",
            new=AsyncMock(return_value=(None, _make_pool_with_clients())),
        ), patch(
            "src.cli.commands.notification.NotificationService"
        ) as mock_ns:
            mock_ns.return_value.teardown_bot = AsyncMock()
            from src.cli.commands.notification import run

            args = argparse.Namespace(
                config="config.yaml",
                notification_action="delete",
            )
            run(args)

    def test_notification_run_test(self, cli_env):
        """Lines 45-48: test action."""
        import argparse

        with patch(
            "src.cli.commands.notification.runtime.init_pool",
            new=AsyncMock(return_value=(None, _make_pool_with_clients())),
        ), patch(
            "src.cli.commands.notification.NotificationService"
        ) as mock_ns:
            mock_ns.return_value.send_notification = AsyncMock()
            from src.cli.commands.notification import run

            args = argparse.Namespace(
                config="config.yaml",
                notification_action="test",
                message="hello",
            )
            run(args)


# ---- CLI analytics.py coverage ----




class TestCLIAnalyticsCoverage:
    """Cover remaining lines in cli/commands/analytics.py."""

    def test_analytics_top(self, cli_env):
        """Lines 17-34: top action."""
        import argparse

        cli_env.get_top_messages = AsyncMock(
            return_value=[
                {
                    "channel_title": "ch1",
                    "text": "hello world",
                    "date": "2026-01-01 12:00",
                    "total_reactions": 10,
                }
            ]
        )

        from src.cli.commands.analytics import run

        args = argparse.Namespace(
            config="config.yaml",
            analytics_action="top",
            limit=5,
            date_from=None,
            date_to=None,
        )
        run(args)

    def test_analytics_top_empty(self, cli_env):
        """Lines 21-23: no messages found."""
        import argparse

        cli_env.get_top_messages = AsyncMock(return_value=[])

        from src.cli.commands.analytics import run

        args = argparse.Namespace(
            config="config.yaml",
            analytics_action="top",
            limit=5,
            date_from=None,
            date_to=None,
        )
        run(args)

    def test_analytics_content_types(self, cli_env):
        """Lines 36-47: content-types action."""
        import argparse

        cli_env.get_engagement_by_media_type = AsyncMock(
            return_value=[
                {"content_type": "photo", "message_count": 10, "avg_reactions": 5.0}
            ]
        )

        from src.cli.commands.analytics import run

        args = argparse.Namespace(
            config="config.yaml",
            analytics_action="content-types",
            date_from=None,
            date_to=None,
        )
        run(args)

    def test_analytics_hourly(self, cli_env):
        """Lines 49-60: hourly action."""
        import argparse

        cli_env.get_hourly_activity = AsyncMock(
            return_value=[{"hour": 12, "message_count": 10, "avg_reactions": 2.0}]
        )

        from src.cli.commands.analytics import run

        args = argparse.Namespace(
            config="config.yaml",
            analytics_action="hourly",
            date_from=None,
            date_to=None,
        )
        run(args)

    def test_analytics_summary(self, cli_env):
        """Lines 62-72: summary action."""
        import argparse

        from src.cli.commands.analytics import run

        args = argparse.Namespace(
            config="config.yaml",
            analytics_action="summary",
            date_from=None,
            date_to=None,
        )
        run(args)


# ---- CLI filter.py coverage ----




class TestCLIFilterCoverage:
    """Cover remaining lines in cli/commands/filter.py."""

    def test_filter_analyze(self, cli_env):
        """Lines 59-91: analyze action."""
        import argparse
        from types import SimpleNamespace


        report = SimpleNamespace(
            results=[
                SimpleNamespace(
                    channel_id=1,
                    title="ch1",
                    uniqueness_pct=80.0,
                    subscriber_ratio=0.5,
                    cyrillic_pct=90.0,
                    short_msg_pct=10.0,
                    cross_dupe_pct=5.0,
                    flags=["low_uniqueness"],
                )
            ],
            total_channels=1,
            filtered_count=1,
        )

        with patch("src.cli.commands.filter.ChannelAnalyzer") as mock_analyzer:
            mock_analyzer.return_value.analyze_all = AsyncMock(return_value=report)
            mock_analyzer.return_value.apply_filters = AsyncMock(return_value=1)

            from src.cli.commands.filter import run

            args = argparse.Namespace(
                config="config.yaml",
                filter_action="analyze",
            )
            run(args)

    def test_filter_precheck(self, cli_env):
        """Lines 98-103: precheck action."""
        import argparse

        with patch("src.cli.commands.filter.ChannelAnalyzer") as mock_analyzer:
            mock_analyzer.return_value.precheck_subscriber_ratio = AsyncMock(
                return_value=3
            )

            from src.cli.commands.filter import run

            args = argparse.Namespace(
                config="config.yaml",
                filter_action="precheck",
            )
            run(args)

    def test_filter_toggle(self, cli_env):
        """Lines 105-113: toggle action."""
        import argparse

        from src.models import Channel

        ch = Channel(id=1, channel_id=100, title="test", is_filtered=False)
        cli_env.get_channel_by_pk = AsyncMock(return_value=ch)
        cli_env.set_channel_filtered = AsyncMock()

        from src.cli.commands.filter import run

        args = argparse.Namespace(
            config="config.yaml",
            filter_action="toggle",
            pk=1,
        )
        run(args)

    def test_filter_reset(self, cli_env):
        """Lines 115-117: reset action."""
        import argparse

        with patch("src.cli.commands.filter.ChannelAnalyzer") as mock_analyzer:
            mock_analyzer.return_value.reset_filters = AsyncMock()

            from src.cli.commands.filter import run

            args = argparse.Namespace(
                config="config.yaml",
                filter_action="reset",
            )
            run(args)


# ---- CLI pipeline.py coverage ----




class TestCLIPipelineCoverage:
    """Cover remaining lines in cli/commands/pipeline.py."""

    def test_pipeline_toggle(self, cli_env):
        """Lines 152-157: toggle action."""
        import argparse

        cli_env.repos.content_pipelines.get_by_id = AsyncMock(return_value=None)

        with patch("src.cli.commands.pipeline.PipelineService") as mock_ps:
            mock_ps.return_value.toggle = AsyncMock(return_value=True)

            from src.cli.commands.pipeline import run

            args = argparse.Namespace(
                config="config.yaml",
                pipeline_action="toggle",
                id=1,
            )
            run(args)

    def test_pipeline_delete(self, cli_env):
        """Lines 159-161: delete action."""
        import argparse

        with patch("src.cli.commands.pipeline.PipelineService") as mock_ps:
            mock_ps.return_value.delete = AsyncMock()

            from src.cli.commands.pipeline import run

            args = argparse.Namespace(
                config="config.yaml",
                pipeline_action="delete",
                id=1,
            )
            run(args)

    def test_pipeline_approve(self, cli_env):
        """Lines 304-310: approve action."""
        import argparse

        from src.models import GenerationRun

        run_obj = GenerationRun(id=1, pipeline_id=1)
        cli_env.repos.generation_runs.get = AsyncMock(return_value=run_obj)
        cli_env.repos.generation_runs.set_moderation_status = AsyncMock()

        from src.cli.commands.pipeline import run

        args = argparse.Namespace(
            config="config.yaml",
            pipeline_action="approve",
            run_id=1,
        )
        run(args)

    def test_pipeline_reject(self, cli_env):
        """Lines 312-318: reject action."""
        import argparse

        from src.models import GenerationRun

        run_obj = GenerationRun(id=1, pipeline_id=1)
        cli_env.repos.generation_runs.get = AsyncMock(return_value=run_obj)
        cli_env.repos.generation_runs.set_moderation_status = AsyncMock()

        from src.cli.commands.pipeline import run

        args = argparse.Namespace(
            config="config.yaml",
            pipeline_action="reject",
            run_id=1,
        )
        run(args)

    def test_pipeline_bulk_approve(self, cli_env):
        """Lines 320-329: bulk-approve action."""
        import argparse

        from src.models import GenerationRun

        run_obj = GenerationRun(id=1, pipeline_id=1)
        cli_env.repos.generation_runs.get = AsyncMock(return_value=run_obj)
        cli_env.repos.generation_runs.set_moderation_status = AsyncMock()

        from src.cli.commands.pipeline import run

        args = argparse.Namespace(
            config="config.yaml",
            pipeline_action="bulk-approve",
            run_ids=[1, 2],
        )
        run(args)

    def test_pipeline_bulk_reject(self, cli_env):
        """Lines 331-340: bulk-reject action."""
        import argparse

        from src.models import GenerationRun

        run_obj = GenerationRun(id=1, pipeline_id=1)
        cli_env.repos.generation_runs.get = AsyncMock(return_value=run_obj)
        cli_env.repos.generation_runs.set_moderation_status = AsyncMock()

        from src.cli.commands.pipeline import run

        args = argparse.Namespace(
            config="config.yaml",
            pipeline_action="bulk-reject",
            run_ids=[1, 2],
        )
        run(args)

    def test_pipeline_publish_no_run(self, cli_env):
        """Lines 343-346: publish with no run found."""
        import argparse

        cli_env.repos.generation_runs.get = AsyncMock(return_value=None)

        with patch("src.cli.commands.pipeline.PipelineService"):
            from src.cli.commands.pipeline import run

            args = argparse.Namespace(
                config="config.yaml",
                pipeline_action="publish",
                run_id=999,
            )
            run(args)

    def test_pipeline_run_show(self, cli_env):
        """Lines 261-279: run-show action."""
        import argparse

        from src.models import GenerationRun

        run_obj = GenerationRun(
            id=1,
            pipeline_id=1,
            status="completed",
            generated_text="A" * 600,
            image_url="http://img.url",
            published_at=datetime.now(timezone.utc),
        )
        cli_env.repos.generation_runs.get = AsyncMock(return_value=run_obj)

        with patch("src.cli.commands.pipeline.PipelineService"):
            from src.cli.commands.pipeline import run

            args = argparse.Namespace(
                config="config.yaml",
                pipeline_action="run-show",
                run_id=1,
            )
            run(args)

    def test_pipeline_queue_empty(self, cli_env):
        """Lines 281-302: queue action with no pending runs."""
        import argparse

        from src.models import ContentPipeline

        pipeline = ContentPipeline(id=1, name="test", prompt_template="t")

        with patch("src.cli.commands.pipeline.PipelineService") as mock_ps:
            mock_ps.return_value.get = AsyncMock(return_value=pipeline)
            cli_env.repos.generation_runs.list_pending_moderation = AsyncMock(
                return_value=[]
            )

            from src.cli.commands.pipeline import run

            args = argparse.Namespace(
                config="config.yaml",
                pipeline_action="queue",
                id=1,
                limit=10,
            )
            run(args)

    def test_pipeline_edit_validation_error(self, cli_env):
        """Lines 144-146: edit with validation error."""
        import argparse

        from src.models import ContentPipeline
        from src.services.pipeline_service import PipelineValidationError

        existing = ContentPipeline(
            id=1,
            name="test",
            prompt_template="t",
            generate_interval_minutes=60,
            is_active=True,
        )

        with patch("src.cli.commands.pipeline.PipelineService") as mock_ps:
            mock_ps.return_value.get = AsyncMock(return_value=existing)
            mock_ps.return_value.update = AsyncMock(
                side_effect=PipelineValidationError("bad")
            )

            from src.cli.commands.pipeline import run

            mock_ps.return_value.get_sources = AsyncMock(return_value=[])
            mock_ps.return_value.get_targets = AsyncMock(return_value=[])


            args = argparse.Namespace(
                config="config.yaml",
                pipeline_action="edit",
                id=1,
                name=None,
                prompt_template=None,
                source=None,
                target=None,
                llm_model=None,
                image_model=None,
                publish_mode=None,
                generation_backend=None,
                interval=None,
                active=None,
            )
            run(args)


# ---------------------------------------------------------------------------
# === COVERAGE PUSH BATCH 3 ===
# Target: push remaining 3 modules (cli, telegram, web) to 90%+
# ---------------------------------------------------------------------------


# ---- CLI analytics.py additional coverage ----




class TestCLIAnalyticsBatch3:
    """Cover remaining CLI analytics lines."""

    def test_analytics_trending_topics(self, cli_env):
        """Lines 112-126: trending-topics action."""
        import argparse

        with patch("src.services.trend_service.TrendService") as mock_ts:
            from types import SimpleNamespace

            mock_ts.return_value.get_trending_topics = AsyncMock(
                return_value=[SimpleNamespace(keyword="test", count=5)]
            )
            from src.cli.commands.analytics import run

            args = argparse.Namespace(
                config="config.yaml",
                analytics_action="trending-topics",
                date_from=None,
                date_to=None,
                days=7,
                limit=20,
            )
            run(args)

    def test_analytics_trending_channels(self, cli_env):
        """Lines 128-142: trending-channels action."""
        import argparse

        with patch("src.services.trend_service.TrendService") as mock_ts:
            from types import SimpleNamespace

            mock_ts.return_value.get_trending_channels = AsyncMock(
                return_value=[SimpleNamespace(title="ch1", count=10)]
            )
            from src.cli.commands.analytics import run

            args = argparse.Namespace(
                config="config.yaml",
                analytics_action="trending-channels",
                date_from=None,
                date_to=None,
                days=7,
                limit=20,
            )
            run(args)

    def test_analytics_velocity(self, cli_env):
        """Lines 144-157: velocity action."""
        import argparse

        with patch("src.services.trend_service.TrendService") as mock_ts:
            from types import SimpleNamespace

            mock_ts.return_value.get_message_velocity = AsyncMock(
                return_value=[SimpleNamespace(date="2026-01-01", count=50)]
            )
            from src.cli.commands.analytics import run

            args = argparse.Namespace(
                config="config.yaml",
                analytics_action="velocity",
                date_from=None,
                date_to=None,
                days=30,
            )
            run(args)

    def test_analytics_peak_hours(self, cli_env):
        """Lines 159-171: peak-hours action."""
        import argparse

        with patch("src.services.trend_service.TrendService") as mock_ts:
            from types import SimpleNamespace

            mock_ts.return_value.get_peak_hours = AsyncMock(
                return_value=[SimpleNamespace(hour=12, count=100)]
            )
            from src.cli.commands.analytics import run

            args = argparse.Namespace(
                config="config.yaml",
                analytics_action="peak-hours",
                date_from=None,
                date_to=None,
            )
            run(args)

    def test_analytics_calendar(self, cli_env):
        """Lines 173-194: calendar action."""
        import argparse

        with patch(
            "src.services.content_calendar_service.ContentCalendarService"
        ) as mock_cs:
            from types import SimpleNamespace

            mock_cs.return_value.get_upcoming = AsyncMock(
                return_value=[
                    SimpleNamespace(
                        run_id=1,
                        pipeline_name="test",
                        moderation_status="pending",
                        scheduled_time=None,
                        created_at=datetime.now(timezone.utc),
                        preview="some preview text",
                    )
                ]
            )
            from src.cli.commands.analytics import run

            args = argparse.Namespace(
                config="config.yaml",
                analytics_action="calendar",
                date_from=None,
                date_to=None,
                limit=20,
                pipeline_id=None,
            )
            run(args)


# ---- CLI notification dry-run coverage ----




class TestCLINotificationDryRunBatch3:
    """Cover notification dry-run action."""

    def test_notification_dry_run_no_queries(self, cli_env):
        """Lines 50-70: dry-run with no queries."""
        import argparse

        cli_env.get_notification_queries = AsyncMock(return_value=[])

        with patch(
            "src.cli.commands.notification.runtime.init_pool",
            new=AsyncMock(return_value=(None, _make_pool_with_clients())),
        ), patch(
            "src.cli.commands.notification.NotificationService"
        ):
            from src.cli.commands.notification import run

            args = argparse.Namespace(
                config="config.yaml",
                notification_action="dry-run",
            )
            run(args)

    def test_notification_dry_run_with_queries(self, cli_env):
        """Lines 71-85: dry-run with queries and matches."""
        import argparse

        from src.models import SearchQuery

        sq = SearchQuery(id=1, query="test", notify_on_collect=True)
        cli_env.get_notification_queries = AsyncMock(return_value=[sq])
        cli_env.repos.tasks.get_last_completed_collect_task = AsyncMock(
            return_value=None
        )
        cli_env.repos.settings.get_setting = AsyncMock(return_value=None)

        with patch(
            "src.cli.commands.notification.runtime.init_pool",
            new=AsyncMock(return_value=(None, _make_pool_with_clients())),
        ), patch(
            "src.cli.commands.notification.NotificationService"
        ):
            from src.cli.commands.notification import run

            args = argparse.Namespace(
                config="config.yaml",
                notification_action="dry-run",
            )
            run(args)

    def test_notification_status_with_bot(self, cli_env):
        """Lines 31-38: status action with bot."""
        import argparse

        from src.models import NotificationBot

        bot = NotificationBot(
            tg_user_id=1,
            bot_username="test_bot",
            bot_token="token",
            bot_id=123,
        )

        with patch(
            "src.cli.commands.notification.runtime.init_pool",
            new=AsyncMock(return_value=(None, _make_pool_with_clients())),
        ), patch(
            "src.cli.commands.notification.NotificationService"
        ) as mock_ns:
            mock_ns.return_value.get_status = AsyncMock(return_value=bot)
            from src.cli.commands.notification import run

            args = argparse.Namespace(
                config="config.yaml",
                notification_action="status",
            )
            run(args)


# ---- telegram/auth.py additional coverage ----


@pytest.mark.native_backend_allowed


class TestCLIPipelineBatch3:
    """Cover more pipeline CLI actions."""

    def test_pipeline_runs_with_status_filter(self, cli_env):
        """Lines 250-259: runs with status filter."""
        import argparse

        from src.models import ContentPipeline, GenerationRun

        pipeline = ContentPipeline(id=1, name="test", prompt_template="t")
        run_obj = GenerationRun(
            id=1,
            pipeline_id=1,
            status="completed",
            moderation_status="approved",
            created_at=datetime.now(timezone.utc),
        )

        with patch("src.cli.commands.pipeline.PipelineService") as mock_ps:
            mock_ps.return_value.get = AsyncMock(return_value=pipeline)
            cli_env.repos.generation_runs.list_by_pipeline = AsyncMock(
                return_value=[run_obj]
            )

            from src.cli.commands.pipeline import run

            args = argparse.Namespace(
                config="config.yaml",
                pipeline_action="runs",
                id=1,
                limit=10,
                status="completed",
            )
            run(args)

    def test_pipeline_queue_with_pending_runs(self, cli_env):
        """Lines 293-302: queue with pending runs."""
        import argparse

        from src.models import ContentPipeline, GenerationRun

        pipeline = ContentPipeline(id=1, name="test", prompt_template="t")
        run_obj = GenerationRun(
            id=1,
            pipeline_id=1,
            moderation_status="pending",
            created_at=datetime.now(timezone.utc),
            generated_text="Preview text",
        )

        with patch("src.cli.commands.pipeline.PipelineService") as mock_ps:
            mock_ps.return_value.get = AsyncMock(return_value=pipeline)
            cli_env.repos.generation_runs.list_pending_moderation = AsyncMock(
                return_value=[run_obj]
            )

            from src.cli.commands.pipeline import run

            args = argparse.Namespace(
                config="config.yaml",
                pipeline_action="queue",
                id=1,
                limit=10,
            )
            run(args)

    def test_pipeline_bulk_approve_missing_run(self, cli_env):
        """Lines 324-326: bulk-approve with missing run."""
        import argparse

        cli_env.repos.generation_runs.get = AsyncMock(return_value=None)
        cli_env.repos.generation_runs.set_moderation_status = AsyncMock()

        from src.cli.commands.pipeline import run

        args = argparse.Namespace(
            config="config.yaml",
            pipeline_action="bulk-approve",
            run_ids=[999],
        )
        run(args)

    def test_pipeline_bulk_reject_missing_run(self, cli_env):
        """Lines 335-337: bulk-reject with missing run."""
        import argparse

        cli_env.repos.generation_runs.get = AsyncMock(return_value=None)
        cli_env.repos.generation_runs.set_moderation_status = AsyncMock()

        from src.cli.commands.pipeline import run

        args = argparse.Namespace(
            config="config.yaml",
            pipeline_action="bulk-reject",
            run_ids=[999],
        )
        run(args)

    def test_pipeline_publish_no_pipeline_id(self, cli_env):
        """Lines 347-349: publish run with no pipeline_id."""
        import argparse

        from src.models import GenerationRun

        run_obj = GenerationRun(id=1, pipeline_id=None)
        cli_env.repos.generation_runs.get = AsyncMock(return_value=run_obj)

        with patch("src.cli.commands.pipeline.PipelineService"):
            from src.cli.commands.pipeline import run

            args = argparse.Namespace(
                config="config.yaml",
                pipeline_action="publish",
                run_id=1,
            )
            run(args)

    def test_pipeline_publish_no_pipeline(self, cli_env):
        """Lines 352-354: publish run with missing pipeline."""
        import argparse

        from src.models import GenerationRun

        run_obj = GenerationRun(id=1, pipeline_id=99)
        cli_env.repos.generation_runs.get = AsyncMock(return_value=run_obj)

        with patch("src.cli.commands.pipeline.PipelineService") as mock_ps:
            mock_ps.return_value.get = AsyncMock(return_value=None)
            from src.cli.commands.pipeline import run

            args = argparse.Namespace(
                config="config.yaml",
                pipeline_action="publish",
                run_id=1,
            )
            run(args)


# ---- web/session.py coverage ----


