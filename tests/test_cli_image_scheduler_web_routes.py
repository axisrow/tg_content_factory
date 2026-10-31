"""Tests for CLI image, settings, analytics, scheduler, and related web routes.

Covers:
- CLI: image (generate/models/providers)
- CLI: settings (get/set/info)
- CLI: analytics (summary/pipeline-stats/daily/trending-topics/trending-channels/velocity/peak-hours/calendar)
- CLI: scheduler (status/stop/job-toggle/set-interval/task-cancel/clear-pending)
- Web: /images routes (page/generate/models/search)
- Web: /accounts routes (flood-status/flood-clear/toggle/delete)
- Web: /calendar routes (page/api/calendar/api/upcoming/api/stats)
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tests.helpers import cli_ns as _ns
from tg_shared.auth import TelegramAuth

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _run_ns(**kwargs):
    defaults = {"config": "config.yaml"}
    defaults.update(kwargs)
    return _ns(**defaults)


# ---------------------------------------------------------------------------
# CLI: image command
# ---------------------------------------------------------------------------


class TestCLIImageCommand:
    """Tests for src/cli/commands/image.py"""

    def test_providers_none_configured(self, capsys):
        """providers action with no adapters prints help message."""
        from tg_agent.cli.commands.image import run

        with patch("tg_agent.cli.commands.image.ImageGenerationService") as mock_svc_cls:
            svc = MagicMock()
            svc.adapter_names = []
            mock_svc_cls.return_value = svc

            run(_run_ns(image_action="providers"))
            out = capsys.readouterr().out
            assert "No providers configured" in out

    def test_providers_lists_names(self, capsys):
        """providers action prints each adapter name."""
        from tg_agent.cli.commands.image import run

        with patch("tg_agent.cli.commands.image.ImageGenerationService") as mock_svc_cls:
            svc = MagicMock()
            svc.adapter_names = ["replicate", "together"]
            mock_svc_cls.return_value = svc

            run(_run_ns(image_action="providers"))
            out = capsys.readouterr().out
            assert "replicate" in out
            assert "together" in out

    def test_generate_no_providers(self, capsys):
        """generate action when no providers configured prints warning."""
        from tg_agent.cli.commands.image import run

        with patch("tg_agent.cli.commands.image.ImageGenerationService") as mock_svc_cls:
            svc = MagicMock()
            svc.is_available = AsyncMock(return_value=False)
            mock_svc_cls.return_value = svc

            run(_run_ns(image_action="generate", model=None, prompt="a cat"))
            out = capsys.readouterr().out
            assert "No image providers configured" in out

    def test_generate_success(self, capsys):
        """generate action prints result URL on success."""
        from tg_agent.cli.commands.image import run

        with patch("tg_agent.cli.commands.image.ImageGenerationService") as mock_svc_cls:
            svc = MagicMock()
            svc.is_available = AsyncMock(return_value=True)
            svc.generate = AsyncMock(return_value="https://example.com/img.png")
            mock_svc_cls.return_value = svc

            run(_run_ns(image_action="generate", model="replicate:flux", prompt="a cat"))
            out = capsys.readouterr().out
            assert "https://example.com/img.png" in out

    def test_generate_failure(self, capsys):
        """generate action prints failure message when result is None."""
        from tg_agent.cli.commands.image import run

        with patch("tg_agent.cli.commands.image.ImageGenerationService") as mock_svc_cls:
            svc = MagicMock()
            svc.is_available = AsyncMock(return_value=True)
            svc.generate = AsyncMock(return_value=None)
            svc.last_failure = None
            mock_svc_cls.return_value = svc

            run(_run_ns(image_action="generate", model=None, prompt="a cat"))
            out = capsys.readouterr().out
            assert "Generation failed" in out

    def test_generate_timeout_failure(self, capsys):
        """generate action prints actionable timeout message when the service reports timeout."""
        from tg_agent.cli.commands.image import run
        from tg_agent.services.image_generation_service import ImageGenerationFailure

        with patch("tg_agent.cli.commands.image.ImageGenerationService") as mock_svc_cls:
            svc = MagicMock()
            svc.is_available = AsyncMock(return_value=True)
            svc.generate = AsyncMock(return_value=None)
            svc.last_failure = ImageGenerationFailure(
                kind="timeout", provider="codex", model="codex:gpt-5.4", message="timed out"
            )
            mock_svc_cls.return_value = svc

            run(_run_ns(image_action="generate", model="codex:gpt-5.4", prompt="a cat"))
            out = capsys.readouterr().out
            assert "Generation timed out for model=codex:gpt-5.4 after 180s" in out
            assert "choose another image provider/model" in out

    def test_models_no_results(self, capsys):
        """models action with no results prints message."""
        from tg_agent.cli.commands.image import run

        with patch("tg_agent.cli.commands.image.ImageGenerationService") as mock_svc_cls:
            svc = MagicMock()
            svc.search_models = AsyncMock(return_value=[])
            mock_svc_cls.return_value = svc

            run(_run_ns(image_action="models", provider="replicate", query=""))
            out = capsys.readouterr().out
            assert "No models found" in out

    def test_models_with_results(self, capsys):
        """models action prints model strings and descriptions."""
        from tg_agent.cli.commands.image import run

        with patch("tg_agent.cli.commands.image.ImageGenerationService") as mock_svc_cls:
            svc = MagicMock()
            svc.search_models = AsyncMock(return_value=[
                {"model_string": "replicate:flux", "run_count": 1000, "description": "Fast model"},
                {"model_string": "replicate:sdxl", "run_count": None, "description": ""},
            ])
            mock_svc_cls.return_value = svc

            run(_run_ns(image_action="models", provider="replicate", query="flux"))
            out = capsys.readouterr().out
            assert "replicate:flux" in out
            assert "Fast model" in out

    def test_unknown_action(self, capsys):
        """Unknown action prints usage help."""
        from tg_agent.cli.commands.image import run

        with patch("tg_agent.cli.commands.image.ImageGenerationService") as mock_svc_cls:
            svc = MagicMock()
            mock_svc_cls.return_value = svc

            run(_run_ns(image_action="unknown"))
            out = capsys.readouterr().out
            assert "Usage" in out


# ---------------------------------------------------------------------------
# CLI: settings command
# ---------------------------------------------------------------------------


class TestCLISettingsCommand:
    """Tests for src/cli/commands/settings.py"""

    def test_get_all_empty(self, cli_env, capsys):
        """get action with no key lists whatever settings exist (at least header)."""
        from tg_agent.cli.commands.settings import run

        run(_ns(settings_action="get", key=None))
        out = capsys.readouterr().out
        # Either shows "No settings found" or a table header — either way no crash
        assert out.strip() != ""

    def test_get_all_with_data(self, cli_env, capsys):
        """get action with no key lists all settings."""
        asyncio.run(cli_env.set_setting("test_key", "test_value"))

        from tg_agent.cli.commands.settings import run

        run(_ns(settings_action="get", key=None))
        out = capsys.readouterr().out
        assert "test_key" in out
        assert "test_value" in out

    def test_get_specific_key(self, cli_env, capsys):
        """get action with specific key prints value."""
        asyncio.run(cli_env.set_setting("my_key", "my_value"))

        from tg_agent.cli.commands.settings import run

        run(_ns(settings_action="get", key="my_key"))
        out = capsys.readouterr().out
        assert "my_key" in out
        assert "my_value" in out

    def test_get_missing_key(self, cli_env, capsys):
        """get action with missing key prints '(not set)'."""
        from tg_agent.cli.commands.settings import run

        run(_ns(settings_action="get", key="nonexistent_key"))
        out = capsys.readouterr().out
        assert "(not set)" in out

    def test_set_setting(self, cli_env, capsys):
        """set action prints confirmation (DB access happens inside the CLI run)."""
        from tg_agent.cli.commands.settings import run

        run(_ns(settings_action="set", key="foo", value="bar"))
        out = capsys.readouterr().out
        assert "Set foo = bar" in out

    def test_info_action(self, cli_env, capsys):
        """info action prints system information."""
        from tg_agent.cli.commands.settings import run

        run(_ns(settings_action="info"))
        out = capsys.readouterr().out
        assert "System information" in out


# ---------------------------------------------------------------------------
# CLI: analytics extended commands
# ---------------------------------------------------------------------------


class TestCLIAnalyticsExtended:
    """Tests for analytics sub-commands not covered by test_cli_analytics.py"""

    def test_summary_empty(self, cli_env, capsys):
        """summary action works with empty DB."""
        from tg_agent.cli.commands.analytics import run

        run(_ns(analytics_action="summary", date_from=None, date_to=None))
        out = capsys.readouterr().out
        assert "Total generations" in out

    def test_pipeline_stats_empty(self, cli_env, capsys):
        """pipeline-stats action with no data prints message."""
        from tg_agent.cli.commands.analytics import run

        run(_ns(analytics_action="pipeline-stats", date_from=None, date_to=None, pipeline_id=None))
        out = capsys.readouterr().out
        assert "No pipeline stats found" in out

    def test_daily_empty(self, cli_env, capsys):
        """daily action with no data prints 'No data'."""
        from tg_agent.cli.commands.analytics import run

        _target = "tg_agent.services.content_analytics_service.ContentAnalyticsService.get_daily_stats"
        with patch(_target, new=AsyncMock(return_value=[])):
            run(_ns(analytics_action="daily", date_from=None, date_to=None, days=30, pipeline_id=None))
        out = capsys.readouterr().out
        assert "No data" in out

    def test_trending_topics_empty(self, cli_env, capsys):
        """trending-topics action with no data prints message."""
        from tg_agent.cli.commands.analytics import run

        run(_ns(analytics_action="trending-topics", date_from=None, date_to=None, days=7, limit=20))
        out = capsys.readouterr().out
        assert "No trending topics found" in out

    def test_trending_channels_empty(self, cli_env, capsys):
        """trending-channels action with no data prints message."""
        from tg_agent.cli.commands.analytics import run

        run(_ns(analytics_action="trending-channels", date_from=None, date_to=None, days=7, limit=20))
        out = capsys.readouterr().out
        assert "No channel data found" in out

    def test_velocity_empty(self, cli_env, capsys):
        """velocity action with no data prints message."""
        from tg_agent.cli.commands.analytics import run

        with patch("tg_agent.services.trend_service.TrendService.get_message_velocity", new=AsyncMock(return_value=[])):
            run(_ns(analytics_action="velocity", date_from=None, date_to=None, days=30))
        out = capsys.readouterr().out
        assert "No velocity data found" in out

    def test_peak_hours_empty(self, cli_env, capsys):
        """peak-hours action with no data prints message."""
        from tg_agent.cli.commands.analytics import run

        with patch("tg_agent.services.trend_service.TrendService.get_peak_hours", new=AsyncMock(return_value=[])):
            run(_ns(analytics_action="peak-hours", date_from=None, date_to=None))
        out = capsys.readouterr().out
        assert "No peak hours data found" in out

    def test_calendar_empty(self, cli_env, capsys):
        """calendar action with no upcoming publications prints message."""
        from tg_agent.cli.commands.analytics import run

        run(_ns(analytics_action="calendar", date_from=None, date_to=None, limit=20, pipeline_id=None))
        out = capsys.readouterr().out
        assert "No upcoming publications" in out


# ---------------------------------------------------------------------------
# CLI: scheduler command (actions that don't need a real pool)
# ---------------------------------------------------------------------------


@pytest.fixture
def cli_env_scheduler(cli_env):
    """Patch runtime.init_pool to return an empty fake pool."""
    fake_pool = MagicMock()
    fake_pool.clients = {}
    fake_pool.disconnect_all = AsyncMock()

    async def fake_init_pool(config, db):
        return TelegramAuth(0, ""), fake_pool

    with patch("tg_agent.cli.runtime.init_pool", side_effect=fake_init_pool):
        yield cli_env, fake_pool


class TestCLISchedulerCommand:
    """Tests for scheduler sub-commands (status/stop/job-toggle/set-interval/task-cancel/clear-pending)."""

    def test_status_prints_interval(self, cli_env_scheduler, capsys):
        """status action prints interval and autostart."""
        cli_env, fake_pool = cli_env_scheduler
        fake_pool.clients = {"dummy": MagicMock()}

        from tg_agent.cli.commands.scheduler import run

        run(_ns(scheduler_action="status"))
        out = capsys.readouterr().out
        assert "Interval" in out

    def test_stop_disables_autostart(self, cli_env_scheduler, capsys):
        """stop action prints autostart-disabled message."""
        cli_env, fake_pool = cli_env_scheduler
        fake_pool.clients = {"dummy": MagicMock()}

        from tg_agent.cli.commands.scheduler import run

        run(_ns(scheduler_action="stop"))
        out = capsys.readouterr().out
        assert "Scheduler autostart disabled" in out

    def test_job_toggle_disables(self, cli_env_scheduler, capsys):
        """job-toggle disables an enabled job."""
        cli_env, fake_pool = cli_env_scheduler
        fake_pool.clients = {"dummy": MagicMock()}

        from tg_agent.cli.commands.scheduler import run

        run(_ns(scheduler_action="job-toggle", job_id="collect_all"))
        out = capsys.readouterr().out
        assert "collect_all" in out

    def test_job_toggle_enables(self, cli_env_scheduler, capsys):
        """job-toggle re-enables a disabled job."""
        cli_env, fake_pool = cli_env_scheduler
        fake_pool.clients = {"dummy": MagicMock()}
        asyncio.run(cli_env.repos.settings.set_setting("scheduler_job_disabled:collect_all", "1"))

        from tg_agent.cli.commands.scheduler import run

        run(_ns(scheduler_action="job-toggle", job_id="collect_all"))
        out = capsys.readouterr().out
        assert "enabled" in out

    def test_set_interval_collect_all(self, cli_env_scheduler, capsys):
        """set-interval for collect_all prints confirmation."""
        cli_env, fake_pool = cli_env_scheduler
        fake_pool.clients = {"dummy": MagicMock()}

        from tg_agent.cli.commands.scheduler import run

        run(_ns(scheduler_action="set-interval", job_id="collect_all", minutes=45))
        out = capsys.readouterr().out
        assert "45" in out

    def test_set_interval_clamps_min(self, cli_env_scheduler, capsys):
        """set-interval clamps below-minimum value to 1. Uses a real job id (collect_all):
        an unknown job_id now correctly prints 'Unknown job_id' and returns (#835/9), so the
        clamp must be asserted on a job the command actually accepts."""
        cli_env, fake_pool = cli_env_scheduler
        fake_pool.clients = {"dummy": MagicMock()}

        from tg_agent.cli.commands.scheduler import run

        run(_ns(scheduler_action="set-interval", job_id="collect_all", minutes=0))
        out = capsys.readouterr().out
        assert "1 min" in out

    def test_task_cancel_not_found(self, cli_env_scheduler, capsys):
        """task-cancel on missing task prints not-found message."""
        cli_env, fake_pool = cli_env_scheduler
        fake_pool.clients = {"dummy": MagicMock()}

        from tg_agent.cli.commands.scheduler import run

        run(_ns(scheduler_action="task-cancel", task_id=99999))
        out = capsys.readouterr().out
        assert "not found" in out or "Task 99999" in out

    def test_clear_pending(self, cli_env_scheduler, capsys):
        """clear-pending prints deleted count."""
        cli_env, fake_pool = cli_env_scheduler
        fake_pool.clients = {"dummy": MagicMock()}

        from tg_agent.cli.commands.scheduler import run

        run(_ns(scheduler_action="clear-pending"))
        out = capsys.readouterr().out
        assert "Cleared" in out
