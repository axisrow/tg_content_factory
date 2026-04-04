"""Tests for CLI settings commands: agent, filter-criteria, semantic."""
from __future__ import annotations

from unittest.mock import patch

import pytest

from src.config import AppConfig
from src.database import Database
from tests.helpers import cli_ns as _ns


@pytest.fixture
def cli_env(cli_db):
    config = AppConfig()

    async def fake_init_db(config_path: str):
        cmd_db = Database(cli_db._db_path)
        await cmd_db.initialize()
        return config, cmd_db

    with patch("src.cli.commands.settings.runtime.init_db", side_effect=fake_init_db):
        yield cli_db


class TestAgent:
    def test_agent_show_defaults(self, cli_env, capsys):
        from src.cli.commands.settings import run
        run(_ns(settings_action="agent"))
        out = capsys.readouterr().out
        assert "agent_backend" in out
        assert "agent_default_prompt_template" in out
        assert "(not set)" in out

    def test_agent_set_backend(self, cli_env, capsys):
        from src.cli.commands.settings import run
        run(_ns(settings_action="agent", backend="claude-agent-sdk"))
        out = capsys.readouterr().out
        assert "agent_backend = claude-agent-sdk" in out

    def test_agent_set_prompt_template(self, cli_env, capsys):
        from src.cli.commands.settings import run
        run(_ns(settings_action="agent", prompt_template="You are a helpful assistant."))
        out = capsys.readouterr().out
        assert "agent_default_prompt_template" in out

    def test_agent_set_both(self, cli_env, capsys):
        from src.cli.commands.settings import run
        run(_ns(settings_action="agent", backend="deepagents", prompt_template="Be concise."))
        out = capsys.readouterr().out
        assert "agent_backend = deepagents" in out
        assert "agent_default_prompt_template" in out

    def test_agent_show_after_set(self, cli_env, capsys):
        from src.cli.commands.settings import run
        run(_ns(settings_action="agent", backend="claude-agent-sdk"))
        capsys.readouterr()
        run(_ns(settings_action="agent"))
        out = capsys.readouterr().out
        assert "claude-agent-sdk" in out


class TestFilterCriteria:
    def test_filter_criteria_show_defaults(self, cli_env, capsys):
        from src.cli.commands.settings import run
        run(_ns(settings_action="filter-criteria"))
        out = capsys.readouterr().out
        assert "filter_min_uniqueness" in out
        assert "filter_min_subscriber_ratio" in out
        assert "filter_max_cross_dupe_pct" in out
        assert "filter_min_cyrillic_pct" in out

    def test_filter_criteria_set_min_uniqueness(self, cli_env, capsys):
        from src.cli.commands.settings import run
        run(_ns(settings_action="filter-criteria", min_uniqueness=0.8))
        out = capsys.readouterr().out
        assert "filter_min_uniqueness = 0.8" in out

    def test_filter_criteria_set_min_sub_ratio(self, cli_env, capsys):
        from src.cli.commands.settings import run
        run(_ns(settings_action="filter-criteria", min_sub_ratio=0.5))
        out = capsys.readouterr().out
        assert "filter_min_subscriber_ratio = 0.5" in out

    def test_filter_criteria_set_max_cross_dupe(self, cli_env, capsys):
        from src.cli.commands.settings import run
        run(_ns(settings_action="filter-criteria", max_cross_dupe=30))
        out = capsys.readouterr().out
        assert "filter_max_cross_dupe_pct = 30" in out

    def test_filter_criteria_set_min_cyrillic(self, cli_env, capsys):
        from src.cli.commands.settings import run
        run(_ns(settings_action="filter-criteria", min_cyrillic=60))
        out = capsys.readouterr().out
        assert "filter_min_cyrillic_pct = 60" in out

    def test_filter_criteria_set_multiple(self, cli_env, capsys):
        from src.cli.commands.settings import run
        run(_ns(settings_action="filter-criteria", min_uniqueness=0.9, min_cyrillic=70))
        out = capsys.readouterr().out
        assert "filter_min_uniqueness = 0.9" in out
        assert "filter_min_cyrillic_pct = 70" in out

    def test_filter_criteria_show_after_set(self, cli_env, capsys):
        from src.cli.commands.settings import run
        run(_ns(settings_action="filter-criteria", min_uniqueness=0.75))
        capsys.readouterr()
        run(_ns(settings_action="filter-criteria"))
        out = capsys.readouterr().out
        assert "0.75" in out


class TestSemantic:
    def test_semantic_show_defaults(self, cli_env, capsys):
        from src.cli.commands.settings import run
        run(_ns(settings_action="semantic"))
        out = capsys.readouterr().out
        assert "semantic_provider" in out
        assert "semantic_model" in out
        assert "semantic_api_key" in out

    def test_semantic_set_provider(self, cli_env, capsys):
        from src.cli.commands.settings import run
        run(_ns(settings_action="semantic", provider="openai"))
        out = capsys.readouterr().out
        assert "semantic_provider = openai" in out

    def test_semantic_set_model(self, cli_env, capsys):
        from src.cli.commands.settings import run
        run(_ns(settings_action="semantic", model="text-embedding-3-small"))
        out = capsys.readouterr().out
        assert "semantic_model = text-embedding-3-small" in out

    def test_semantic_set_api_key_short(self, cli_env, capsys):
        from src.cli.commands.settings import run
        run(_ns(settings_action="semantic", api_key="short"))
        out = capsys.readouterr().out
        assert "semantic_api_key = short" in out

    def test_semantic_set_api_key_long_truncates(self, cli_env, capsys):
        from src.cli.commands.settings import run
        long_key = "sk-" + "a" * 30
        run(_ns(settings_action="semantic", api_key=long_key))
        out = capsys.readouterr().out
        assert "..." in out

    def test_semantic_set_multiple(self, cli_env, capsys):
        from src.cli.commands.settings import run
        run(_ns(settings_action="semantic", provider="ollama", model="nomic-embed"))
        out = capsys.readouterr().out
        assert "semantic_provider = ollama" in out
        assert "semantic_model = nomic-embed" in out

    def test_semantic_show_after_set(self, cli_env, capsys):
        from src.cli.commands.settings import run
        run(_ns(settings_action="semantic", provider="openai"))
        capsys.readouterr()
        run(_ns(settings_action="semantic"))
        out = capsys.readouterr().out
        assert "openai" in out
