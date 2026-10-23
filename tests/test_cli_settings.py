"""Tests for CLI settings commands: agent, filter-criteria, semantic."""
from __future__ import annotations

import asyncio
from unittest.mock import patch

import pytest
import typer

from tests.helpers import cli_ns as _ns
from tg_agent.config import AppConfig
from tg_agent.database import Database
from tg_agent.security import SessionCipher


@pytest.fixture
def cli_env(cli_db):
    config = AppConfig()

    async def fake_init_db(config_path: str):
        cmd_db = Database(cli_db._db_path)
        await cmd_db.initialize()
        return config, cmd_db

    with patch("tg_agent.cli.commands.settings.runtime.init_db", side_effect=fake_init_db):
        yield cli_db


@pytest.fixture
def cli_env_encrypted(cli_db):
    """CLI init_db returns a db carrying the session cipher (SESSION_ENCRYPTION_KEY set)."""
    config = AppConfig()

    async def fake_init_db(config_path: str):
        cmd_db = Database(
            cli_db._db_path, session_encryption_secret="test-session-encryption-key"
        )
        await cmd_db.initialize()
        return config, cmd_db

    with patch("tg_agent.cli.commands.settings.runtime.init_db", side_effect=fake_init_db):
        yield cli_db


@pytest.fixture
def cli_env_no_encryption_key(tmp_path):
    """CLI against a db without any session cipher (SESSION_ENCRYPTION_KEY unset)."""
    db_path = str(tmp_path / "no_key.db")
    config = AppConfig()

    async def fake_init_db(config_path: str):
        cmd_db = Database(db_path, session_encryption_secret=None)
        await cmd_db.initialize()
        return config, cmd_db

    with patch("tg_agent.cli.commands.settings.runtime.init_db", side_effect=fake_init_db):
        yield db_path


async def _write_setting(db_path: str, key: str, value: str) -> None:
    db = Database(db_path, session_encryption_secret="test-session-encryption-key")
    await db.initialize()
    try:
        await db.set_setting(key, value)
    finally:
        await db.close()


async def _read_setting(db_path: str, key: str) -> str | None:
    db = Database(db_path, session_encryption_secret="test-session-encryption-key")
    await db.initialize()
    try:
        return await db.get_setting(key)
    finally:
        await db.close()


class TestAgent:
    def test_agent_show_defaults(self, cli_env, capsys):
        from tg_agent.cli.commands.settings import run
        run(_ns(settings_action="agent"))
        out = capsys.readouterr().out
        assert "agent_backend" in out
        assert "agent_prompt_template" in out
        assert "(not set)" in out

    def test_agent_set_backend(self, cli_env, capsys):
        from tg_agent.cli.commands.settings import run
        run(_ns(settings_action="agent", backend="claude-agent-sdk"))
        out = capsys.readouterr().out
        assert "agent_backend_override = claude-agent-sdk" in out

    def test_agent_set_prompt_template(self, cli_env, capsys):
        from tg_agent.cli.commands.settings import run
        run(_ns(settings_action="agent", prompt_template="You are a helpful assistant."))
        out = capsys.readouterr().out
        assert "agent_prompt_template" in out

    def test_agent_set_both(self, cli_env, capsys):
        from tg_agent.cli.commands.settings import run
        run(_ns(settings_action="agent", backend="deepagents", prompt_template="Be concise."))
        out = capsys.readouterr().out
        assert "agent_backend_override = deepagents" in out
        assert "agent_prompt_template" in out

    def test_agent_show_after_set(self, cli_env, capsys):
        from tg_agent.cli.commands.settings import run
        run(_ns(settings_action="agent", backend="claude-agent-sdk"))
        capsys.readouterr()
        run(_ns(settings_action="agent"))
        out = capsys.readouterr().out
        assert "claude-agent-sdk" in out


class TestFilterCriteria:
    def test_filter_criteria_show_defaults(self, cli_env, capsys):
        from tg_agent.cli.commands.settings import run
        run(_ns(settings_action="filter-criteria"))
        out = capsys.readouterr().out
        assert "filter_min_uniqueness" in out
        assert "filter_min_subscriber_ratio" in out
        assert "filter_max_cross_dupe_pct" in out
        assert "filter_min_cyrillic_pct" in out

    def test_filter_criteria_set_min_uniqueness(self, cli_env, capsys):
        from tg_agent.cli.commands.settings import run
        run(_ns(settings_action="filter-criteria", min_uniqueness=0.8))
        out = capsys.readouterr().out
        assert "filter_min_uniqueness = 0.8" in out

    def test_filter_criteria_set_min_sub_ratio(self, cli_env, capsys):
        from tg_agent.cli.commands.settings import run
        run(_ns(settings_action="filter-criteria", min_sub_ratio=0.5))
        out = capsys.readouterr().out
        assert "filter_min_subscriber_ratio = 0.5" in out

    def test_filter_criteria_set_max_cross_dupe(self, cli_env, capsys):
        from tg_agent.cli.commands.settings import run
        run(_ns(settings_action="filter-criteria", max_cross_dupe=30))
        out = capsys.readouterr().out
        assert "filter_max_cross_dupe_pct = 30" in out

    def test_filter_criteria_set_min_cyrillic(self, cli_env, capsys):
        from tg_agent.cli.commands.settings import run
        run(_ns(settings_action="filter-criteria", min_cyrillic=60))
        out = capsys.readouterr().out
        assert "filter_min_cyrillic_pct = 60" in out

    def test_filter_criteria_set_multiple(self, cli_env, capsys):
        from tg_agent.cli.commands.settings import run
        run(_ns(settings_action="filter-criteria", min_uniqueness=0.9, min_cyrillic=70))
        out = capsys.readouterr().out
        assert "filter_min_uniqueness = 0.9" in out
        assert "filter_min_cyrillic_pct = 70" in out

    def test_filter_criteria_show_after_set(self, cli_env, capsys):
        from tg_agent.cli.commands.settings import run
        run(_ns(settings_action="filter-criteria", min_uniqueness=0.75))
        capsys.readouterr()
        run(_ns(settings_action="filter-criteria"))
        out = capsys.readouterr().out
        assert "0.75" in out

    # Валидация диапазонов (#1596): проценты 0–100, ratio > 0; невалидное
    # значение отклоняет вызов целиком — ничего не сохраняется.

    def test_filter_criteria_rejects_percent_above_100(self, cli_env, capsys):
        from tg_agent.cli.commands.settings import run
        with pytest.raises(typer.Exit):
            run(_ns(settings_action="filter-criteria", min_uniqueness=101))
        err = capsys.readouterr().err
        assert "Invalid" in err
        assert "Nothing saved" in err

    def test_filter_criteria_rejects_negative_percent(self, cli_env, capsys):
        from tg_agent.cli.commands.settings import run
        with pytest.raises(typer.Exit):
            run(_ns(settings_action="filter-criteria", max_cross_dupe=-5))
        assert "Invalid" in capsys.readouterr().err

    def test_filter_criteria_rejects_nonpositive_sub_ratio(self, cli_env, capsys):
        from tg_agent.cli.commands.settings import run
        with pytest.raises(typer.Exit):
            run(_ns(settings_action="filter-criteria", min_sub_ratio=0))
        assert "Invalid" in capsys.readouterr().err

    def test_filter_criteria_rejection_writes_nothing(self, cli_env, capsys):
        from tg_agent.cli.commands.settings import run
        with pytest.raises(typer.Exit):
            run(_ns(settings_action="filter-criteria", min_uniqueness=150, min_cyrillic=60))
        capsys.readouterr()
        run(_ns(settings_action="filter-criteria"))
        out = capsys.readouterr().out
        # Валидный min_cyrillic=60 из отклонённого вызова тоже не сохранился.
        assert "filter_min_cyrillic_pct = (not set)" in out


class TestReactions:
    def test_reactions_show_defaults(self, cli_env, capsys):
        from tg_agent.cli.commands.settings import run
        run(_ns(settings_action="reactions"))
        out = capsys.readouterr().out
        assert "reaction_min_interval_sec" in out
        assert "(not set)" in out

    def test_reactions_set_min_interval(self, cli_env, capsys):
        from tg_agent.cli.commands.settings import run
        run(_ns(settings_action="reactions", min_interval=5))
        out = capsys.readouterr().out
        assert "reaction_min_interval_sec = 5" in out

    def test_reactions_clamps_below_floor(self, cli_env, capsys):
        from tg_agent.cli.commands.settings import run
        run(_ns(settings_action="reactions", min_interval=0))
        out = capsys.readouterr().out
        assert "reaction_min_interval_sec = 1" in out

    def test_reactions_clamps_above_ceiling(self, cli_env, capsys):
        from tg_agent.cli.commands.settings import run
        run(_ns(settings_action="reactions", min_interval=9999))
        out = capsys.readouterr().out
        assert "reaction_min_interval_sec = 300" in out

    def test_reactions_show_after_set(self, cli_env, capsys):
        from tg_agent.cli.commands.settings import run
        run(_ns(settings_action="reactions", min_interval=12))
        capsys.readouterr()
        run(_ns(settings_action="reactions"))
        out = capsys.readouterr().out
        assert "reaction_min_interval_sec = 12" in out


class TestSemantic:
    def test_semantic_show_defaults(self, cli_env, capsys):
        from tg_agent.cli.commands.settings import run
        run(_ns(settings_action="semantic"))
        out = capsys.readouterr().out
        assert "semantic_embeddings_provider" in out
        assert "semantic_embeddings_model" in out
        assert "semantic_embeddings_api_key" in out

    def test_semantic_set_provider(self, cli_env, capsys):
        from tg_agent.cli.commands.settings import run
        run(_ns(settings_action="semantic", provider="openai"))
        out = capsys.readouterr().out
        assert "semantic_embeddings_provider = openai" in out

    def test_semantic_set_model(self, cli_env, capsys):
        from tg_agent.cli.commands.settings import run
        run(_ns(settings_action="semantic", model="text-embedding-3-small"))
        out = capsys.readouterr().out
        assert "semantic_embeddings_model = text-embedding-3-small" in out

    def test_semantic_set_api_key_masked_and_stored_encrypted(self, cli_env_encrypted, capsys):
        from tg_agent.cli.commands.settings import run

        run(_ns(settings_action="semantic", api_key="short"))
        out = capsys.readouterr().out
        assert "short" not in out
        assert "••••••••" in out

    def test_semantic_set_api_key_long_masked(self, cli_env_encrypted, capsys):
        from tg_agent.cli.commands.settings import run

        long_key = "sk-" + "a" * 30
        run(_ns(settings_action="semantic", api_key=long_key))
        out = capsys.readouterr().out
        assert long_key not in out
        assert "••••••••" in out

    def test_semantic_set_multiple(self, cli_env, capsys):
        from tg_agent.cli.commands.settings import run
        run(_ns(settings_action="semantic", provider="ollama", model="nomic-embed"))
        out = capsys.readouterr().out
        assert "semantic_embeddings_provider = ollama" in out
        assert "semantic_embeddings_model = nomic-embed" in out

    def test_semantic_show_after_set(self, cli_env, capsys):
        from tg_agent.cli.commands.settings import run
        run(_ns(settings_action="semantic", provider="openai"))
        capsys.readouterr()
        run(_ns(settings_action="semantic"))
        out = capsys.readouterr().out
        assert "openai" in out


class TestSecretMasking:
    """Issue #1597: the embeddings API key is masked in output and stored encrypted.

    Two symptoms from the issue: (1) `settings get` prints the secret verbatim;
    (2) the key is stored in the settings table as plaintext. The CLI here talks
    to a real SQLite file, so both are asserted end to end.
    """

    KEY = "semantic_embeddings_api_key"
    RAW = "sk-FAKEKEY-0123456789abcdefghijklmnopqrstuvwxyz"
    ENC_SECRET = "test-session-encryption-key"
    TG_API_HASH = "0123456789abcdef0123456789abcdef"  # dummy, never a real hash

    def test_get_all_masks_secret_values(self, cli_env, cli_db, capsys):
        from tg_agent.cli.commands.settings import run

        asyncio.run(_write_setting(cli_db._db_path, self.KEY, self.RAW))
        run(_ns(settings_action="get"))
        out = capsys.readouterr().out
        assert self.KEY in out  # row still listed
        assert self.RAW not in out  # value never printed
        assert "••••••••" in out

    def test_get_all_masks_session_secret_key(self, cli_env, cli_db, capsys):
        from tg_agent.cli.commands.settings import run

        asyncio.run(_write_setting(cli_db._db_path, "session_secret_key", "hmac-secret-1"))
        run(_ns(settings_action="get"))
        out = capsys.readouterr().out
        assert "hmac-secret-1" not in out

    def test_get_specific_secret_key_masks_value(self, cli_env, cli_db, capsys):
        from tg_agent.cli.commands.settings import run

        asyncio.run(_write_setting(cli_db._db_path, self.KEY, self.RAW))
        run(_ns(settings_action="get", key=self.KEY))
        out = capsys.readouterr().out
        assert self.KEY in out
        assert self.RAW not in out
        assert "••••••••" in out

    def test_get_masks_tg_api_hash(self, cli_env, cli_db, capsys):
        """Issue #1664: tg_api_hash is a credential — masked like api keys."""
        from tg_agent.cli.commands.settings import run

        asyncio.run(_write_setting(cli_db._db_path, "tg_api_hash", self.TG_API_HASH))
        run(_ns(settings_action="get"))
        out = capsys.readouterr().out
        assert "tg_api_hash" in out  # row still listed
        assert self.TG_API_HASH not in out  # value never printed
        assert "••••••••" in out

    def test_get_specific_tg_api_hash_masks_value(self, cli_env, cli_db, capsys):
        from tg_agent.cli.commands.settings import run

        asyncio.run(_write_setting(cli_db._db_path, "tg_api_hash", self.TG_API_HASH))
        run(_ns(settings_action="get", key="tg_api_hash"))
        out = capsys.readouterr().out
        assert self.TG_API_HASH not in out
        assert "••••••••" in out

    def test_get_non_secret_key_still_prints_value(self, cli_env, cli_db, capsys):
        from tg_agent.cli.commands.settings import run

        asyncio.run(_write_setting(cli_db._db_path, "collect_interval_minutes", "45"))
        run(_ns(settings_action="get", key="collect_interval_minutes"))
        out = capsys.readouterr().out
        assert "45" in out

    def test_semantic_set_api_key_stored_encrypted_not_plaintext(
        self, cli_env_encrypted, cli_db, capsys
    ):
        from tg_agent.cli.commands.settings import run

        run(_ns(settings_action="semantic", api_key=self.RAW))
        capsys.readouterr()
        stored = asyncio.run(_read_setting(cli_db._db_path, self.KEY))
        assert stored is not None
        assert stored.startswith("enc:v2:")
        assert self.RAW not in stored
        # Round-trip: the stored value decrypts back to the entered key.
        assert SessionCipher(self.ENC_SECRET).decrypt(stored) == self.RAW

    def test_semantic_show_masks_api_key(self, cli_env_encrypted, cli_db, capsys):
        from tg_agent.cli.commands.settings import run

        run(_ns(settings_action="semantic", api_key=self.RAW))
        capsys.readouterr()
        run(_ns(settings_action="semantic"))
        out = capsys.readouterr().out
        assert self.RAW not in out
        assert "••••••••" in out

    def test_semantic_set_api_key_refuses_without_encryption_key(
        self, cli_env_no_encryption_key, capsys
    ):
        from tg_agent.cli.commands.settings import run

        with pytest.raises(typer.Exit):
            run(_ns(settings_action="semantic", api_key=self.RAW))
        err = capsys.readouterr().err
        assert "ERROR" in err
        assert "SESSION_ENCRYPTION_KEY" in err
        stored = asyncio.run(_read_setting(cli_env_no_encryption_key, self.KEY))
        assert stored is None  # refused, not silently stored as plaintext

    def test_settings_set_api_key_routes_to_encrypted_store(
        self, cli_env_encrypted, cli_db, capsys
    ):
        from tg_agent.cli.commands.settings import run

        run(_ns(settings_action="set", key=self.KEY, value=self.RAW))
        out = capsys.readouterr().out
        assert self.RAW not in out
        stored = asyncio.run(_read_setting(cli_db._db_path, self.KEY))
        assert stored is not None
        assert stored.startswith("enc:v2:")
        assert SessionCipher(self.ENC_SECRET).decrypt(stored) == self.RAW

    def test_settings_set_secret_key_masks_echo(self, cli_env, capsys):
        from tg_agent.cli.commands.settings import run

        run(_ns(settings_action="set", key="session_secret_key", value="hmac-secret-1"))
        out = capsys.readouterr().out
        assert "hmac-secret-1" not in out
