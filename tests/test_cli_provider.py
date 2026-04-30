"""Tests for CLI provider commands."""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, patch

import pytest

from src.agent.provider_registry import ZAI_CODING_BASE_URL, ZAI_DEFAULT_BASE_URL, ProviderRuntimeConfig
from src.config import AppConfig
from src.database import Database
from src.services.agent_provider_service import AgentProviderService, ProviderModelCacheEntry
from tests.helpers import cli_ns as _ns


@pytest.fixture
def cli_env(cli_db):
    config = AppConfig()

    async def fake_init_db(config_path: str):
        cmd_db = Database(cli_db._db_path)
        await cmd_db.initialize()
        return config, cmd_db

    with patch("src.cli.commands.provider.runtime.init_db", side_effect=fake_init_db):
        yield cli_db


def _make_cfg(provider="openai", enabled=True, priority=0, selected_model="", error=""):
    return ProviderRuntimeConfig(
        provider=provider,
        enabled=enabled,
        priority=priority,
        selected_model=selected_model,
        plain_fields={},
        secret_fields={"api_key": "test-key"},
        last_validation_error=error,
    )


def _make_entry(provider="openai", models=None, source="api", error=""):
    return ProviderModelCacheEntry(
        provider=provider,
        models=models or ["gpt-4", "gpt-3.5-turbo"],
        source=source,
        error=error,
    )


class TestList:
    @patch("src.cli.commands.provider.AgentProviderService")
    def test_list_no_providers(self, mock_svc, cli_env, capsys):
        svc = mock_svc.return_value
        svc.load_provider_configs = AsyncMock(return_value=[])
        svc.load_model_cache = AsyncMock(return_value={})
        from src.cli.commands.provider import run
        run(_ns(provider_action="list"))
        out = capsys.readouterr().out
        assert "No providers configured" in out
        assert "Available providers" in out

    @patch("src.cli.commands.provider.AgentProviderService")
    def test_list_with_providers(self, mock_svc, cli_env, capsys):
        svc = mock_svc.return_value
        cfg = _make_cfg("openai", selected_model="gpt-4")
        svc.load_provider_configs = AsyncMock(return_value=[cfg])
        svc.load_model_cache = AsyncMock(return_value={"openai": _make_entry("openai")})
        from src.cli.commands.provider import run
        run(_ns(provider_action="list"))
        out = capsys.readouterr().out
        assert "openai" in out
        assert "gpt-4" in out
        assert "Yes" in out

    @patch("src.cli.commands.provider.AgentProviderService")
    def test_list_disabled_provider(self, mock_svc, cli_env, capsys):
        svc = mock_svc.return_value
        cfg = _make_cfg("openai", enabled=False)
        svc.load_provider_configs = AsyncMock(return_value=[cfg])
        svc.load_model_cache = AsyncMock(return_value={})
        from src.cli.commands.provider import run
        run(_ns(provider_action="list"))
        out = capsys.readouterr().out
        assert "No" in out

    @patch("src.cli.commands.provider.AgentProviderService")
    def test_list_with_error(self, mock_svc, cli_env, capsys):
        svc = mock_svc.return_value
        cfg = _make_cfg("openai", error="connection timed out unexpectedly")
        svc.load_provider_configs = AsyncMock(return_value=[cfg])
        svc.load_model_cache = AsyncMock(return_value={})
        from src.cli.commands.provider import run
        run(_ns(provider_action="list"))
        out = capsys.readouterr().out
        assert "connection timed out unexpectedly"[:30] in out


class TestAdd:
    @patch("src.cli.commands.provider.AgentProviderService")
    def test_add_unknown_provider(self, mock_svc, cli_env, capsys):
        svc = mock_svc.return_value
        svc.writes_enabled = True
        from src.cli.commands.provider import run
        run(_ns(provider_action="add", name="nonexistent_xyz"))
        out = capsys.readouterr().out
        assert "Unknown provider" in out

    @patch("src.cli.commands.provider.AgentProviderService")
    def test_add_requires_encryption(self, mock_svc, cli_env, capsys):
        svc = mock_svc.return_value
        svc.writes_enabled = False
        from src.cli.commands.provider import run
        run(_ns(provider_action="add", name="openai", api_key="sk-test"))
        out = capsys.readouterr().out
        assert "SESSION_ENCRYPTION_KEY" in out

    @patch("src.cli.commands.provider.AgentProviderService")
    def test_add_new_provider(self, mock_svc, cli_env, capsys):
        svc = mock_svc.return_value
        svc.writes_enabled = True
        svc.load_provider_configs = AsyncMock(return_value=[])
        svc.save_provider_configs = AsyncMock()
        from src.cli.commands.provider import run
        run(_ns(provider_action="add", name="openai", api_key="sk-test123", base_url=None))
        out = capsys.readouterr().out
        assert "Added provider: openai" in out
        svc.save_provider_configs.assert_awaited_once()

    @patch("src.cli.commands.provider.AgentProviderService")
    def test_add_existing_updates(self, mock_svc, cli_env, capsys):
        svc = mock_svc.return_value
        svc.writes_enabled = True
        existing = _make_cfg("openai", priority=5, selected_model="gpt-4")
        svc.load_provider_configs = AsyncMock(return_value=[existing])
        svc.save_provider_configs = AsyncMock()
        from src.cli.commands.provider import run
        run(_ns(provider_action="add", name="openai", api_key="sk-new", base_url=None))
        out = capsys.readouterr().out
        assert "Updated provider: openai" in out

    @patch("src.cli.commands.provider.AgentProviderService")
    def test_add_with_base_url(self, mock_svc, cli_env, capsys):
        svc = mock_svc.return_value
        svc.writes_enabled = True
        svc.load_provider_configs = AsyncMock(return_value=[])
        svc.save_provider_configs = AsyncMock()
        from src.cli.commands.provider import run
        run(_ns(provider_action="add", name="openai", api_key="sk-test", base_url="http://localhost:8000"))
        out = capsys.readouterr().out
        assert "Added provider: openai" in out


class TestDelete:
    @patch("src.cli.commands.provider.AgentProviderService")
    def test_delete_requires_encryption(self, mock_svc, cli_env, capsys):
        svc = mock_svc.return_value
        svc.writes_enabled = False
        from src.cli.commands.provider import run
        run(_ns(provider_action="delete", name="openai"))
        out = capsys.readouterr().out
        assert "SESSION_ENCRYPTION_KEY" in out

    @patch("src.cli.commands.provider.AgentProviderService")
    def test_delete_not_found(self, mock_svc, cli_env, capsys):
        svc = mock_svc.return_value
        svc.writes_enabled = True
        svc.load_provider_configs = AsyncMock(return_value=[])
        from src.cli.commands.provider import run
        run(_ns(provider_action="delete", name="openai"))
        out = capsys.readouterr().out
        assert "not found" in out

    @patch("src.cli.commands.provider.AgentProviderService")
    def test_delete_existing(self, mock_svc, cli_env, capsys):
        svc = mock_svc.return_value
        svc.writes_enabled = True
        cfg = _make_cfg("openai")
        svc.load_provider_configs = AsyncMock(return_value=[cfg])
        svc.save_provider_configs = AsyncMock()
        from src.cli.commands.provider import run
        run(_ns(provider_action="delete", name="openai"))
        out = capsys.readouterr().out
        assert "Deleted provider: openai" in out
        saved = svc.save_provider_configs.call_args[0][0]
        assert len(saved) == 0


class TestProbe:
    @patch("src.cli.commands.provider.AgentProviderService")
    def test_probe_not_configured(self, mock_svc, cli_env, capsys):
        svc = mock_svc.return_value
        svc.load_provider_configs = AsyncMock(return_value=[])
        from src.cli.commands.provider import run
        run(_ns(provider_action="probe", name="openai"))
        out = capsys.readouterr().out
        assert "not configured" in out

    @patch("src.cli.commands.provider.AgentProviderService")
    def test_probe_success(self, mock_svc, cli_env, capsys):
        svc = mock_svc.return_value
        cfg = _make_cfg("openai")
        svc.load_provider_configs = AsyncMock(return_value=[cfg])
        entry = _make_entry("openai", models=["gpt-4", "gpt-3.5-turbo", "gpt-4o"])
        svc.refresh_models_for_provider = AsyncMock(return_value=entry)
        from src.cli.commands.provider import run
        run(_ns(provider_action="probe", name="openai"))
        out = capsys.readouterr().out
        assert "OK" in out
        assert "3 models" in out

    @patch("src.cli.commands.provider.AgentProviderService")
    def test_probe_with_error(self, mock_svc, cli_env, capsys):
        svc = mock_svc.return_value
        cfg = _make_cfg("openai")
        svc.load_provider_configs = AsyncMock(return_value=[cfg])
        entry = _make_entry("openai", models=["gpt-4"], error="rate limited")
        svc.refresh_models_for_provider = AsyncMock(return_value=entry)
        from src.cli.commands.provider import run
        run(_ns(provider_action="probe", name="openai"))
        out = capsys.readouterr().out
        assert "WARN" in out
        assert "rate limited" in out

    @patch("src.cli.commands.provider.AgentProviderService")
    def test_probe_failure(self, mock_svc, cli_env, capsys):
        svc = mock_svc.return_value
        cfg = _make_cfg("openai")
        svc.load_provider_configs = AsyncMock(return_value=[cfg])
        svc.refresh_models_for_provider = AsyncMock(side_effect=Exception("timeout"))
        from src.cli.commands.provider import run
        run(_ns(provider_action="probe", name="openai"))
        out = capsys.readouterr().out
        assert "FAIL" in out

    @patch("src.cli.commands.provider.AgentProviderService")
    def test_probe_many_models_truncates(self, mock_svc, cli_env, capsys):
        svc = mock_svc.return_value
        cfg = _make_cfg("openai")
        svc.load_provider_configs = AsyncMock(return_value=[cfg])
        models = [f"model-{i}" for i in range(15)]
        entry = _make_entry("openai", models=models)
        svc.refresh_models_for_provider = AsyncMock(return_value=entry)
        from src.cli.commands.provider import run
        run(_ns(provider_action="probe", name="openai"))
        out = capsys.readouterr().out
        assert "and 5 more" in out


class TestRefresh:
    @patch("src.cli.commands.provider.AgentProviderService")
    def test_refresh_single(self, mock_svc, cli_env, capsys):
        svc = mock_svc.return_value
        entry = _make_entry("openai", models=["gpt-4"])
        svc.refresh_models_for_provider = AsyncMock(return_value=entry)
        from src.cli.commands.provider import run
        run(_ns(provider_action="refresh", name="openai"))
        out = capsys.readouterr().out
        assert "OK" in out
        assert "1 models" in out

    @patch("src.cli.commands.provider.AgentProviderService")
    def test_refresh_single_failure(self, mock_svc, cli_env, capsys):
        svc = mock_svc.return_value
        svc.refresh_models_for_provider = AsyncMock(side_effect=Exception("network"))
        from src.cli.commands.provider import run
        run(_ns(provider_action="refresh", name="openai"))
        out = capsys.readouterr().out
        assert "FAIL" in out

    @patch("src.cli.commands.provider.AgentProviderService")
    def test_refresh_all(self, mock_svc, cli_env, capsys):
        svc = mock_svc.return_value
        results = {
            "openai": _make_entry("openai", models=["gpt-4"]),
            "groq": _make_entry("groq", models=["llama3"], error="quota"),
        }
        svc.refresh_all_models = AsyncMock(return_value=results)
        from src.cli.commands.provider import run
        run(_ns(provider_action="refresh", name=None))
        out = capsys.readouterr().out
        assert "openai" in out
        assert "groq" in out
        assert "WARN" in out


class TestTestAll:
    @patch("src.cli.commands.provider.AgentProviderService")
    def test_test_all_no_providers(self, mock_svc, cli_env, capsys):
        svc = mock_svc.return_value
        svc.load_provider_configs = AsyncMock(return_value=[])
        from src.cli.commands.provider import run
        run(_ns(provider_action="test-all"))
        out = capsys.readouterr().out
        assert "No providers configured" in out

    @patch("src.cli.commands.provider.AgentProviderService")
    def test_test_all_success(self, mock_svc, cli_env, capsys):
        svc = mock_svc.return_value
        cfg1 = _make_cfg("openai")
        cfg2 = _make_cfg("groq")
        svc.load_provider_configs = AsyncMock(return_value=[cfg1, cfg2])
        entry_ok = _make_entry("openai", models=["gpt-4"])
        entry_warn = _make_entry("groq", models=["llama3"], error="slow")
        svc.refresh_models_for_provider = AsyncMock(side_effect=[entry_ok, entry_warn])
        from src.cli.commands.provider import run
        run(_ns(provider_action="test-all"))
        out = capsys.readouterr().out
        assert "OK" in out
        assert "WARN" in out

    @patch("src.cli.commands.provider.AgentProviderService")
    def test_test_all_failure(self, mock_svc, cli_env, capsys):
        svc = mock_svc.return_value
        cfg = _make_cfg("openai")
        svc.load_provider_configs = AsyncMock(return_value=[cfg])
        svc.refresh_models_for_provider = AsyncMock(side_effect=Exception("timeout"))
        from src.cli.commands.provider import run
        run(_ns(provider_action="test-all"))
        out = capsys.readouterr().out
        assert "FAIL" in out


def _save_real_cli_provider_config(cli_db, config: AppConfig, cfg: ProviderRuntimeConfig) -> None:
    asyncio.run(AgentProviderService(cli_db, config).save_provider_configs([cfg]))


def test_probe_zai_real_service_reads_db_and_refreshes_models(cli_db, capsys, monkeypatch):
    config = AppConfig()
    config.security.session_encryption_key = "provider-cli-secret"
    _save_real_cli_provider_config(
        cli_db,
        config,
        ProviderRuntimeConfig(
            provider="zai",
            enabled=True,
            priority=0,
            selected_model="glm-5-turbo",
            plain_fields={"base_url": ZAI_CODING_BASE_URL},
            secret_fields={"api_key": "zai-key"},
        ),
    )
    fetched: list[tuple[str, dict[str, str] | None]] = []

    async def fake_fetch_json(_self, url: str, headers: dict[str, str] | None = None):
        fetched.append((url, headers))
        return {"data": [{"id": "glm-5-turbo"}, {"id": "glm-5"}]}

    async def fake_init_db(_config_path: str):
        cmd_db = Database(cli_db._db_path)
        await cmd_db.initialize()
        return config, cmd_db

    monkeypatch.setattr(AgentProviderService, "_fetch_json", fake_fetch_json)
    with patch("src.cli.commands.provider.runtime.init_db", side_effect=fake_init_db):
        from src.cli.commands.provider import run

        run(_ns(provider_action="probe", name="zai"))

    out = capsys.readouterr().out
    assert "Probing zai" in out
    assert "OK: 2 models available" in out
    assert "glm-5-turbo" in out
    assert fetched == [(f"{ZAI_CODING_BASE_URL}/models", {"Authorization": "Bearer zai-key"})]


def test_test_all_real_service_reads_db_and_refreshes_zai_models(cli_db, capsys, monkeypatch):
    config = AppConfig()
    config.security.session_encryption_key = "provider-cli-secret"
    _save_real_cli_provider_config(
        cli_db,
        config,
        ProviderRuntimeConfig(
            provider="zai",
            enabled=True,
            priority=0,
            selected_model="glm-5-turbo",
            plain_fields={"base_url": ""},
            secret_fields={"api_key": "zai-key"},
        ),
    )
    fetched: list[str] = []

    async def fake_fetch_json(_self, url: str, headers: dict[str, str] | None = None):
        assert headers == {"Authorization": "Bearer zai-key"}
        fetched.append(url)
        return {"data": [{"id": "glm-5-turbo"}]}

    async def fake_init_db(_config_path: str):
        cmd_db = Database(cli_db._db_path)
        await cmd_db.initialize()
        return config, cmd_db

    monkeypatch.setattr(AgentProviderService, "_fetch_json", fake_fetch_json)
    with patch("src.cli.commands.provider.runtime.init_db", side_effect=fake_init_db):
        from src.cli.commands.provider import run

        run(_ns(provider_action="test-all"))

    out = capsys.readouterr().out
    assert "Testing 1 provider(s)" in out
    assert "zai... OK (1 models)" in out
    assert fetched == [f"{ZAI_DEFAULT_BASE_URL}/models"]
