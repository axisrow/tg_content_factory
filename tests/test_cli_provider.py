"""Tests for CLI provider commands."""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, patch

import pytest
import typer

from tests.helpers import cli_ns as _ns
from tg_agent.agent.provider_registry import (
    ZAI_CODING_BASE_URL,
    ZAI_GENERAL_BASE_URL,
    ProviderRuntimeConfig,
)
from tg_agent.config import AppConfig
from tg_agent.database import Database
from tg_agent.services.agent_provider_service import ProviderConfigService, ProviderModelCacheEntry


@pytest.fixture
def cli_env(cli_db):
    config = AppConfig()

    async def fake_init_db(config_path: str):
        cmd_db = Database(cli_db._db_path)
        await cmd_db.initialize()
        return config, cmd_db

    with patch("tg_agent.cli.commands.provider.runtime.init_db", side_effect=fake_init_db):
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
    @patch("tg_agent.cli.commands.provider.ProviderConfigService")
    def test_list_no_providers(self, mock_svc, cli_env, capsys):
        svc = mock_svc.return_value
        svc.load_provider_configs = AsyncMock(return_value=[])
        svc.load_model_cache = AsyncMock(return_value={})
        from tg_agent.cli.commands.provider import run
        run(_ns(provider_action="list"))
        out = capsys.readouterr().out
        assert "No providers configured" in out
        assert "Available providers" in out

    @patch("tg_agent.cli.commands.provider.ProviderConfigService")
    def test_list_with_providers(self, mock_svc, cli_env, capsys):
        svc = mock_svc.return_value
        cfg = _make_cfg("openai", selected_model="gpt-4")
        svc.load_provider_configs = AsyncMock(return_value=[cfg])
        svc.load_model_cache = AsyncMock(return_value={"openai": _make_entry("openai")})
        from tg_agent.cli.commands.provider import run
        run(_ns(provider_action="list"))
        out = capsys.readouterr().out
        assert "openai" in out
        assert "gpt-4" in out
        assert "Yes" in out

    @patch("tg_agent.cli.commands.provider.ProviderConfigService")
    def test_list_disabled_provider(self, mock_svc, cli_env, capsys):
        svc = mock_svc.return_value
        cfg = _make_cfg("openai", enabled=False)
        svc.load_provider_configs = AsyncMock(return_value=[cfg])
        svc.load_model_cache = AsyncMock(return_value={})
        from tg_agent.cli.commands.provider import run
        run(_ns(provider_action="list"))
        out = capsys.readouterr().out
        assert "No" in out

    @patch("tg_agent.cli.commands.provider.ProviderConfigService")
    def test_list_with_error(self, mock_svc, cli_env, capsys):
        svc = mock_svc.return_value
        cfg = _make_cfg("openai", error="connection timed out unexpectedly")
        svc.load_provider_configs = AsyncMock(return_value=[cfg])
        svc.load_model_cache = AsyncMock(return_value={})
        from tg_agent.cli.commands.provider import run
        run(_ns(provider_action="list"))
        out = capsys.readouterr().out
        assert "connection timed out unexpectedly"[:30] in out


class TestAdd:
    @patch("tg_agent.cli.commands.provider.ProviderConfigService")
    def test_add_unknown_provider(self, mock_svc, cli_env, capsys):
        svc = mock_svc.return_value
        svc.writes_enabled = True
        from tg_agent.cli.commands.provider import run
        with pytest.raises(typer.Exit):
            run(_ns(provider_action="add", name="nonexistent_xyz"))
        assert "Unknown provider" in capsys.readouterr().err

    @patch("tg_agent.cli.commands.provider.ProviderConfigService")
    def test_add_requires_encryption(self, mock_svc, cli_env, capsys):
        svc = mock_svc.return_value
        svc.writes_enabled = False
        from tg_agent.cli.commands.provider import run
        with pytest.raises(typer.Exit):
            run(_ns(provider_action="add", name="openai", api_key="sk-test"))
        assert "SESSION_ENCRYPTION_KEY" in capsys.readouterr().err

    @patch("tg_agent.cli.commands.provider.ProviderConfigService")
    def test_add_new_provider(self, mock_svc, cli_env, capsys):
        svc = mock_svc.return_value
        svc.writes_enabled = True
        svc.load_provider_configs = AsyncMock(return_value=[])
        svc.save_provider_configs = AsyncMock()
        from tg_agent.cli.commands.provider import run
        run(_ns(provider_action="add", name="openai", api_key="sk-test123", base_url=None))
        out = capsys.readouterr().out
        assert "Added provider: openai" in out
        svc.save_provider_configs.assert_awaited_once()

    @patch("tg_agent.cli.commands.provider.ProviderConfigService")
    def test_add_existing_updates(self, mock_svc, cli_env, capsys):
        svc = mock_svc.return_value
        svc.writes_enabled = True
        existing = _make_cfg("openai", priority=5, selected_model="gpt-4")
        svc.load_provider_configs = AsyncMock(return_value=[existing])
        svc.save_provider_configs = AsyncMock()
        from tg_agent.cli.commands.provider import run
        run(_ns(provider_action="add", name="openai", api_key="sk-new", base_url=None))
        out = capsys.readouterr().out
        assert "Updated provider: openai" in out

    @patch("tg_agent.cli.commands.provider.ProviderConfigService")
    def test_add_with_base_url(self, mock_svc, cli_env, capsys):
        svc = mock_svc.return_value
        svc.writes_enabled = True
        svc.load_provider_configs = AsyncMock(return_value=[])
        svc.save_provider_configs = AsyncMock()
        from tg_agent.cli.commands.provider import run
        run(_ns(provider_action="add", name="openai", api_key="sk-test", base_url="http://localhost:8000"))
        out = capsys.readouterr().out
        assert "Added provider: openai" in out


class TestDelete:
    @patch("tg_agent.cli.commands.provider.ProviderConfigService")
    def test_delete_requires_encryption(self, mock_svc, cli_env, capsys):
        svc = mock_svc.return_value
        svc.writes_enabled = False
        from tg_agent.cli.commands.provider import run
        with pytest.raises(typer.Exit):
            run(_ns(provider_action="delete", name="openai"))
        assert "SESSION_ENCRYPTION_KEY" in capsys.readouterr().err

    @patch("tg_agent.cli.commands.provider.ProviderConfigService")
    def test_delete_not_found(self, mock_svc, cli_env, capsys):
        svc = mock_svc.return_value
        svc.writes_enabled = True
        svc.load_provider_configs = AsyncMock(return_value=[])
        from tg_agent.cli.commands.provider import run
        with pytest.raises(typer.Exit):
            run(_ns(provider_action="delete", name="openai"))
        assert "not found" in capsys.readouterr().err

    @patch("tg_agent.cli.commands.provider.ProviderConfigService")
    def test_delete_existing(self, mock_svc, cli_env, capsys):
        svc = mock_svc.return_value
        svc.writes_enabled = True
        cfg = _make_cfg("openai")
        svc.load_provider_configs = AsyncMock(return_value=[cfg])
        svc.save_provider_configs = AsyncMock()
        from tg_agent.cli.commands.provider import run
        run(_ns(provider_action="delete", name="openai"))
        out = capsys.readouterr().out
        assert "Deleted provider: openai" in out
        saved = svc.save_provider_configs.call_args[0][0]
        assert len(saved) == 0


class TestProbe:
    @patch("tg_agent.cli.commands.provider.ProviderConfigService")
    def test_probe_not_configured(self, mock_svc, cli_env, capsys):
        svc = mock_svc.return_value
        svc.load_provider_configs = AsyncMock(return_value=[])
        from tg_agent.cli.commands.provider import run
        with pytest.raises(typer.Exit):
            run(_ns(provider_action="probe", name="openai"))
        assert "not configured" in capsys.readouterr().err

    @patch("tg_agent.cli.commands.provider.ProviderConfigService")
    def test_probe_success(self, mock_svc, cli_env, capsys):
        svc = mock_svc.return_value
        cfg = _make_cfg("openai")
        svc.load_provider_configs = AsyncMock(return_value=[cfg])
        entry = _make_entry("openai", models=["gpt-4", "gpt-3.5-turbo", "gpt-4o"])
        svc.refresh_models_for_provider = AsyncMock(return_value=entry)
        from tg_agent.cli.commands.provider import run
        run(_ns(provider_action="probe", name="openai"))
        out = capsys.readouterr().out
        assert "OK" in out
        assert "3 models" in out

    @patch("tg_agent.cli.commands.provider.ProviderConfigService")
    def test_probe_with_error(self, mock_svc, cli_env, capsys):
        svc = mock_svc.return_value
        cfg = _make_cfg("openai")
        svc.load_provider_configs = AsyncMock(return_value=[cfg])
        entry = _make_entry("openai", models=["gpt-4"], error="rate limited")
        svc.refresh_models_for_provider = AsyncMock(return_value=entry)
        from tg_agent.cli.commands.provider import run
        # #1962: the live fetch failed — the cached list is no confirmed connection.
        with pytest.raises(typer.Exit):
            run(_ns(provider_action="probe", name="openai"))
        captured = capsys.readouterr()
        assert "FAIL: rate limited" in captured.err
        assert "OK" not in captured.out
        assert "rate limited" not in captured.out

    @patch("tg_agent.cli.commands.provider.ProviderConfigService")
    def test_probe_failure(self, mock_svc, cli_env, capsys):
        svc = mock_svc.return_value
        cfg = _make_cfg("openai")
        svc.load_provider_configs = AsyncMock(return_value=[cfg])
        svc.refresh_models_for_provider = AsyncMock(side_effect=Exception("timeout"))
        from tg_agent.cli.commands.provider import run
        with pytest.raises(typer.Exit):
            run(_ns(provider_action="probe", name="openai"))
        assert "FAIL" in capsys.readouterr().err

    @patch("tg_agent.cli.commands.provider.ProviderConfigService")
    def test_probe_many_models_truncates(self, mock_svc, cli_env, capsys):
        svc = mock_svc.return_value
        cfg = _make_cfg("openai")
        svc.load_provider_configs = AsyncMock(return_value=[cfg])
        models = [f"model-{i}" for i in range(15)]
        entry = _make_entry("openai", models=models)
        svc.refresh_models_for_provider = AsyncMock(return_value=entry)
        from tg_agent.cli.commands.provider import run
        run(_ns(provider_action="probe", name="openai"))
        out = capsys.readouterr().out
        assert "and 5 more" in out


class TestRefresh:
    @patch("tg_agent.cli.commands.provider.ProviderConfigService")
    def test_refresh_single(self, mock_svc, cli_env, capsys):
        svc = mock_svc.return_value
        entry = _make_entry("openai", models=["gpt-4"])
        svc.refresh_models_for_provider = AsyncMock(return_value=entry)
        from tg_agent.cli.commands.provider import run
        run(_ns(provider_action="refresh", name="openai"))
        out = capsys.readouterr().out
        assert "OK" in out
        assert "1 models" in out

    @patch("tg_agent.cli.commands.provider.ProviderConfigService")
    def test_refresh_single_failure(self, mock_svc, cli_env, capsys):
        svc = mock_svc.return_value
        svc.refresh_models_for_provider = AsyncMock(side_effect=Exception("network"))
        from tg_agent.cli.commands.provider import run
        with pytest.raises(typer.Exit):
            run(_ns(provider_action="refresh", name="openai"))
        assert "FAIL" in capsys.readouterr().err

    @patch("tg_agent.cli.commands.provider.ProviderConfigService")
    def test_refresh_all(self, mock_svc, cli_env, capsys):
        svc = mock_svc.return_value
        results = {
            "openai": _make_entry("openai", models=["gpt-4"]),
            "groq": _make_entry("groq", models=["llama3"], error="quota"),
        }
        svc.refresh_all_models = AsyncMock(return_value=results)
        from tg_agent.cli.commands.provider import run
        # #1962: the table stays on stdout; the provider that did not refresh fails the run.
        with pytest.raises(typer.Exit):
            run(_ns(provider_action="refresh", name=None))
        captured = capsys.readouterr()
        assert "openai: 1 models — OK" in captured.out
        assert "groq: 1 models — FAILED" in captured.out
        assert "groq: quota" in captured.err
        assert "1 of 2 provider(s) failed." in captured.err


class TestTestAll:
    @patch("tg_agent.cli.commands.provider.ProviderConfigService")
    def test_test_all_no_providers(self, mock_svc, cli_env, capsys):
        svc = mock_svc.return_value
        svc.load_provider_configs = AsyncMock(return_value=[])
        from tg_agent.cli.commands.provider import run
        run(_ns(provider_action="test-all"))
        out = capsys.readouterr().out
        assert "No providers configured" in out

    @patch("tg_agent.cli.commands.provider.ProviderConfigService")
    def test_test_all_success(self, mock_svc, cli_env, capsys):
        svc = mock_svc.return_value
        cfg1 = _make_cfg("openai")
        cfg2 = _make_cfg("groq")
        svc.load_provider_configs = AsyncMock(return_value=[cfg1, cfg2])
        entry_ok = _make_entry("openai", models=["gpt-4"])
        entry_also_ok = _make_entry("groq", models=["llama3"])
        svc.refresh_models_for_provider = AsyncMock(side_effect=[entry_ok, entry_also_ok])
        from tg_agent.cli.commands.provider import run
        run(_ns(provider_action="test-all"))
        captured = capsys.readouterr()
        assert captured.out.count("OK (1 models)") == 2  # both providers
        assert "FAIL" not in captured.out
        assert captured.err == ""

    @patch("tg_agent.cli.commands.provider.ProviderConfigService")
    def test_test_all_reported_error_is_a_failure(self, mock_svc, cli_env, capsys):
        """#1962: the service answers a failed live fetch with the cached list and
        the reason — that provider failed, like one that raised."""
        svc = mock_svc.return_value
        cfg1 = _make_cfg("openai")
        cfg2 = _make_cfg("groq")
        svc.load_provider_configs = AsyncMock(return_value=[cfg1, cfg2])
        entry_ok = _make_entry("openai", models=["gpt-4"])
        entry_failed = _make_entry("groq", models=["llama3"], source="static cache", error="slow")
        svc.refresh_models_for_provider = AsyncMock(side_effect=[entry_ok, entry_failed])
        from tg_agent.cli.commands.provider import run
        with pytest.raises(typer.Exit):
            run(_ns(provider_action="test-all"))
        captured = capsys.readouterr()
        assert "openai... OK (1 models)" in captured.out
        assert "groq... FAIL" in captured.out
        assert "groq: slow" in captured.err
        assert "1 of 2 provider(s) failed." in captured.err

    @patch("tg_agent.cli.commands.provider.ProviderConfigService")
    def test_test_all_failure(self, mock_svc, cli_env, capsys):
        svc = mock_svc.return_value
        cfg = _make_cfg("openai")
        svc.load_provider_configs = AsyncMock(return_value=[cfg])
        svc.refresh_models_for_provider = AsyncMock(side_effect=Exception("timeout"))
        from tg_agent.cli.commands.provider import run
        with pytest.raises(typer.Exit):
            run(_ns(provider_action="test-all"))
        captured = capsys.readouterr()
        assert "FAIL" in captured.out
        assert "failed." in captured.err


def _save_real_cli_provider_config(cli_db, config: AppConfig, cfg: ProviderRuntimeConfig) -> None:
    asyncio.run(ProviderConfigService(cli_db, config).save_provider_configs([cfg]))


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

    monkeypatch.setattr(ProviderConfigService, "_fetch_json", fake_fetch_json)
    with patch("tg_agent.cli.commands.provider.runtime.init_db", side_effect=fake_init_db):
        from tg_agent.cli.commands.provider import run

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
            plain_fields={"base_url": ZAI_GENERAL_BASE_URL},
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

    monkeypatch.setattr(ProviderConfigService, "_fetch_json", fake_fetch_json)
    with patch("tg_agent.cli.commands.provider.runtime.init_db", side_effect=fake_init_db):
        from tg_agent.cli.commands.provider import run

        run(_ns(provider_action="test-all"))

    out = capsys.readouterr().out
    assert "Testing 1 provider(s)" in out
    assert "zai... OK (1 models)" in out
    assert fetched == [f"{ZAI_GENERAL_BASE_URL}/models"]


@pytest.mark.parametrize(
    ("provider_action", "name", "reason_line"),
    [
        ("probe", "openai", "FAIL: TimeoutError"),
        ("refresh", "openai", "FAIL: TimeoutError"),
        ("test-all", None, "openai: TimeoutError"),
    ],
)
def test_real_service_messageless_fetch_failure_fails_the_command(
    cli_db, capsys, monkeypatch, provider_action, name, reason_line
):
    """#1962: the service stores str(exc) as the reason. A live fetch that dies
    without a message must still fail the command that confirms the connection."""
    config = AppConfig()
    config.security.session_encryption_key = "provider-cli-secret"
    _save_real_cli_provider_config(cli_db, config, _make_cfg("openai"))

    async def fetch_without_message(_self, url: str, headers: dict[str, str] | None = None):
        raise TimeoutError()

    async def fake_init_db(_config_path: str):
        cmd_db = Database(cli_db._db_path)
        await cmd_db.initialize()
        return config, cmd_db

    monkeypatch.setattr(ProviderConfigService, "_fetch_json", fetch_without_message)
    with patch("tg_agent.cli.commands.provider.runtime.init_db", side_effect=fake_init_db):
        from tg_agent.cli.commands.provider import run

        with pytest.raises(typer.Exit):
            run(_ns(provider_action=provider_action, name=name))

    captured = capsys.readouterr()
    assert reason_line in captured.err
    assert "OK" not in captured.out
