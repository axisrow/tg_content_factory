"""Tests for src/cli/commands/provider.py — CLI provider subcommands."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import typer

from tests.helpers import cli_ns, fake_asyncio_run, make_cli_config, make_cli_db
from tg_agent.cli.commands.provider import run


def _args(**overrides):
    defaults = {"config": "config.yaml"}
    defaults.update(overrides)
    return cli_ns(**defaults)


def _make_provider_config(provider="openai", enabled=True, priority=0, selected_model="",
                           last_validation_error=None, secret_fields=None, plain_fields=None):
    cfg = MagicMock()
    cfg.provider = provider
    cfg.enabled = enabled
    cfg.priority = priority
    cfg.selected_model = selected_model
    cfg.last_validation_error = last_validation_error
    cfg.secret_fields = secret_fields or {}
    cfg.plain_fields = plain_fields or {}
    return cfg


def _make_svc(**overrides):
    svc = MagicMock()
    svc.writes_enabled = True
    svc.load_provider_configs = AsyncMock(return_value=[])
    svc.save_provider_configs = AsyncMock()
    svc.load_model_cache = AsyncMock(return_value={})
    svc.refresh_models_for_provider = AsyncMock()
    svc.refresh_all_models = AsyncMock(return_value={})
    for k, v in overrides.items():
        setattr(svc, k, v)
    return svc


# ---------------------------------------------------------------------------
# list
# ---------------------------------------------------------------------------


def test_list_empty(capsys):
    db = make_cli_db()
    svc = _make_svc()
    with patch("tg_agent.cli.commands.provider.runtime.init_db", AsyncMock(return_value=(make_cli_config(), db))), \
         patch("tg_agent.cli.commands.provider.ProviderConfigService", return_value=svc), \
         patch("asyncio.run", fake_asyncio_run):
        run(_args(provider_action="list"))
    assert "No providers" in capsys.readouterr().out


def test_list_with_configs(capsys):
    db = make_cli_db()
    cfg = _make_provider_config("openai", selected_model="gpt-4o")
    entry = MagicMock()
    entry.models = ["gpt-4o", "gpt-4o-mini"]
    svc = _make_svc(
        load_provider_configs=AsyncMock(return_value=[cfg]),
        load_model_cache=AsyncMock(return_value={"openai": entry}),
    )
    with patch("tg_agent.cli.commands.provider.runtime.init_db", AsyncMock(return_value=(make_cli_config(), db))), \
         patch("tg_agent.cli.commands.provider.ProviderConfigService", return_value=svc), \
         patch("asyncio.run", fake_asyncio_run):
        run(_args(provider_action="list"))
    assert "openai" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# add
# ---------------------------------------------------------------------------


def test_add_unknown_provider(capsys):
    db = make_cli_db()
    svc = _make_svc()
    with patch("tg_agent.cli.commands.provider.runtime.init_db", AsyncMock(return_value=(make_cli_config(), db))), \
         patch("tg_agent.cli.commands.provider.ProviderConfigService", return_value=svc), \
         patch("tg_agent.cli.commands.provider.provider_spec", return_value=None), \
         patch("asyncio.run", fake_asyncio_run), \
         pytest.raises(typer.Exit):
        run(_args(provider_action="add", name="unknown_provider", api_key="key", base_url=None))
    assert "Unknown provider" in capsys.readouterr().err


def test_add_writes_disabled(capsys):
    db = make_cli_db()
    svc = _make_svc(writes_enabled=False)
    spec = MagicMock(secret_fields=[], plain_fields=[])
    with patch("tg_agent.cli.commands.provider.runtime.init_db", AsyncMock(return_value=(make_cli_config(), db))), \
         patch("tg_agent.cli.commands.provider.ProviderConfigService", return_value=svc), \
         patch("tg_agent.cli.commands.provider.provider_spec", return_value=spec), \
         patch("asyncio.run", fake_asyncio_run), \
         pytest.raises(typer.Exit):
        run(_args(provider_action="add", name="openai", api_key="key", base_url=None))
    assert "SESSION_ENCRYPTION_KEY" in capsys.readouterr().err


def test_add_new_provider(capsys):
    db = make_cli_db()
    svc = _make_svc()
    field = MagicMock(name="api_key")
    spec = MagicMock(secret_fields=[field], plain_fields=[])
    with patch("tg_agent.cli.commands.provider.runtime.init_db", AsyncMock(return_value=(make_cli_config(), db))), \
         patch("tg_agent.cli.commands.provider.ProviderConfigService", return_value=svc), \
         patch("tg_agent.cli.commands.provider.provider_spec", return_value=spec), \
         patch("asyncio.run", fake_asyncio_run):
        run(_args(provider_action="add", name="openai", api_key="sk-test", base_url=None))
    assert "Added" in capsys.readouterr().out
    svc.save_provider_configs.assert_called_once()


def test_add_update_existing(capsys):
    db = make_cli_db()
    existing = _make_provider_config("openai")
    svc = _make_svc(load_provider_configs=AsyncMock(return_value=[existing]))
    field = MagicMock(name="api_key")
    spec = MagicMock(secret_fields=[field], plain_fields=[])
    with patch("tg_agent.cli.commands.provider.runtime.init_db", AsyncMock(return_value=(make_cli_config(), db))), \
         patch("tg_agent.cli.commands.provider.ProviderConfigService", return_value=svc), \
         patch("tg_agent.cli.commands.provider.provider_spec", return_value=spec), \
         patch("asyncio.run", fake_asyncio_run):
        run(_args(provider_action="add", name="openai", api_key="sk-new", base_url=None))
    assert "Updated" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# delete
# ---------------------------------------------------------------------------


def test_delete_writes_disabled(capsys):
    db = make_cli_db()
    svc = _make_svc(writes_enabled=False)
    with patch("tg_agent.cli.commands.provider.runtime.init_db", AsyncMock(return_value=(make_cli_config(), db))), \
         patch("tg_agent.cli.commands.provider.ProviderConfigService", return_value=svc), \
         patch("asyncio.run", fake_asyncio_run), \
         pytest.raises(typer.Exit):
        run(_args(provider_action="delete", name="openai"))
    assert "SESSION_ENCRYPTION_KEY" in capsys.readouterr().err


def test_delete_not_found(capsys):
    db = make_cli_db()
    svc = _make_svc(load_provider_configs=AsyncMock(return_value=[]))
    with patch("tg_agent.cli.commands.provider.runtime.init_db", AsyncMock(return_value=(make_cli_config(), db))), \
         patch("tg_agent.cli.commands.provider.ProviderConfigService", return_value=svc), \
         patch("asyncio.run", fake_asyncio_run), \
         pytest.raises(typer.Exit):
        run(_args(provider_action="delete", name="openai"))
    assert "not found" in capsys.readouterr().err


def test_delete_success(capsys):
    db = make_cli_db()
    cfg = _make_provider_config("openai")
    svc = _make_svc(load_provider_configs=AsyncMock(return_value=[cfg]))
    with patch("tg_agent.cli.commands.provider.runtime.init_db", AsyncMock(return_value=(make_cli_config(), db))), \
         patch("tg_agent.cli.commands.provider.ProviderConfigService", return_value=svc), \
         patch("asyncio.run", fake_asyncio_run):
        run(_args(provider_action="delete", name="openai"))
    assert "Deleted" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# probe
# ---------------------------------------------------------------------------


def test_probe_not_configured(capsys):
    db = make_cli_db()
    svc = _make_svc(load_provider_configs=AsyncMock(return_value=[]))
    with patch("tg_agent.cli.commands.provider.runtime.init_db", AsyncMock(return_value=(make_cli_config(), db))), \
         patch("tg_agent.cli.commands.provider.ProviderConfigService", return_value=svc), \
         patch("asyncio.run", fake_asyncio_run), \
         pytest.raises(typer.Exit):
        run(_args(provider_action="probe", name="openai"))
    assert "not configured" in capsys.readouterr().err


def test_probe_success(capsys):
    db = make_cli_db()
    cfg = _make_provider_config("openai")
    entry = MagicMock(error=None, models=["gpt-4o", "gpt-4o-mini"], source="api")
    svc = _make_svc(
        load_provider_configs=AsyncMock(return_value=[cfg]),
        refresh_models_for_provider=AsyncMock(return_value=entry),
    )
    with patch("tg_agent.cli.commands.provider.runtime.init_db", AsyncMock(return_value=(make_cli_config(), db))), \
         patch("tg_agent.cli.commands.provider.ProviderConfigService", return_value=svc), \
         patch("asyncio.run", fake_asyncio_run):
        run(_args(provider_action="probe", name="openai"))
    out = capsys.readouterr().out
    assert "OK" in out
    assert "2 models" in out


def test_probe_with_error(capsys):
    """#1962: the live fetch failed — the cached list is not a confirmed connection."""
    db = make_cli_db()
    cfg = _make_provider_config("openai")
    entry = MagicMock(error="timeout", models=["gpt-4o"], source="static cache")
    svc = _make_svc(
        load_provider_configs=AsyncMock(return_value=[cfg]),
        refresh_models_for_provider=AsyncMock(return_value=entry),
    )
    with patch("tg_agent.cli.commands.provider.runtime.init_db", AsyncMock(return_value=(make_cli_config(), db))), \
         patch("tg_agent.cli.commands.provider.ProviderConfigService", return_value=svc), \
         patch("asyncio.run", fake_asyncio_run), \
         pytest.raises(typer.Exit):
        run(_args(provider_action="probe", name="openai"))
    captured = capsys.readouterr()
    assert "FAIL: timeout" in captured.err
    assert "OK" not in captured.out


def test_probe_exception(capsys):
    db = make_cli_db()
    cfg = _make_provider_config("openai")
    svc = _make_svc(
        load_provider_configs=AsyncMock(return_value=[cfg]),
        refresh_models_for_provider=AsyncMock(side_effect=RuntimeError("conn refused")),
    )
    with patch("tg_agent.cli.commands.provider.runtime.init_db", AsyncMock(return_value=(make_cli_config(), db))), \
         patch("tg_agent.cli.commands.provider.ProviderConfigService", return_value=svc), \
         patch("asyncio.run", fake_asyncio_run), \
         pytest.raises(typer.Exit):
        run(_args(provider_action="probe", name="openai"))
    assert "FAIL" in capsys.readouterr().err


# ---------------------------------------------------------------------------
# refresh
# ---------------------------------------------------------------------------


def test_refresh_single(capsys):
    db = make_cli_db()
    entry = MagicMock(error=None, models=["m1", "m2"], source="api")
    svc = _make_svc(refresh_models_for_provider=AsyncMock(return_value=entry))
    with patch("tg_agent.cli.commands.provider.runtime.init_db", AsyncMock(return_value=(make_cli_config(), db))), \
         patch("tg_agent.cli.commands.provider.ProviderConfigService", return_value=svc), \
         patch("asyncio.run", fake_asyncio_run):
        run(_args(provider_action="refresh", name="openai"))
    assert "2 models" in capsys.readouterr().out


def test_refresh_single_with_error(capsys):
    """#1962: a refresh that fell back to the static cache did not refresh."""
    db = make_cli_db()
    entry = MagicMock(error="401 Unauthorized", models=["m1"], source="static cache")
    svc = _make_svc(refresh_models_for_provider=AsyncMock(return_value=entry))
    with patch("tg_agent.cli.commands.provider.runtime.init_db", AsyncMock(return_value=(make_cli_config(), db))), \
         patch("tg_agent.cli.commands.provider.ProviderConfigService", return_value=svc), \
         patch("asyncio.run", fake_asyncio_run), \
         pytest.raises(typer.Exit):
        run(_args(provider_action="refresh", name="openai"))
    captured = capsys.readouterr()
    assert "FAIL: 401 Unauthorized" in captured.err
    assert "OK" not in captured.out


def test_refresh_single_exception(capsys):
    db = make_cli_db()
    svc = _make_svc(refresh_models_for_provider=AsyncMock(side_effect=RuntimeError("fail")))
    with patch("tg_agent.cli.commands.provider.runtime.init_db", AsyncMock(return_value=(make_cli_config(), db))), \
         patch("tg_agent.cli.commands.provider.ProviderConfigService", return_value=svc), \
         patch("asyncio.run", fake_asyncio_run), \
         pytest.raises(typer.Exit):
        run(_args(provider_action="refresh", name="openai"))
    assert "FAIL" in capsys.readouterr().err


def test_refresh_all(capsys):
    db = make_cli_db()
    entry = MagicMock(error=None, models=["m1"], source="api")
    svc = _make_svc(refresh_all_models=AsyncMock(return_value={"openai": entry}))
    with patch("tg_agent.cli.commands.provider.runtime.init_db", AsyncMock(return_value=(make_cli_config(), db))), \
         patch("tg_agent.cli.commands.provider.ProviderConfigService", return_value=svc), \
         patch("asyncio.run", fake_asyncio_run):
        run(_args(provider_action="refresh", name=None))
    assert "openai" in capsys.readouterr().out


def test_refresh_all_with_one_failed(capsys):
    """#1962: the table stays on stdout; a provider that did not refresh fails the run."""
    db = make_cli_db()
    ok = MagicMock(error=None, models=["m1"], source="live")
    broken = MagicMock(error="401 Unauthorized", models=["old"], source="static cache")
    svc = _make_svc(refresh_all_models=AsyncMock(return_value={"openai": ok, "groq": broken}))
    with patch("tg_agent.cli.commands.provider.runtime.init_db", AsyncMock(return_value=(make_cli_config(), db))), \
         patch("tg_agent.cli.commands.provider.ProviderConfigService", return_value=svc), \
         patch("asyncio.run", fake_asyncio_run), \
         pytest.raises(typer.Exit):
        run(_args(provider_action="refresh", name=None))
    captured = capsys.readouterr()
    assert "openai: 1 models — OK" in captured.out
    assert "groq: 1 models — FAILED" in captured.out
    assert "groq: 401 Unauthorized" in captured.err
    assert "1 of 2 provider(s) failed." in captured.err


# ---------------------------------------------------------------------------
# test-all
# ---------------------------------------------------------------------------


def test_test_all_no_configs(capsys):
    db = make_cli_db()
    svc = _make_svc()
    with patch("tg_agent.cli.commands.provider.runtime.init_db", AsyncMock(return_value=(make_cli_config(), db))), \
         patch("tg_agent.cli.commands.provider.ProviderConfigService", return_value=svc), \
         patch("asyncio.run", fake_asyncio_run):
        run(_args(provider_action="test-all"))
    assert "No providers" in capsys.readouterr().out


def test_test_all_success(capsys):
    db = make_cli_db()
    cfg = _make_provider_config("openai")
    entry = MagicMock(error=None, models=["gpt-4o"])
    svc = _make_svc(
        load_provider_configs=AsyncMock(return_value=[cfg]),
        refresh_models_for_provider=AsyncMock(return_value=entry),
    )
    with patch("tg_agent.cli.commands.provider.runtime.init_db", AsyncMock(return_value=(make_cli_config(), db))), \
         patch("tg_agent.cli.commands.provider.ProviderConfigService", return_value=svc), \
         patch("asyncio.run", fake_asyncio_run):
        run(_args(provider_action="test-all"))
    assert "OK" in capsys.readouterr().out


def test_test_all_counts_a_reported_error_as_failure(capsys):
    """#1962: the service answers a failed live fetch with the cached list and the
    reason — that provider failed, like one that raised."""
    db = make_cli_db()
    cfg = _make_provider_config("openai")
    entry = MagicMock(error="401 Unauthorized", models=["gpt-4o"], source="static cache")
    svc = _make_svc(
        load_provider_configs=AsyncMock(return_value=[cfg]),
        refresh_models_for_provider=AsyncMock(return_value=entry),
    )
    with patch("tg_agent.cli.commands.provider.runtime.init_db", AsyncMock(return_value=(make_cli_config(), db))), \
         patch("tg_agent.cli.commands.provider.ProviderConfigService", return_value=svc), \
         patch("asyncio.run", fake_asyncio_run), \
         pytest.raises(typer.Exit):
        run(_args(provider_action="test-all"))
    captured = capsys.readouterr()
    assert "FAIL" in captured.out
    assert "OK" not in captured.out
    assert "openai: 401 Unauthorized" in captured.err
    assert "1 of 1 provider(s) failed." in captured.err


def test_test_all_with_failure(capsys):
    db = make_cli_db()
    cfg = _make_provider_config("bad_provider")
    svc = _make_svc(
        load_provider_configs=AsyncMock(return_value=[cfg]),
        refresh_models_for_provider=AsyncMock(side_effect=RuntimeError("conn err")),
    )
    with patch("tg_agent.cli.commands.provider.runtime.init_db", AsyncMock(return_value=(make_cli_config(), db))), \
         patch("tg_agent.cli.commands.provider.ProviderConfigService", return_value=svc), \
         patch("asyncio.run", fake_asyncio_run), \
         pytest.raises(typer.Exit):
        run(_args(provider_action="test-all"))
    captured = capsys.readouterr()
    assert "FAIL" in captured.out
    assert "bad_provider: conn err" in captured.err
    assert "failed." in captured.err
