"""Tests for src/cli/commands/settings.py — CLI settings subcommands."""
from __future__ import annotations

import argparse
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.cli.commands.settings import run


def _fake_asyncio_run(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def _args(**overrides):
    defaults = {"config": "config.yaml"}
    defaults.update(overrides)
    return argparse.Namespace(**defaults)


def _make_db(**overrides):
    db = MagicMock()
    db.close = AsyncMock()
    db.get_setting = AsyncMock(return_value=None)
    db.set_setting = AsyncMock()
    db.get_stats = AsyncMock(return_value={"channels": 5})
    db.repos.settings.list_all = AsyncMock(return_value=[])
    for k, v in overrides.items():
        setattr(db, k, v)
    return db


def _make_config():
    return MagicMock()


# ---------------------------------------------------------------------------
# get
# ---------------------------------------------------------------------------


def test_get_specific_key(capsys):
    db = _make_db(get_setting=AsyncMock(return_value="myval"))
    with patch("src.cli.commands.settings.runtime.init_db", AsyncMock(return_value=(_make_config(), db))), \
         patch("asyncio.run", _fake_asyncio_run):
        run(_args(settings_action="get", key="test_key"))
    assert "myval" in capsys.readouterr().out


def test_get_key_not_set(capsys):
    db = _make_db(get_setting=AsyncMock(return_value=None))
    with patch("src.cli.commands.settings.runtime.init_db", AsyncMock(return_value=(_make_config(), db))), \
         patch("asyncio.run", _fake_asyncio_run):
        run(_args(settings_action="get", key="missing"))
    assert "not set" in capsys.readouterr().out


def test_get_all_empty(capsys):
    db = _make_db()
    with patch("src.cli.commands.settings.runtime.init_db", AsyncMock(return_value=(_make_config(), db))), \
         patch("asyncio.run", _fake_asyncio_run):
        run(_args(settings_action="get", key=None))
    assert "No settings" in capsys.readouterr().out


def test_get_all_with_rows(capsys):
    db = _make_db()
    db.repos.settings.list_all = AsyncMock(return_value=[("key1", "val1"), ("key2", "val2")])
    with patch("src.cli.commands.settings.runtime.init_db", AsyncMock(return_value=(_make_config(), db))), \
         patch("asyncio.run", _fake_asyncio_run):
        run(_args(settings_action="get", key=None))
    out = capsys.readouterr().out
    assert "key1" in out
    assert "val1" in out


# ---------------------------------------------------------------------------
# set
# ---------------------------------------------------------------------------


def test_set_key_value(capsys):
    db = _make_db()
    with patch("src.cli.commands.settings.runtime.init_db", AsyncMock(return_value=(_make_config(), db))), \
         patch("asyncio.run", _fake_asyncio_run):
        run(_args(settings_action="set", key="mykey", value="myval"))
    db.set_setting.assert_called_with("mykey", "myval")
    assert "mykey" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# info
# ---------------------------------------------------------------------------


def test_info(capsys):
    db = _make_db()
    with patch("src.cli.commands.settings.runtime.init_db", AsyncMock(return_value=(_make_config(), db))), \
         patch("asyncio.run", _fake_asyncio_run):
        run(_args(settings_action="info"))
    assert "channels" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# agent
# ---------------------------------------------------------------------------


def test_agent_set_backend(capsys):
    db = _make_db()
    with patch("src.cli.commands.settings.runtime.init_db", AsyncMock(return_value=(_make_config(), db))), \
         patch("asyncio.run", _fake_asyncio_run):
        run(_args(settings_action="agent", backend="claude", prompt_template=None))
    db.set_setting.assert_called_with("agent_backend", "claude")


def test_agent_set_prompt_template(capsys):
    db = _make_db()
    with patch("src.cli.commands.settings.runtime.init_db", AsyncMock(return_value=(_make_config(), db))), \
         patch("asyncio.run", _fake_asyncio_run):
        run(_args(settings_action="agent", backend=None, prompt_template="My prompt template for testing"))
    assert "Set" in capsys.readouterr().out


def test_agent_show_current(capsys):
    db = _make_db()
    with patch("src.cli.commands.settings.runtime.init_db", AsyncMock(return_value=(_make_config(), db))), \
         patch("asyncio.run", _fake_asyncio_run):
        run(_args(settings_action="agent", backend=None, prompt_template=None))
    out = capsys.readouterr().out
    assert "agent_backend" in out


# ---------------------------------------------------------------------------
# filter-criteria
# ---------------------------------------------------------------------------


def test_filter_criteria_set_values(capsys):
    db = _make_db()
    with patch("src.cli.commands.settings.runtime.init_db", AsyncMock(return_value=(_make_config(), db))), \
         patch("asyncio.run", _fake_asyncio_run):
        run(_args(settings_action="filter-criteria", min_uniqueness=0.5, min_sub_ratio=0.1,
                   max_cross_dupe=None, min_cyrillic=None))
    out = capsys.readouterr().out
    assert "filter_min_uniqueness" in out


def test_filter_criteria_show_current(capsys):
    db = _make_db()
    with patch("src.cli.commands.settings.runtime.init_db", AsyncMock(return_value=(_make_config(), db))), \
         patch("asyncio.run", _fake_asyncio_run):
        run(_args(settings_action="filter-criteria", min_uniqueness=None, min_sub_ratio=None,
                   max_cross_dupe=None, min_cyrillic=None))
    out = capsys.readouterr().out
    assert "filter_min_uniqueness" in out


# ---------------------------------------------------------------------------
# semantic
# ---------------------------------------------------------------------------


def test_semantic_set_values(capsys):
    db = _make_db()
    with patch("src.cli.commands.settings.runtime.init_db", AsyncMock(return_value=(_make_config(), db))), \
         patch("asyncio.run", _fake_asyncio_run):
        run(_args(settings_action="semantic", provider="openai", model="text-embedding-3-small",
                   api_key=None))
    out = capsys.readouterr().out
    assert "semantic_provider" in out


def test_semantic_api_key_truncation(capsys):
    db = _make_db()
    long_key = "sk-" + "a" * 50
    with patch("src.cli.commands.settings.runtime.init_db", AsyncMock(return_value=(_make_config(), db))), \
         patch("asyncio.run", _fake_asyncio_run):
        run(_args(settings_action="semantic", provider=None, model=None, api_key=long_key))
    out = capsys.readouterr().out
    assert "..." in out


def test_semantic_show_current(capsys):
    db = _make_db()
    with patch("src.cli.commands.settings.runtime.init_db", AsyncMock(return_value=(_make_config(), db))), \
         patch("asyncio.run", _fake_asyncio_run):
        run(_args(settings_action="semantic", provider=None, model=None, api_key=None))
    out = capsys.readouterr().out
    assert "semantic_provider" in out


def test_semantic_show_api_key_masked(capsys):
    db = _make_db(get_setting=AsyncMock(return_value="sk-secret-key-12345"))
    with patch("src.cli.commands.settings.runtime.init_db", AsyncMock(return_value=(_make_config(), db))), \
         patch("asyncio.run", _fake_asyncio_run):
        run(_args(settings_action="semantic", provider=None, model=None, api_key=None))
    out = capsys.readouterr().out
    assert "..." in out
