"""Tests for src/cli/commands/search.py — CLI search subcommands."""
from __future__ import annotations

import logging
from unittest.mock import AsyncMock, MagicMock, patch

from tests.helpers import cli_ns, fake_asyncio_run, make_cli_config, make_cli_db
from tg_agent.cli.commands.search import run


def _args(**overrides):
    defaults = {"config": "config.yaml", "mode": "local", "query": "test", "limit": 20,
                "channel_id": None, "min_length": 0, "max_length": 0, "fts": False,
                "index_now": False, "reset_index": False}
    defaults.update(overrides)
    return cli_ns(**defaults)


def _make_pool():
    pool = MagicMock()
    pool.clients = {"+1234567890": MagicMock()}
    pool.disconnect_all = AsyncMock()
    return pool


# ---------------------------------------------------------------------------
# index_now
# ---------------------------------------------------------------------------


def test_index_now(capsys):
    db = make_cli_db()
    config = make_cli_config()
    mock_es = MagicMock()
    mock_es.index_pending_messages = AsyncMock(return_value=42)
    with patch("tg_agent.cli.commands.search.runtime.init_db", AsyncMock(return_value=(config, db))), \
         patch("tg_agent.cli.commands.search.EmbeddingService", return_value=mock_es), \
         patch("asyncio.run", fake_asyncio_run):
        run(_args(index_now=True, reset_index=False, query=""))
    assert "42" in capsys.readouterr().out


def test_index_now_with_reset(capsys):
    db = make_cli_db()
    config = make_cli_config()
    mock_es = MagicMock()
    mock_es.index_pending_messages = AsyncMock(return_value=10)
    with patch("tg_agent.cli.commands.search.runtime.init_db", AsyncMock(return_value=(config, db))), \
         patch("tg_agent.cli.commands.search.EmbeddingService", return_value=mock_es), \
         patch("asyncio.run", fake_asyncio_run):
        run(_args(index_now=True, reset_index=True, query=""))
    db.repos.messages.reset_embeddings_index.assert_called_once()


# ---------------------------------------------------------------------------
# no query
# ---------------------------------------------------------------------------


def test_no_query():
    db = make_cli_db()
    config = make_cli_config()
    with patch("tg_agent.cli.commands.search.runtime.init_db", AsyncMock(return_value=(config, db))), \
         patch("asyncio.run", fake_asyncio_run):
        run(_args(index_now=False, query=""))


def test_empty_query_rejected_before_engine(caplog):
    # Regression pin for the guard cf3bf0f0 silently dropped: an empty query
    # must log the required-query error and never instantiate SearchEngine.
    db = make_cli_db()
    config = make_cli_config()
    with patch("tg_agent.cli.commands.search.runtime.init_db", AsyncMock(return_value=(config, db))), \
         patch("tg_agent.cli.commands.search.SearchEngine") as engine_cls, \
         patch("asyncio.run", fake_asyncio_run):
        run(_args(index_now=False, query=""))
    assert "Search query is required" in caplog.text
    engine_cls.assert_not_called()


# ---------------------------------------------------------------------------
# local search
# ---------------------------------------------------------------------------


def test_local_search_with_results(capsys):
    db = make_cli_db()
    config = make_cli_config()
    msg = MagicMock(date="2024-01-01", channel_id=100, text="Hello world")
    result = MagicMock(total=1, query="test", messages=[msg])
    mock_engine = MagicMock()
    mock_engine.search_local = AsyncMock(return_value=result)
    with patch("tg_agent.cli.commands.search.runtime.init_db", AsyncMock(return_value=(config, db))), \
         patch("tg_agent.cli.commands.search.SearchEngine", return_value=mock_engine), \
         patch("asyncio.run", fake_asyncio_run):
        run(_args(mode="local"))
    out = capsys.readouterr().out
    assert "Found 1" in out
    assert "Hello" in out


# ---------------------------------------------------------------------------
# telegram search
# ---------------------------------------------------------------------------


def test_telegram_search_no_clients():
    db = make_cli_db()
    config = make_cli_config()
    pool = MagicMock()
    pool.clients = {}
    pool.disconnect_all = AsyncMock()
    with patch("tg_agent.cli.commands.search.runtime.init_db", AsyncMock(return_value=(config, db))), \
         patch("tg_agent.cli.commands.search.runtime.init_pool", AsyncMock(return_value=(MagicMock(), pool))), \
         patch("asyncio.run", fake_asyncio_run):
        run(_args(mode="telegram"))


def test_telegram_search_with_results(capsys):
    db = make_cli_db()
    config = make_cli_config()
    pool = _make_pool()
    msg = MagicMock(date="2024-01-01", channel_id=100, text="TG result")
    result = MagicMock(total=1, query="test", messages=[msg])
    mock_engine = MagicMock()
    mock_engine.search_telegram = AsyncMock(return_value=result)
    with patch("tg_agent.cli.commands.search.runtime.init_db", AsyncMock(return_value=(config, db))), \
         patch("tg_agent.cli.commands.search.runtime.init_pool", AsyncMock(return_value=(MagicMock(), pool))), \
         patch("tg_agent.cli.commands.search.SearchEngine", return_value=mock_engine), \
         patch("asyncio.run", fake_asyncio_run):
        run(_args(mode="telegram"))
    assert "TG result" in capsys.readouterr().out


def test_mode_tg_alias_dispatches_to_telegram(capsys):
    # `--mode tg` must behave exactly like `telegram` (#1753)
    db = make_cli_db()
    config = make_cli_config()
    pool = _make_pool()
    msg = MagicMock(date="2024-01-01", channel_id=100, text="TG result")
    result = MagicMock(total=1, query="test", messages=[msg])
    mock_engine = MagicMock()
    mock_engine.search_telegram = AsyncMock(return_value=result)
    with patch("tg_agent.cli.commands.search.runtime.init_db", AsyncMock(return_value=(config, db))), \
         patch("tg_agent.cli.commands.search.runtime.init_pool", AsyncMock(return_value=(MagicMock(), pool))), \
         patch("tg_agent.cli.commands.search.SearchEngine", return_value=mock_engine), \
         patch("asyncio.run", fake_asyncio_run):
        run(_args(mode="tg"))
    mock_engine.search_telegram.assert_awaited_once()
    assert "TG result" in capsys.readouterr().out


def test_telegram_search_with_dates_reaches_engine(caplog):
    # #1750: --mode telegram accepts the date window and forwards the raw
    # strings to the engine (parsing happens in SearchEngine, not the CLI).
    db = make_cli_db()
    config = make_cli_config()
    pool = _make_pool()
    result = MagicMock(total=0, query="test", messages=[])
    mock_engine = MagicMock()
    mock_engine.search_telegram = AsyncMock(return_value=result)
    with caplog.at_level(logging.ERROR), \
         patch("tg_agent.cli.commands.search.runtime.init_db", AsyncMock(return_value=(config, db))), \
         patch("tg_agent.cli.commands.search.runtime.init_pool", AsyncMock(return_value=(MagicMock(), pool))), \
         patch("tg_agent.cli.commands.search.SearchEngine", return_value=mock_engine), \
         patch("asyncio.run", fake_asyncio_run):
        run(_args(mode="telegram", date_from="2026-10-19", date_to="2026-11-02"))
    mock_engine.search_telegram.assert_awaited_once()
    kwargs = mock_engine.search_telegram.await_args.kwargs
    assert kwargs["date_from"] == "2026-10-19"
    assert kwargs["date_to"] == "2026-11-02"
    assert "only supported with modes" not in caplog.text


def test_dates_rejected_on_windowless_live_mode(caplog):
    # my_chats/channel have no window at all — the loud guard must stay.
    db = make_cli_db()
    config = make_cli_config()
    pool = _make_pool()
    mock_engine = MagicMock()
    mock_engine.search_my_chats = AsyncMock()
    with caplog.at_level(logging.ERROR), \
         patch("tg_agent.cli.commands.search.runtime.init_db", AsyncMock(return_value=(config, db))), \
         patch("tg_agent.cli.commands.search.runtime.init_pool", AsyncMock(return_value=(MagicMock(), pool))), \
         patch("tg_agent.cli.commands.search.SearchEngine", return_value=mock_engine), \
         patch("asyncio.run", fake_asyncio_run):
        run(_args(mode="my_chats", date_from="2026-10-19"))
    assert "only supported with modes" in caplog.text
    mock_engine.search_my_chats.assert_not_awaited()


# ---------------------------------------------------------------------------
# semantic search
# ---------------------------------------------------------------------------


def test_semantic_search(capsys):
    db = make_cli_db()
    config = make_cli_config()
    result = MagicMock(total=0, query="test", messages=[])
    mock_engine = MagicMock()
    mock_engine.search_semantic = AsyncMock(return_value=result)
    with patch("tg_agent.cli.commands.search.runtime.init_db", AsyncMock(return_value=(config, db))), \
         patch("tg_agent.cli.commands.search.SearchEngine", return_value=mock_engine), \
         patch("asyncio.run", fake_asyncio_run):
        run(_args(mode="semantic", min_length=10, max_length=500))
    assert "Found 0" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# hybrid search
# ---------------------------------------------------------------------------


def test_hybrid_search(capsys):
    db = make_cli_db()
    config = make_cli_config()
    result = MagicMock(total=0, query="test", messages=[])
    mock_engine = MagicMock()
    mock_engine.search_hybrid = AsyncMock(return_value=result)
    with patch("tg_agent.cli.commands.search.runtime.init_db", AsyncMock(return_value=(config, db))), \
         patch("tg_agent.cli.commands.search.SearchEngine", return_value=mock_engine), \
         patch("asyncio.run", fake_asyncio_run):
        run(_args(mode="hybrid", fts=True, min_length=0, max_length=0))
    assert "Found 0" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# my_chats search
# ---------------------------------------------------------------------------


def test_my_chats_search(capsys):
    db = make_cli_db()
    config = make_cli_config()
    pool = _make_pool()
    result = MagicMock(total=0, query="test", messages=[])
    mock_engine = MagicMock()
    mock_engine.search_my_chats = AsyncMock(return_value=result)
    with patch("tg_agent.cli.commands.search.runtime.init_db", AsyncMock(return_value=(config, db))), \
         patch("tg_agent.cli.commands.search.runtime.init_pool", AsyncMock(return_value=(MagicMock(), pool))), \
         patch("tg_agent.cli.commands.search.SearchEngine", return_value=mock_engine), \
         patch("asyncio.run", fake_asyncio_run):
        run(_args(mode="my_chats"))
    assert "Found 0" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# channel search
# ---------------------------------------------------------------------------


def test_channel_search(capsys):
    db = make_cli_db()
    config = make_cli_config()
    pool = _make_pool()
    result = MagicMock(total=0, query="test", messages=[])
    mock_engine = MagicMock()
    mock_engine.search_in_channel = AsyncMock(return_value=result)
    with patch("tg_agent.cli.commands.search.runtime.init_db", AsyncMock(return_value=(config, db))), \
         patch("tg_agent.cli.commands.search.runtime.init_pool", AsyncMock(return_value=(MagicMock(), pool))), \
         patch("tg_agent.cli.commands.search.SearchEngine", return_value=mock_engine), \
         patch("asyncio.run", fake_asyncio_run):
        run(_args(mode="channel", channel_id=100))
    assert "Found 0" in capsys.readouterr().out
