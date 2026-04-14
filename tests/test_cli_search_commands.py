"""Tests for src/cli/commands/search.py — CLI search subcommands."""
from __future__ import annotations

import argparse
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from src.cli.commands.search import run


def _fake_asyncio_run(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def _args(**overrides):
    defaults = {"config": "config.yaml", "mode": "local", "query": "test", "limit": 20,
                "channel_id": None, "min_length": 0, "max_length": 0, "fts": False,
                "index_now": False, "reset_index": False}
    defaults.update(overrides)
    return argparse.Namespace(**defaults)


def _make_db():
    db = MagicMock()
    db.close = AsyncMock()
    db.repos.messages.reset_embeddings_index = AsyncMock()
    db.search_messages = AsyncMock(return_value=([], 0))
    return db


def _make_config():
    return MagicMock()


def _make_pool():
    pool = MagicMock()
    pool.clients = {"+1234567890": MagicMock()}
    pool.disconnect_all = AsyncMock()
    return pool


# ---------------------------------------------------------------------------
# index_now
# ---------------------------------------------------------------------------


def test_index_now(capsys):
    db = _make_db()
    config = _make_config()
    mock_es = MagicMock()
    mock_es.index_pending_messages = AsyncMock(return_value=42)
    with patch("src.cli.commands.search.runtime.init_db", AsyncMock(return_value=(config, db))), \
         patch("src.cli.commands.search.EmbeddingService", return_value=mock_es), \
         patch("asyncio.run", _fake_asyncio_run):
        run(_args(index_now=True, reset_index=False, query=""))
    assert "42" in capsys.readouterr().out


def test_index_now_with_reset(capsys):
    db = _make_db()
    config = _make_config()
    mock_es = MagicMock()
    mock_es.index_pending_messages = AsyncMock(return_value=10)
    with patch("src.cli.commands.search.runtime.init_db", AsyncMock(return_value=(config, db))), \
         patch("src.cli.commands.search.EmbeddingService", return_value=mock_es), \
         patch("asyncio.run", _fake_asyncio_run):
        run(_args(index_now=True, reset_index=True, query=""))
    db.repos.messages.reset_embeddings_index.assert_called_once()


# ---------------------------------------------------------------------------
# no query
# ---------------------------------------------------------------------------


def test_no_query():
    db = _make_db()
    config = _make_config()
    with patch("src.cli.commands.search.runtime.init_db", AsyncMock(return_value=(config, db))), \
         patch("asyncio.run", _fake_asyncio_run):
        run(_args(index_now=False, query=""))


# ---------------------------------------------------------------------------
# local search
# ---------------------------------------------------------------------------


def test_local_search_with_results(capsys):
    db = _make_db()
    config = _make_config()
    msg = MagicMock(date="2024-01-01", channel_id=100, text="Hello world")
    result = MagicMock(total=1, query="test", messages=[msg])
    mock_engine = MagicMock()
    mock_engine.search_local = AsyncMock(return_value=result)
    with patch("src.cli.commands.search.runtime.init_db", AsyncMock(return_value=(config, db))), \
         patch("src.cli.commands.search.SearchEngine", return_value=mock_engine), \
         patch("asyncio.run", _fake_asyncio_run):
        run(_args(mode="local"))
    out = capsys.readouterr().out
    assert "Found 1" in out
    assert "Hello" in out


# ---------------------------------------------------------------------------
# telegram search
# ---------------------------------------------------------------------------


def test_telegram_search_no_clients():
    db = _make_db()
    config = _make_config()
    pool = MagicMock()
    pool.clients = {}
    pool.disconnect_all = AsyncMock()
    with patch("src.cli.commands.search.runtime.init_db", AsyncMock(return_value=(config, db))), \
         patch("src.cli.commands.search.runtime.init_pool", AsyncMock(return_value=(MagicMock(), pool))), \
         patch("asyncio.run", _fake_asyncio_run):
        run(_args(mode="telegram"))


def test_telegram_search_with_results(capsys):
    db = _make_db()
    config = _make_config()
    pool = _make_pool()
    msg = MagicMock(date="2024-01-01", channel_id=100, text="TG result")
    result = MagicMock(total=1, query="test", messages=[msg])
    mock_engine = MagicMock()
    mock_engine.search_telegram = AsyncMock(return_value=result)
    with patch("src.cli.commands.search.runtime.init_db", AsyncMock(return_value=(config, db))), \
         patch("src.cli.commands.search.runtime.init_pool", AsyncMock(return_value=(MagicMock(), pool))), \
         patch("src.cli.commands.search.SearchEngine", return_value=mock_engine), \
         patch("asyncio.run", _fake_asyncio_run):
        run(_args(mode="telegram"))
    assert "TG result" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# semantic search
# ---------------------------------------------------------------------------


def test_semantic_search(capsys):
    db = _make_db()
    config = _make_config()
    result = MagicMock(total=0, query="test", messages=[])
    mock_engine = MagicMock()
    mock_engine.search_semantic = AsyncMock(return_value=result)
    with patch("src.cli.commands.search.runtime.init_db", AsyncMock(return_value=(config, db))), \
         patch("src.cli.commands.search.SearchEngine", return_value=mock_engine), \
         patch("asyncio.run", _fake_asyncio_run):
        run(_args(mode="semantic", min_length=10, max_length=500))
    assert "Found 0" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# hybrid search
# ---------------------------------------------------------------------------


def test_hybrid_search(capsys):
    db = _make_db()
    config = _make_config()
    result = MagicMock(total=0, query="test", messages=[])
    mock_engine = MagicMock()
    mock_engine.search_hybrid = AsyncMock(return_value=result)
    with patch("src.cli.commands.search.runtime.init_db", AsyncMock(return_value=(config, db))), \
         patch("src.cli.commands.search.SearchEngine", return_value=mock_engine), \
         patch("asyncio.run", _fake_asyncio_run):
        run(_args(mode="hybrid", fts=True, min_length=0, max_length=0))
    assert "Found 0" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# my_chats search
# ---------------------------------------------------------------------------


def test_my_chats_search(capsys):
    db = _make_db()
    config = _make_config()
    pool = _make_pool()
    result = MagicMock(total=0, query="test", messages=[])
    mock_engine = MagicMock()
    mock_engine.search_my_chats = AsyncMock(return_value=result)
    with patch("src.cli.commands.search.runtime.init_db", AsyncMock(return_value=(config, db))), \
         patch("src.cli.commands.search.runtime.init_pool", AsyncMock(return_value=(MagicMock(), pool))), \
         patch("src.cli.commands.search.SearchEngine", return_value=mock_engine), \
         patch("asyncio.run", _fake_asyncio_run):
        run(_args(mode="my_chats"))
    assert "Found 0" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# channel search
# ---------------------------------------------------------------------------


def test_channel_search(capsys):
    db = _make_db()
    config = _make_config()
    pool = _make_pool()
    result = MagicMock(total=0, query="test", messages=[])
    mock_engine = MagicMock()
    mock_engine.search_in_channel = AsyncMock(return_value=result)
    with patch("src.cli.commands.search.runtime.init_db", AsyncMock(return_value=(config, db))), \
         patch("src.cli.commands.search.runtime.init_pool", AsyncMock(return_value=(MagicMock(), pool))), \
         patch("src.cli.commands.search.SearchEngine", return_value=mock_engine), \
         patch("asyncio.run", _fake_asyncio_run):
        run(_args(mode="channel", channel_id=100))
    assert "Found 0" in capsys.readouterr().out
