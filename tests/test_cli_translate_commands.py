"""Tests for src/cli/commands/translate.py — CLI translate subcommands."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

from src.cli.commands.translate import run
from tests.helpers import cli_ns, fake_asyncio_run, make_cli_config, make_cli_db


def _args(**overrides):
    defaults = {"config": "config.yaml"}
    defaults.update(overrides)
    return cli_ns(**defaults)


def _patches(db, config=None):
    config = config or make_cli_config()
    return (
        patch("src.cli.commands.translate.runtime.init_db", AsyncMock(return_value=(config, db))),
        patch("asyncio.run", fake_asyncio_run),
    )


# ---------------------------------------------------------------------------
# stats
# ---------------------------------------------------------------------------


def test_stats_empty(capsys):
    db = make_cli_db()
    db.repos.messages.get_language_stats = AsyncMock(return_value=[])
    with _patches(db)[0], _patches(db)[1]:
        run(_args(translate_action="stats"))
    assert "No language data" in capsys.readouterr().out


def test_stats_with_data(capsys):
    db = make_cli_db()
    db.repos.messages.get_language_stats = AsyncMock(return_value=[("ru", 100), ("en", 50)])
    with _patches(db)[0], _patches(db)[1]:
        run(_args(translate_action="stats"))
    out = capsys.readouterr().out
    assert "ru" in out
    assert "100" in out
    assert "Total" in out


# ---------------------------------------------------------------------------
# detect
# ---------------------------------------------------------------------------


def test_detect_single_batch(capsys):
    db = make_cli_db()
    db.repos.messages.backfill_language_detection = AsyncMock(return_value=50)
    with _patches(db)[0], _patches(db)[1]:
        run(_args(translate_action="detect", batch_size=5000))
    out = capsys.readouterr().out
    assert "50" in out
    assert "complete" in out.lower()


def test_detect_multiple_batches(capsys):
    db = make_cli_db()
    db.repos.messages.backfill_language_detection = AsyncMock(side_effect=[5000, 5000, 100])
    with _patches(db)[0], _patches(db)[1]:
        run(_args(translate_action="detect", batch_size=5000))
    out = capsys.readouterr().out
    assert "10100" in out


# ---------------------------------------------------------------------------
# run (batch translate)
# ---------------------------------------------------------------------------


def test_run_no_messages(capsys):
    db = make_cli_db()
    db.repos.messages.get_untranslated_messages = AsyncMock(return_value=[])
    mock_ps = MagicMock()
    mock_ps.load_db_providers = AsyncMock(return_value=0)
    mock_ts = MagicMock()
    mock_ts.translate_batch = AsyncMock(return_value=[])
    with _patches(db)[0], _patches(db)[1], \
         patch("src.services.provider_service.AgentProviderService", return_value=mock_ps), \
         patch("src.services.translation_service.TranslationService", return_value=mock_ts):
        run(_args(translate_action="run", target="en", source_filter="", limit=100))
    assert "No messages" in capsys.readouterr().out


def test_run_with_messages(capsys):
    db = make_cli_db()
    msgs = [MagicMock()]
    db.repos.messages.get_untranslated_messages = AsyncMock(return_value=msgs)
    db.repos.messages.update_translation = AsyncMock()
    mock_ps = MagicMock()
    mock_ps.load_db_providers = AsyncMock(return_value=0)
    mock_ts = MagicMock()
    mock_ts.translate_batch = AsyncMock(return_value=[(1, "translated text")])
    with _patches(db)[0], _patches(db)[1], \
         patch("src.services.provider_service.AgentProviderService", return_value=mock_ps), \
         patch("src.services.translation_service.TranslationService", return_value=mock_ts):
        run(_args(translate_action="run", target="en", source_filter="ru", limit=100))
    out = capsys.readouterr().out
    assert "Translating" in out
    assert "1/1" in out


# ---------------------------------------------------------------------------
# message
# ---------------------------------------------------------------------------


def test_message_not_found(capsys):
    db = make_cli_db()
    db.repos.messages.get_by_id = AsyncMock(return_value=None)
    with _patches(db)[0], _patches(db)[1]:
        run(_args(translate_action="message", message_id=999, target="en"))
    assert "not found" in capsys.readouterr().out


def test_message_no_text(capsys):
    db = make_cli_db()
    msg = MagicMock(text="   ")
    db.repos.messages.get_by_id = AsyncMock(return_value=msg)
    with _patches(db)[0], _patches(db)[1]:
        run(_args(translate_action="message", message_id=1, target="en"))
    assert "no text" in capsys.readouterr().out


def test_message_with_text(capsys):
    db = make_cli_db()
    msg = MagicMock(text="Привет мир")
    db.repos.messages.get_by_id = AsyncMock(return_value=msg)
    db.repos.messages.update_translation = AsyncMock()
    mock_ps = MagicMock()
    mock_ps.load_db_providers = AsyncMock(return_value=0)
    mock_ts = MagicMock()
    mock_ts.translate_batch = AsyncMock(return_value=[(1, "Hello world")])
    with _patches(db)[0], _patches(db)[1], \
         patch("src.services.provider_service.AgentProviderService", return_value=mock_ps), \
         patch("src.services.translation_service.TranslationService", return_value=mock_ts):
        run(_args(translate_action="message", message_id=1, target="en"))
    out = capsys.readouterr().out
    assert "Hello world" in out
    assert "Привет мир" in out


def test_message_translation_failed(capsys):
    db = make_cli_db()
    msg = MagicMock(text="Привет")
    db.repos.messages.get_by_id = AsyncMock(return_value=msg)
    mock_ps = MagicMock()
    mock_ps.load_db_providers = AsyncMock(return_value=0)
    mock_ts = MagicMock()
    mock_ts.translate_batch = AsyncMock(return_value=[])
    with _patches(db)[0], _patches(db)[1], \
         patch("src.services.provider_service.AgentProviderService", return_value=mock_ps), \
         patch("src.services.translation_service.TranslationService", return_value=mock_ts):
        run(_args(translate_action="message", message_id=1, target="en"))
    assert "failed" in capsys.readouterr().out.lower()
