"""Tests for CLI translate commands: stats, detect, run, message."""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

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

    with patch("src.cli.commands.translate.runtime.init_db", side_effect=fake_init_db):
        yield cli_db


def _prep_prov_mock(mock_prov_svc):
    """Configure the class-level mock so that an instance's load_db_providers() is awaitable."""
    mock_prov_svc.return_value.load_db_providers = AsyncMock(return_value=0)


class TestStats:
    @patch("src.database.repositories.messages.MessagesRepository.get_language_stats")
    def test_stats_no_data(self, mock_stats, cli_env, capsys):
        mock_stats.return_value = []
        from src.cli.commands.translate import run
        run(_ns(translate_action="stats"))
        out = capsys.readouterr().out
        assert "No language data" in out

    @patch("src.database.repositories.messages.MessagesRepository.get_language_stats")
    def test_stats_with_data(self, mock_stats, cli_env, capsys):
        mock_stats.return_value = [("en", 150), ("ru", 300)]
        from src.cli.commands.translate import run
        run(_ns(translate_action="stats"))
        out = capsys.readouterr().out
        assert "en" in out
        assert "ru" in out
        assert "150" in out
        assert "300" in out
        assert "450" in out

    @patch("src.database.repositories.messages.MessagesRepository.get_language_stats")
    def test_stats_table_format(self, mock_stats, cli_env, capsys):
        mock_stats.return_value = [("de", 10)]
        from src.cli.commands.translate import run
        run(_ns(translate_action="stats"))
        out = capsys.readouterr().out
        assert "Language" in out
        assert "Messages" in out
        assert "---" in out


class TestDetect:
    @patch("src.database.repositories.messages.MessagesRepository.backfill_language_detection")
    def test_detect_no_messages(self, mock_backfill, cli_env, capsys):
        mock_backfill.return_value = 0
        from src.cli.commands.translate import run
        run(_ns(translate_action="detect"))
        out = capsys.readouterr().out
        assert "0 messages updated" in out

    @patch("src.database.repositories.messages.MessagesRepository.backfill_language_detection")
    def test_detect_single_batch(self, mock_backfill, cli_env, capsys):
        mock_backfill.return_value = 100
        from src.cli.commands.translate import run
        run(_ns(translate_action="detect"))
        out = capsys.readouterr().out
        assert "100 messages updated" in out

    @patch("src.database.repositories.messages.MessagesRepository.backfill_language_detection")
    def test_detect_multi_batch(self, mock_backfill, cli_env, capsys):
        batch_size = 5000
        mock_backfill.side_effect = [batch_size, batch_size, 300]
        from src.cli.commands.translate import run
        run(_ns(translate_action="detect", batch_size=batch_size))
        out = capsys.readouterr().out
        assert "10300 messages updated" in out

    @patch("src.database.repositories.messages.MessagesRepository.backfill_language_detection")
    def test_detect_custom_batch_size(self, mock_backfill, cli_env, capsys):
        mock_backfill.side_effect = [100, 50]
        from src.cli.commands.translate import run
        run(_ns(translate_action="detect", batch_size=100))
        out = capsys.readouterr().out
        assert "150 messages updated" in out


class TestRun:
    @patch("src.services.translation_service.TranslationService")
    @patch("src.services.provider_service.AgentProviderService")
    @patch("src.database.repositories.messages.MessagesRepository.get_untranslated_messages")
    def test_run_no_messages(self, mock_get, mock_prov_svc, mock_trans_svc, cli_env, capsys):
        _prep_prov_mock(mock_prov_svc)
        mock_get.return_value = []
        from src.cli.commands.translate import run
        run(_ns(translate_action="run"))
        out = capsys.readouterr().out
        assert "No messages to translate" in out

    @patch("src.database.repositories.messages.MessagesRepository.update_translation", new_callable=AsyncMock)
    @patch("src.services.translation_service.TranslationService")
    @patch("src.services.provider_service.AgentProviderService")
    @patch("src.database.repositories.messages.MessagesRepository.get_untranslated_messages")
    def test_run_translates_messages(self, mock_get, mock_prov_svc, mock_trans_svc, mock_update, cli_env, capsys):
        _prep_prov_mock(mock_prov_svc)
        msgs = [SimpleNamespace(id=1), SimpleNamespace(id=2)]
        mock_get.return_value = msgs
        svc = mock_trans_svc.return_value
        svc.translate_batch = AsyncMock(return_value=[(1, "hello"), (2, "world")])
        from src.cli.commands.translate import run
        run(_ns(translate_action="run", target="en"))
        out = capsys.readouterr().out
        assert "Translating 2 messages" in out
        assert "Translated 2/2" in out

    @patch("src.services.translation_service.TranslationService")
    @patch("src.services.provider_service.AgentProviderService")
    @patch("src.database.repositories.messages.MessagesRepository.get_untranslated_messages")
    def test_run_with_source_filter(self, mock_get, mock_prov_svc, mock_trans_svc, cli_env, capsys):
        _prep_prov_mock(mock_prov_svc)
        msgs = [SimpleNamespace(id=1)]
        mock_get.return_value = msgs
        svc = mock_trans_svc.return_value
        svc.translate_batch = AsyncMock(return_value=[])
        from src.cli.commands.translate import run
        run(_ns(translate_action="run", target="en", source_filter="ru,de", limit=50))
        _, kwargs = mock_get.call_args
        assert kwargs.get("source_langs") == ["ru", "de"] or mock_get.call_args[1].get("source_langs") == ["ru", "de"]

    @patch("src.services.translation_service.TranslationService")
    @patch("src.services.provider_service.AgentProviderService")
    @patch("src.database.repositories.messages.MessagesRepository.get_untranslated_messages")
    def test_run_with_limit(self, mock_get, mock_prov_svc, mock_trans_svc, cli_env, capsys):
        _prep_prov_mock(mock_prov_svc)
        mock_get.return_value = []
        from src.cli.commands.translate import run
        run(_ns(translate_action="run", limit=10))
        _, kwargs = mock_get.call_args
        assert kwargs.get("limit") == 10 or mock_get.call_args[1].get("limit") == 10

    @patch("src.database.repositories.messages.MessagesRepository.update_translation", new_callable=AsyncMock)
    @patch("src.services.translation_service.TranslationService")
    @patch("src.services.provider_service.AgentProviderService")
    @patch("src.database.repositories.messages.MessagesRepository.get_untranslated_messages")
    def test_run_partial_failure(self, mock_get, mock_prov_svc, mock_trans_svc, mock_update, cli_env, capsys):
        _prep_prov_mock(mock_prov_svc)
        msgs = [SimpleNamespace(id=1), SimpleNamespace(id=2), SimpleNamespace(id=3)]
        mock_get.return_value = msgs
        svc = mock_trans_svc.return_value
        svc.translate_batch = AsyncMock(return_value=[(1, "hello")])
        from src.cli.commands.translate import run
        run(_ns(translate_action="run"))
        out = capsys.readouterr().out
        assert "Translated 1/3" in out


class TestMessage:
    @patch("src.database.repositories.messages.MessagesRepository.get_by_id")
    def test_message_not_found(self, mock_get, cli_env, capsys):
        mock_get.return_value = None
        from src.cli.commands.translate import run
        run(_ns(translate_action="message", message_id=999))
        out = capsys.readouterr().out
        assert "not found" in out

    @patch("src.database.repositories.messages.MessagesRepository.get_by_id")
    def test_message_no_text(self, mock_get, cli_env, capsys):
        mock_get.return_value = SimpleNamespace(id=1, text="")
        from src.cli.commands.translate import run
        run(_ns(translate_action="message", message_id=1))
        out = capsys.readouterr().out
        assert "no text" in out

    @patch("src.database.repositories.messages.MessagesRepository.update_translation", new_callable=AsyncMock)
    @patch("src.services.translation_service.TranslationService")
    @patch("src.services.provider_service.AgentProviderService")
    @patch("src.database.repositories.messages.MessagesRepository.get_by_id")
    def test_message_success(self, mock_get, mock_prov_svc, mock_trans_svc, mock_update, cli_env, capsys):
        _prep_prov_mock(mock_prov_svc)
        mock_get.return_value = SimpleNamespace(id=1, text="Привет мир")
        svc = mock_trans_svc.return_value
        svc.translate_batch = AsyncMock(return_value=[(1, "Hello world")])
        from src.cli.commands.translate import run
        run(_ns(translate_action="message", message_id=1, target="en"))
        out = capsys.readouterr().out
        assert "Original" in out
        assert "Hello world" in out

    @patch("src.services.translation_service.TranslationService")
    @patch("src.services.provider_service.AgentProviderService")
    @patch("src.database.repositories.messages.MessagesRepository.get_by_id")
    def test_message_translation_failed(self, mock_get, mock_prov_svc, mock_trans_svc, cli_env, capsys):
        _prep_prov_mock(mock_prov_svc)
        mock_get.return_value = SimpleNamespace(id=1, text="Some text here")
        svc = mock_trans_svc.return_value
        svc.translate_batch = AsyncMock(return_value=[])
        from src.cli.commands.translate import run
        run(_ns(translate_action="message", message_id=1))
        out = capsys.readouterr().out
        assert "Translation failed" in out

    @patch("src.database.repositories.messages.MessagesRepository.update_translation", new_callable=AsyncMock)
    @patch("src.services.translation_service.TranslationService")
    @patch("src.services.provider_service.AgentProviderService")
    @patch("src.database.repositories.messages.MessagesRepository.get_by_id")
    def test_message_with_custom_target(self, mock_get, mock_prov_svc, mock_trans_svc, mock_update, cli_env, capsys):
        _prep_prov_mock(mock_prov_svc)
        mock_get.return_value = SimpleNamespace(id=1, text="Hello")
        svc = mock_trans_svc.return_value
        svc.translate_batch = AsyncMock(return_value=[(1, "Hallo")])
        from src.cli.commands.translate import run
        run(_ns(translate_action="message", message_id=1, target="de"))
        out = capsys.readouterr().out
        assert "Hallo" in out
