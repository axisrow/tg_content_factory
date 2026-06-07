"""Tests for src/cli/runtime.py — setup_logging, ensure_data_dirs, redirect/restore logging, init_pool."""
from __future__ import annotations

import asyncio
import logging
from unittest.mock import AsyncMock, MagicMock, patch

from src.cli.runtime import (
    ensure_data_dirs,
    install_log_redaction,
    redirect_logging_to_file,
    restore_logging,
    setup_logging,
)
from src.utils.safe_logging import RedactingFormatter, redact_log_text, text_hash


class TestSetupLogging:
    def test_setup_logging_configures_root_logger(self, tmp_path):
        """setup_logging sets basicConfig on the root logger."""
        root = logging.getLogger()
        original_handlers = root.handlers[:]
        original_level = root.level
        root.handlers.clear()

        try:
            setup_logging(log_path=tmp_path / "app.log")
            assert root.level == logging.INFO
            assert len(root.handlers) >= 1
        finally:
            for h in root.handlers[:]:
                if h not in original_handlers:
                    h.close()
            root.handlers = original_handlers
            root.setLevel(original_level)


class TestEnsureDataDirs:
    def test_ensure_data_dirs_creates_subdirs(self, tmp_path, monkeypatch):
        """ensure_data_dirs creates expected subdirectories."""
        data_root = tmp_path / "data_test"
        monkeypatch.setattr("src.cli.runtime._DATA_ROOT", data_root)

        ensure_data_dirs()

        for sub in ("image", "images", "downloads", "photo_uploads", "telegram_sessions"):
            assert (data_root / sub).is_dir()

    def test_ensure_data_dirs_idempotent(self, tmp_path, monkeypatch):
        """ensure_data_dirs does not fail when dirs already exist."""
        data_root = tmp_path / "data_idem"
        monkeypatch.setattr("src.cli.runtime._DATA_ROOT", data_root)

        ensure_data_dirs()
        ensure_data_dirs()  # should not raise

        assert (data_root / "image").is_dir()


class TestRedirectLogging:
    def test_redirect_logging_to_file(self, tmp_path):
        """redirect_logging_to_file replaces console handlers with a file handler."""
        root = logging.getLogger()
        original_handlers = root.handlers[:]

        # Add a console handler if none
        console = logging.StreamHandler()
        root.addHandler(console)

        try:
            log_file = str(tmp_path / "tui_test.log")
            removed = redirect_logging_to_file(log_file)

            # Should have removed the console handler
            assert console in removed
            # Should have added a file handler
            file_handlers = [h for h in root.handlers if isinstance(h, logging.FileHandler)]
            assert len(file_handlers) >= 1
        finally:
            # Clean up
            for h in root.handlers[:]:
                if isinstance(h, logging.FileHandler):
                    h.close()
                    root.removeHandler(h)
            root.handlers = original_handlers

    def test_restore_logging(self, tmp_path):
        """restore_logging restores previously removed console handlers."""
        root = logging.getLogger()
        original_handlers = root.handlers[:]

        console = logging.StreamHandler()
        root.addHandler(console)

        try:
            log_file = str(tmp_path / "tui_restore.log")
            removed = redirect_logging_to_file(log_file)
            restore_logging(removed)

            # Console handler should be back
            assert console in root.handlers
            # Plain TUI FileHandler should be gone; RotatingFileHandler (app.log) may remain
            plain_file_handlers = [h for h in root.handlers if type(h) is logging.FileHandler]
            assert len(plain_file_handlers) == 0
        finally:
            root.handlers = original_handlers

    def test_restore_logging_single_handler(self, tmp_path):
        """restore_logging accepts a single handler (not a list)."""
        root = logging.getLogger()
        original_handlers = root.handlers[:]

        console = logging.StreamHandler()
        root.addHandler(console)

        try:
            log_file = str(tmp_path / "tui_single.log")
            redirect_logging_to_file(log_file)
            restore_logging(console)

            assert console in root.handlers
        finally:
            root.handlers = original_handlers

    def test_restore_logging_none(self, tmp_path):
        """restore_logging with None does nothing."""
        root = logging.getLogger()
        original_handlers = root.handlers[:]
        try:
            restore_logging(None)
            # Should not raise or change anything
        finally:
            root.handlers = original_handlers


class TestRedactingFilter:
    def test_redact_log_text_masks_phone_and_query(self):
        out = redact_log_text("auth.send_code start phone=+1234567890")
        assert "+1234567890" not in out
        assert "phone=+12...7890" in out

        out = redact_log_text("Search query 'kremlin news' (id=5)")
        assert "kremlin news" not in out
        assert f"hash:{text_hash('kremlin news')}" in out

    def test_redact_log_text_preserves_numeric_metadata(self):
        # Short numeric fields and hex hashes must not be mangled.
        line = "timeout command_id=123 duration_ms=4500 query_hash=9a8701151ccd query_len=2"
        assert redact_log_text(line) == line

    def test_formatter_redacts_output_without_mutating_record(self):
        record = logging.LogRecord(
            name="x", level=logging.INFO, pathname=__file__, lineno=1,
            msg="resolve for %s", args=("+1234567890",), exc_info=None,
        )
        out = RedactingFormatter("%(message)s").format(record)
        assert "+1234567890" not in out
        assert "+12...7890" in out
        # The shared record is untouched — record-level consumers (caplog) see raw.
        assert record.getMessage() == "resolve for +1234567890"

    def test_install_log_redaction_idempotent(self):
        handler = logging.StreamHandler()
        install_log_redaction(handler)
        first = handler.formatter
        install_log_redaction(handler)
        assert isinstance(handler.formatter, RedactingFormatter)
        assert handler.formatter is first

    def test_setup_logging_redacts_through_handler(self, tmp_path):
        log_file = tmp_path / "redact.log"
        root = logging.getLogger()
        original_handlers = root.handlers[:]
        original_level = root.level
        root.handlers.clear()
        try:
            setup_logging(log_path=log_file)
            logging.getLogger("redact.test").info("auth phone=+1234567890 query='top secret'")
            for h in root.handlers:
                h.flush()
            contents = log_file.read_text(encoding="utf-8")
            assert "+1234567890" not in contents
            assert "top secret" not in contents
            assert "+12...7890" in contents
        finally:
            for h in root.handlers[:]:
                if h not in original_handlers:
                    h.close()
            root.handlers = original_handlers
            root.setLevel(original_level)


class TestInitPool:
    def test_init_pool_with_config_values(self, tmp_path):
        """init_pool uses config telegram api_id and api_hash when set."""
        from src.cli.runtime import init_pool
        from src.config import AppConfig, TelegramRuntimeConfig

        config = AppConfig()
        config.telegram.api_id = 12345
        config.telegram.api_hash = "test_hash_123"
        config.telegram_runtime = TelegramRuntimeConfig()

        db = MagicMock()
        db.get_setting = AsyncMock(return_value=None)

        mock_pool = MagicMock()
        mock_pool.initialize = AsyncMock()

        loop = asyncio.new_event_loop()
        try:
            with patch("src.cli.runtime.ClientPool", return_value=mock_pool) as mock_pool_cls:
                auth, pool = loop.run_until_complete(init_pool(config, db))

                mock_pool_cls.assert_called_once()
                call_kwargs = mock_pool_cls.call_args
                auth_arg = call_kwargs[0][0]
                assert auth_arg._api_id == 12345
                assert auth_arg._api_hash == "test_hash_123"
        finally:
            loop.close()

    def test_init_pool_fallback_to_db_settings(self, tmp_path):
        """init_pool reads api_id/api_hash from DB when config has defaults."""
        from src.cli.runtime import init_pool
        from src.config import AppConfig, TelegramRuntimeConfig

        config = AppConfig()
        config.telegram.api_id = 0
        config.telegram.api_hash = ""
        config.telegram_runtime = TelegramRuntimeConfig()

        db = MagicMock()
        db.get_setting = AsyncMock(side_effect=lambda key: {
            "tg_api_id": "98765",
            "tg_api_hash": "db_hash_value",
        }.get(key))

        mock_pool = MagicMock()
        mock_pool.initialize = AsyncMock()

        loop = asyncio.new_event_loop()
        try:
            with patch("src.cli.runtime.ClientPool", return_value=mock_pool) as mock_pool_cls:
                auth, pool = loop.run_until_complete(init_pool(config, db))

                auth_arg = mock_pool_cls.call_args[0][0]
                assert auth_arg._api_id == 98765
                assert auth_arg._api_hash == "db_hash_value"
        finally:
            loop.close()

    def test_init_pool_no_db_settings_fallback(self, tmp_path):
        """init_pool handles missing DB settings gracefully."""
        from src.cli.runtime import init_pool
        from src.config import AppConfig, TelegramRuntimeConfig

        config = AppConfig()
        config.telegram.api_id = 0
        config.telegram.api_hash = ""
        config.telegram_runtime = TelegramRuntimeConfig()

        db = MagicMock()
        db.get_setting = AsyncMock(return_value=None)

        mock_pool = MagicMock()
        mock_pool.initialize = AsyncMock()

        loop = asyncio.new_event_loop()
        try:
            with patch("src.cli.runtime.ClientPool", return_value=mock_pool) as mock_pool_cls:
                auth, pool = loop.run_until_complete(init_pool(config, db))

                auth_arg = mock_pool_cls.call_args[0][0]
                # Should still create an auth object with 0/empty
                assert auth_arg._api_id == 0
        finally:
            loop.close()

    def test_init_pool_passes_requested_phones_to_pool_initialize(self, tmp_path):
        """init_pool can initialize only the requested live account."""
        from src.cli.runtime import init_pool
        from src.config import AppConfig, TelegramRuntimeConfig

        config = AppConfig()
        config.telegram.api_id = 12345
        config.telegram.api_hash = "test_hash_123"
        config.telegram_runtime = TelegramRuntimeConfig()

        db = MagicMock()
        db.get_setting = AsyncMock(return_value=None)

        mock_pool = MagicMock()
        mock_pool.initialize = AsyncMock()

        loop = asyncio.new_event_loop()
        try:
            with patch("src.cli.runtime.ClientPool", return_value=mock_pool):
                loop.run_until_complete(init_pool(config, db, phones=("+1000",)))

                mock_pool.initialize.assert_awaited_once_with(phones=("+1000",))
        finally:
            loop.close()
