"""Tests for src/cli/runtime.py — setup_logging, ensure_data_dirs, redirect/restore logging, init_pool."""
from __future__ import annotations

import asyncio
import logging
from unittest.mock import AsyncMock, MagicMock, patch

from src.cli.runtime import (
    ensure_data_dirs,
    redirect_logging_to_file,
    restore_logging,
    setup_logging,
)


class TestSetupLogging:
    def test_setup_logging_configures_root_logger(self):
        """setup_logging sets basicConfig on the root logger."""
        root = logging.getLogger()
        # Clear any existing handlers to ensure clean test
        original_handlers = root.handlers[:]
        original_level = root.level
        root.handlers.clear()

        try:
            setup_logging()
            assert root.level == logging.INFO
            assert len(root.handlers) >= 1
        finally:
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

        with patch("src.cli.runtime.ClientPool", return_value=mock_pool) as mock_pool_cls:
            auth, pool = asyncio.run(init_pool(config, db))

            mock_pool_cls.assert_called_once()
            call_kwargs = mock_pool_cls.call_args
            auth_arg = call_kwargs[0][0]
            assert auth_arg._api_id == 12345
            assert auth_arg._api_hash == "test_hash_123"

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

        with patch("src.cli.runtime.ClientPool", return_value=mock_pool) as mock_pool_cls:
            auth, pool = asyncio.run(init_pool(config, db))

            auth_arg = mock_pool_cls.call_args[0][0]
            assert auth_arg._api_id == 98765
            assert auth_arg._api_hash == "db_hash_value"

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

        with patch("src.cli.runtime.ClientPool", return_value=mock_pool) as mock_pool_cls:
            auth, pool = asyncio.run(init_pool(config, db))

            auth_arg = mock_pool_cls.call_args[0][0]
            # Should still create an auth object with 0/empty
            assert auth_arg._api_id == 0
