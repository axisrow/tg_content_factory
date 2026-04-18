"""Tests for web routes dialogs and remaining small modules."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.web.routes import dialogs as dialogs_mod


# --- dialogs route helpers ---


def test_dialogs_router_defined():
    assert hasattr(dialogs_mod, "router")


def test_dialogs_module_imports():
    from src.web.routes.dialogs import router

    assert router is not None


# --- settings routes helper tests ---


def test_settings_router_defined():
    from src.web.routes.settings import router

    assert router is not None


# --- collection queue tests ---


async def test_collection_queue_import():
    from src.collection_queue import CollectionQueue

    assert CollectionQueue is not None


# --- dialog_cache repository tests ---


async def test_dialog_cache_repo_import():
    from src.database.repositories.dialog_cache import DialogCacheRepository

    assert DialogCacheRepository is not None


# --- panel_auth tests ---


def test_panel_auth_import():
    from src.web.panel_auth import is_public_path, sanitize_next, login_redirect_url

    assert is_public_path("/health")
    assert is_public_path("/static/css/style.css")
    assert not is_public_path("/dashboard")
    assert sanitize_next(None) == "/"
    assert sanitize_next("//evil.com") == "/"
    assert sanitize_next("/dashboard") == "/dashboard"
    assert "next=" in login_redirect_url("/settings")


# --- web app tests ---


def test_web_app_import():
    from src.web.app import create_app

    assert callable(create_app)


# --- scheduler service tests ---


def test_scheduler_service_import():
    from src.scheduler import service as svc_mod

    assert svc_mod is not None


# --- database settings repository ---


async def test_settings_repo_import():
    from src.database.repositories.settings import SettingsRepository

    assert SettingsRepository is not None


# --- channel stats repository ---


async def test_channel_stats_repo_import():
    from src.database.repositories.channel_stats import ChannelStatsRepository

    assert ChannelStatsRepository is not None


# --- generation_runs repository ---


async def test_generation_runs_repo_import():
    from src.database.repositories.generation_runs import GenerationRunsRepository

    assert GenerationRunsRepository is not None


# --- content_pipelines repository ---


async def test_content_pipelines_repo_import():
    from src.database.repositories.content_pipelines import ContentPipelinesRepository

    assert ContentPipelinesRepository is not None


# --- runtime_snapshots repository ---


async def test_runtime_snapshots_repo_import():
    from src.database.repositories.runtime_snapshots import RuntimeSnapshotsRepository

    assert RuntimeSnapshotsRepository is not None
