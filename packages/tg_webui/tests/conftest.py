"""Fixtures for the tg_webui package tests (#1720 wave 3).

The web tests run on the core suite's test infrastructure — the real-pool
harness, the in-memory DB, the real-Telegram policy and the safety autouse
fixtures (CLI transport enforcement, session-lock isolation, host-marker
blinding, mock-path leak guard). A conftest only applies to its own directory,
so they are re-exported from ``tests/conftest.py`` here instead of being copied;
the per-item hook is delegated for the same reason.
"""

from __future__ import annotations

from pathlib import Path

import pytest

import tests.conftest as _core
from tests.conftest import *  # noqa: F403 — public fixtures (db, cli_env, real_pool_harness_factory, ...)
from tests.conftest import (  # noqa: F401 — private autouse fixtures, not covered by the star import
    _close_patched_databases,
    _enforce_cli_transport,
    _hide_host_gateway_binding,
    _isolate_session_locks,
    _reuse_test_session_key,
)


def pytest_runtest_setup(item):
    _core.pytest_runtest_setup(item)


_PACKAGE_TESTS_DIR = Path(__file__).resolve().parent


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    # Session-wide hook: when tests/ is collected too, the core conftest already
    # handles its own items, so only this package's items are passed on.
    _core.pytest_collection_modifyitems(
        config, [item for item in items if item.path.is_relative_to(_PACKAGE_TESTS_DIR)]
    )


@pytest.fixture
async def pipeline_client(tmp_path, db, real_pool_harness_factory):
    """Authenticated web client seeded for pipeline route tests.

    Shared fixture (deduped from test_pipelines.py / test_integration_generation.py):
    builds the web app over a real DB, seeds one account + source channel + a
    cached target dialog, and stubs pool.get_dialogs_for_phone so the pipelines
    page can list a publish target offline.
    """
    from types import MethodType

    from webui_helpers import build_web_app, make_auth_client

    from tests.helpers import make_test_config
    from tg_agent.models import Account, Channel

    config = make_test_config(tmp_path)
    harness = real_pool_harness_factory()
    # The app must share the harness's in-memory `db` (CLAUDE.md, "`db` fixture is `:memory:`").
    app, built_db = await build_web_app(config, harness, db=db)
    await built_db.add_account(Account(phone="+100", session_string="sess"))
    await built_db.add_channel(Channel(channel_id=1001, title="Source A"))
    await built_db.repos.dialog_cache.replace_dialogs(
        "+100",
        [
            {
                "channel_id": 77,
                "title": "Target A",
                "username": "targeta",
                "channel_type": "channel",
            }
        ],
    )

    async def _get_dialogs_for_phone(
        self,
        phone,
        include_dm=False,
        mode="full",
        refresh=False,
    ):
        return [
            {
                "channel_id": 77,
                "title": "Target A",
                "username": "targeta",
                "channel_type": "channel",
            }
        ]

    app.state.pool.get_dialogs_for_phone = MethodType(_get_dialogs_for_phone, app.state.pool)

    async with make_auth_client(app) as client:
        yield client

    await app.state.collection_queue.shutdown()
    # `built_db` is the `db` fixture, which closes it itself.
