"""Fast offline smoke preflight over real project functions.

This file is the curated ``-m smoke`` set: a handful of fast, offline checks
that each exercise a *real* critical path (no mocks of the thing under test,
no network, no opt-in gates). The goal is a sub-second "does the app even
stand up" gate to run before the full suite — if any of these fail, something
fundamental is broken and the rest of the run is not worth starting.

Rules for this file:
- offline only — never touch the network or a live provider/Telegram;
- fast — in-memory DB or pure construction, no file-backed DB on disk;
- real — call the actual project entrypoint, do not assert on a mock.

Most tests here are ``integration`` by level (they stand up the DB / web app);
``smoke`` is an orthogonal axis layered on top, so they carry both markers.
"""

from __future__ import annotations

import httpx
import pytest

from src.database import Database
from tests.helpers import build_web_app, make_test_config

pytestmark = pytest.mark.smoke


def test_package_imports() -> None:
    """The top-level entrypoint module imports cleanly."""
    import src.main

    assert hasattr(src.main, "main")


def test_cli_parser_builds() -> None:
    """The full CLI argument parser assembles without error."""
    from src.cli.parser import build_parser

    parser = build_parser()
    assert parser.prog


def test_identifier_parsing_real_function() -> None:
    """Channel-identifier parsing handles the documented separators."""
    from src.parsers import extract_identifiers, parse_identifiers

    assert parse_identifiers("@one, @two; @three") == ["@one", "@two", "@three"]
    # extract_identifiers pulls real t.me links and @username tokens.
    found = extract_identifiers("see https://t.me/example and @handle")
    assert "https://t.me/example" in found
    assert "@handle" in found


def test_default_config_constructs() -> None:
    """AppConfig builds with defaults (no config.yaml on disk required)."""
    from src.config import AppConfig

    config = AppConfig()
    assert config.database.path


def test_agent_tool_registry_resolves() -> None:
    """The agent tool allow-list derives from the permission categories."""
    from src.agent.tools.permissions import get_all_allowed_tools

    tools = get_all_allowed_tools()
    assert isinstance(tools, list)
    assert tools, "expected a non-empty agent tool allow-list"


async def test_database_schema_initializes() -> None:
    """A fresh in-memory database initializes its schema and closes cleanly."""
    database = Database(":memory:")
    await database.initialize()
    try:
        # repos bundle is wired and a trivial read works against real schema.
        channels = await database.repos.channels.get_channels()
        assert isinstance(channels, list)
    finally:
        await database.close()


async def test_web_app_health_endpoint(tmp_path, real_pool_harness_factory) -> None:
    """The web app serves /health over a real (offline) ASGI request."""
    config = make_test_config(tmp_path)
    harness = real_pool_harness_factory()
    app, db = await build_web_app(config, harness)
    try:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/health")
        assert resp.status_code == 200
        assert resp.json()["status"] in ("healthy", "degraded")
    finally:
        await db.close()
