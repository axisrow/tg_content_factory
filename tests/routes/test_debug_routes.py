"""Tests for debug routes."""

from __future__ import annotations

import base64

import pytest
from httpx import ASGITransport, AsyncClient

from src.collection_queue import CollectionQueue
from src.config import AppConfig
from src.database import Database
from src.models import Account
from src.scheduler.manager import SchedulerManager
from src.search.ai_search import AISearchEngine
from src.search.engine import SearchEngine
from src.telegram.auth import TelegramAuth
from src.telegram.collector import Collector
from src.web.app import create_app
from src.web.log_handler import LogBuffer


@pytest.fixture
async def client(tmp_path):
    """Create test client with log buffer."""
    config = AppConfig()
    config.database.path = str(tmp_path / "test.db")
    config.telegram.api_id = 12345
    config.telegram.api_hash = "test_hash"
    config.web.password = "testpass"
    app = create_app(config)

    db = Database(config.database.path)
    await db.initialize()
    app.state.db = db

    async def _resolve_channel(self, identifier):
        return {
            "channel_id": -1001234567890,
            "title": "Test Channel",
            "username": "testchannel",
            "channel_type": "channel",
        }

    app.state.pool = type(
        "Pool",
        (),
        {
            "clients": {},
            "resolve_channel": _resolve_channel,
        },
    )()

    app.state.auth = TelegramAuth(12345, "test_hash")
    app.state.notifier = None
    collector = Collector(app.state.pool, db, config.scheduler)
    app.state.collector = collector
    app.state.collection_queue = CollectionQueue(collector, db)
    app.state.search_engine = SearchEngine(db)
    app.state.ai_search = AISearchEngine(config.llm, db)
    app.state.scheduler = SchedulerManager(config.scheduler)
    app.state.session_secret = "test_secret_key"
    app.state.log_buffer = LogBuffer()

    await db.add_account(Account(phone="+1234567890", session_string="test_session"))

    transport = ASGITransport(app=app)
    auth_header = base64.b64encode(b":testpass").decode()
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        follow_redirects=True,
        headers={"Authorization": f"Basic {auth_header}"},
    ) as c:
        yield c

    await app.state.collection_queue.shutdown()
    await db.close()


@pytest.fixture
async def client_no_buffer(tmp_path):
    """Create test client without log buffer."""
    config = AppConfig()
    config.database.path = str(tmp_path / "test.db")
    config.telegram.api_id = 12345
    config.telegram.api_hash = "test_hash"
    config.web.password = "testpass"
    app = create_app(config)

    db = Database(config.database.path)
    await db.initialize()
    app.state.db = db

    async def _resolve_channel(self, identifier):
        return {
            "channel_id": -1001234567890,
            "title": "Test Channel",
            "username": "testchannel",
            "channel_type": "channel",
        }

    app.state.pool = type(
        "Pool",
        (),
        {
            "clients": {},
            "resolve_channel": _resolve_channel,
        },
    )()

    app.state.auth = TelegramAuth(12345, "test_hash")
    app.state.notifier = None
    collector = Collector(app.state.pool, db, config.scheduler)
    app.state.collector = collector
    app.state.collection_queue = CollectionQueue(collector, db)
    app.state.search_engine = SearchEngine(db)
    app.state.ai_search = AISearchEngine(config.llm, db)
    app.state.scheduler = SchedulerManager(config.scheduler)
    app.state.session_secret = "test_secret_key"
    app.state.log_buffer = None  # No buffer

    await db.add_account(Account(phone="+1234567890", session_string="test_session"))

    transport = ASGITransport(app=app)
    auth_header = base64.b64encode(b":testpass").decode()
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        follow_redirects=True,
        headers={"Authorization": f"Basic {auth_header}"},
    ) as c:
        yield c

    await app.state.collection_queue.shutdown()
    await db.close()


@pytest.mark.asyncio
async def test_debug_page_renders(client):
    """Test debug page renders successfully."""
    resp = await client.get("/debug/")
    assert resp.status_code == 200
    assert "debug" in resp.text.lower() or "log" in resp.text.lower()


@pytest.mark.asyncio
async def test_debug_page_with_records(client):
    """Test debug page shows log records."""
    # Add some log records
    import logging

    logger = logging.getLogger("test_debug")
    handler = client._transport.app.state.log_buffer
    handler.setFormatter(logging.Formatter())
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

    logger.info("Test message 1")
    logger.warning("Test warning message")

    resp = await client.get("/debug/")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_debug_page_without_buffer(client_no_buffer):
    """Test debug page when no log buffer configured."""
    resp = await client_no_buffer.get("/debug/")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_debug_logs_partial(client):
    """Test debug logs partial endpoint."""
    resp = await client.get("/debug/logs")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_debug_logs_partial_without_buffer(client_no_buffer):
    """Test debug logs partial when no buffer configured."""
    resp = await client_no_buffer.get("/debug/logs")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_log_buffer_maxlen():
    """Test LogBuffer respects max length."""
    import logging

    buffer = LogBuffer(maxlen=3)
    logger = logging.getLogger("test_maxlen")
    buffer.setFormatter(logging.Formatter())
    logger.addHandler(buffer)
    logger.setLevel(logging.INFO)

    for i in range(5):
        logger.info(f"Message {i}")

    records = buffer.get_records()
    assert len(records) == 3
    # Should have most recent
    assert "Message 4" in records[-1]["message"]


@pytest.mark.asyncio
async def test_log_buffer_record_format():
    """Test LogBuffer record format."""
    import logging

    buffer = LogBuffer()
    logger = logging.getLogger("test_format")
    buffer.setFormatter(logging.Formatter())
    logger.addHandler(buffer)
    logger.setLevel(logging.INFO)

    logger.info("Test message")

    records = buffer.get_records()
    assert len(records) == 1
    assert "time" in records[0]
    assert "level" in records[0]
    assert "logger" in records[0]
    assert "message" in records[0]
    assert records[0]["level"] == "INFO"
    assert records[0]["logger"] == "test_format"


@pytest.mark.asyncio
async def test_log_buffer_levels():
    """Test LogBuffer captures different log levels."""
    import logging

    buffer = LogBuffer()
    logger = logging.getLogger("test_levels")
    buffer.setFormatter(logging.Formatter())
    logger.addHandler(buffer)
    logger.setLevel(logging.DEBUG)

    logger.debug("Debug message")
    logger.info("Info message")
    logger.warning("Warning message")
    logger.error("Error message")

    records = buffer.get_records()
    assert len(records) == 4
    levels = [r["level"] for r in records]
    assert "DEBUG" in levels
    assert "INFO" in levels
    assert "WARNING" in levels
    assert "ERROR" in levels


@pytest.mark.asyncio
async def test_log_buffer_exception():
    """Test LogBuffer handles exceptions in records."""
    import logging

    buffer = LogBuffer()
    logger = logging.getLogger("test_exception")
    buffer.setFormatter(logging.Formatter())
    logger.addHandler(buffer)
    logger.setLevel(logging.ERROR)

    try:
        raise ValueError("Test error")
    except ValueError:
        logger.exception("An error occurred")

    records = buffer.get_records()
    assert len(records) == 1
    assert "ValueError" in records[0]["message"]


@pytest.mark.asyncio
async def test_debug_page_empty_buffer(client):
    """Test debug page with empty log buffer."""
    # Clear buffer
    client._transport.app.state.log_buffer._records.clear()

    resp = await client.get("/debug/")
    assert resp.status_code == 200
