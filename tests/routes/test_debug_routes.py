"""Tests for debug routes."""

from __future__ import annotations

import base64

import pytest
from httpx import ASGITransport, AsyncClient

from src.web.log_handler import LogBuffer
from src.web.routes.debug import _read_log_tail


@pytest.fixture
async def client(base_app):
    """Create test client."""
    app, _, pool = base_app

    async def _resolve_channel(identifier):
        return {
            "channel_id": -1001234567890,
            "title": "Test Channel",
            "username": "testchannel",
            "channel_type": "channel",
        }

    pool.clients = {}
    pool.resolve_channel = _resolve_channel
    app.state.log_buffer = LogBuffer()

    transport = ASGITransport(app=app)
    auth_header = base64.b64encode(b":testpass").decode()
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        follow_redirects=True,
        headers={"Authorization": f"Basic {auth_header}", "Origin": "http://test"},
    ) as c:
        yield c


# ─── _read_log_tail unit tests ───────────────────────────────────────

def test_read_log_tail_missing_file(tmp_path):
    """Returns empty list when log file does not exist."""
    assert _read_log_tail(tmp_path / "nonexistent.log") == []


def test_read_log_tail_parses_records(tmp_path):
    """Parses standard log lines into dicts."""
    log = tmp_path / "app.log"
    log.write_text(
        "2024-01-01 12:00:00 [INFO] myapp.service: Started\n"
        "2024-01-01 12:00:01 [WARNING] myapp.db: Slow query\n",
        encoding="utf-8",
    )
    records = _read_log_tail(log)
    assert len(records) == 2
    assert records[0] == {
        "time": "2024-01-01 12:00:00", "level": "INFO",
        "logger": "myapp.service", "message": "Started",
    }
    assert records[1]["level"] == "WARNING"
    assert records[1]["logger"] == "myapp.db"


def test_read_log_tail_multiline(tmp_path):
    """Continuation lines are merged into the previous record."""
    log = tmp_path / "app.log"
    log.write_text(
        "2024-01-01 12:00:00 [ERROR] app: Something failed\n"
        "Traceback (most recent call last):\n"
        "  File foo.py, line 1\n"
        "ValueError: bad value\n",
        encoding="utf-8",
    )
    records = _read_log_tail(log)
    assert len(records) == 1
    assert "Traceback" in records[0]["message"]
    assert "ValueError" in records[0]["message"]


def test_read_log_tail_respects_max_lines(tmp_path):
    """Only the last max_lines lines are considered."""
    log = tmp_path / "app.log"
    lines = [f"2024-01-01 12:00:{i:02d} [INFO] app: msg {i}\n" for i in range(10)]
    log.write_text("".join(lines), encoding="utf-8")
    records = _read_log_tail(log, max_lines=3)
    assert len(records) == 3
    assert "msg 9" in records[-1]["message"]


# ─── route smoke tests ───────────────────────────────────────────────

@pytest.mark.asyncio
async def test_debug_page_renders(client):
    """Test debug page renders successfully."""
    resp = await client.get("/debug/")
    assert resp.status_code == 200
    assert "debug" in resp.text.lower() or "log" in resp.text.lower()


@pytest.mark.asyncio
async def test_debug_page_reads_from_file(client, tmp_path, monkeypatch):
    """Debug page shows records read from log file."""
    log = tmp_path / "app.log"
    log.write_text(
        "2024-01-01 12:00:00 [INFO] test.logger: Hello from file\n",
        encoding="utf-8",
    )
    monkeypatch.setattr("src.web.routes.debug.APP_LOG_PATH", log)
    resp = await client.get("/debug/")
    assert resp.status_code == 200
    assert "Hello from file" in resp.text


@pytest.mark.asyncio
async def test_debug_page_empty_when_no_file(client, tmp_path, monkeypatch):
    """Debug page renders without error when log file is absent."""
    monkeypatch.setattr("src.web.routes.debug.APP_LOG_PATH", tmp_path / "missing.log")
    resp = await client.get("/debug/")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_debug_logs_partial(client):
    """Test debug logs partial endpoint."""
    resp = await client.get("/debug/logs")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_debug_page_empty_buffer(client):
    """Kept for compatibility — debug page renders when buffer is empty."""
    client._transport.app.state.log_buffer._records.clear()
    resp = await client.get("/debug/")
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


# ─── timing endpoint tests ──────────────────────────────────────────


@pytest.mark.asyncio
async def test_debug_timing_page(client):
    """Test timing page renders."""
    resp = await client.get("/debug/timing")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_debug_timing_rows(client):
    """Test timing rows partial."""
    resp = await client.get("/debug/timing/rows")
    assert resp.status_code == 200


# ─── memory endpoint tests ──────────────────────────────────────────


@pytest.mark.asyncio
async def test_debug_memory_returns_json(client):
    """Test memory endpoint returns JSON with expected keys."""
    resp = await client.get("/debug/memory")
    assert resp.status_code == 200
    data = resp.json()
    assert "rss_mb" in data
    assert "gc_counts" in data
    assert "pool" in data


@pytest.mark.asyncio
async def test_debug_memory_pool_info(client):
    """Test memory endpoint includes pool info."""
    resp = await client.get("/debug/memory")
    data = resp.json()
    assert "connected_clients" in data["pool"]
    assert "dialogs_cache_entries" in data["pool"]


@pytest.mark.asyncio
async def test_debug_memory_uses_snapshot_in_web_mode(client, base_app):
    """In web-mode, pool counters must come from runtime_snapshots.pool_counters."""
    from datetime import datetime, timezone

    from src.models import RuntimeSnapshot

    app, db, _ = base_app
    app.state.runtime_mode = "web"
    await db.repos.runtime_snapshots.upsert_snapshot(
        RuntimeSnapshot(
            snapshot_type="pool_counters",
            payload={
                "dialogs_cache_entries": 7,
                "active_leases": {"+1234567890": 2},
                "premium_flood_waits": 1,
                "session_overrides": 3,
            },
            updated_at=datetime.now(timezone.utc),
        )
    )
    resp = await client.get("/debug/memory")
    assert resp.status_code == 200
    body = resp.json()
    assert body["runtime_mode"] == "web"
    assert body["pool"]["source"] == "snapshot"
    assert body["pool"]["dialogs_cache_entries"] == 7
    assert body["pool"]["active_leases"] == {"+1234567890": 2}
    assert body["pool"]["premium_flood_waits"] == 1
    assert body["pool"]["session_overrides"] == 3


@pytest.mark.asyncio
async def test_debug_memory_reports_empty_source_without_snapshot(client, base_app):
    """web-mode without a snapshot yet: source=empty and counters are zero."""
    app, _, _ = base_app
    app.state.runtime_mode = "web"
    resp = await client.get("/debug/memory")
    assert resp.status_code == 200
    body = resp.json()
    assert body["pool"]["source"] == "empty"
    assert body["pool"]["dialogs_cache_entries"] == 0


@pytest.mark.asyncio
async def test_debug_memory_live_counters_in_worker_mode(client, base_app):
    """worker-mode: counters come from live pool, not snapshot."""
    app, _, pool = base_app
    app.state.runtime_mode = "worker"
    pool._dialogs_cache = {"+1": object(), "+2": object()}
    resp = await client.get("/debug/memory")
    assert resp.status_code == 200
    body = resp.json()
    assert body["runtime_mode"] == "worker"
    assert body["pool"]["source"] == "live"
    assert body["pool"]["dialogs_cache_entries"] == 2
