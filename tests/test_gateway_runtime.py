"""Real local subprocesses and sockets; these tests make no Telegram requests."""

import concurrent.futures
import logging
import os
import re
import signal
import time
from logging.handlers import RotatingFileHandler

import httpx
import pytest

from tg_agent.gateway.runtime import configure, endpoint, ensure_gateway, setup_gateway_logging
from tg_agent.gateway.store import private_key


def test_unenrolled_gateway_root_fails_before_creating_anything(tmp_path):
    """No lock or log side effects on an unconfigured root — including mock paths."""
    fresh = tmp_path / "never-created"
    with pytest.raises(RuntimeError, match="not enrolled"):
        ensure_gateway(fresh)
    assert not fresh.exists()


def test_concurrent_key_setup_publishes_one_complete_key(tmp_path):
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        keys = list(executor.map(lambda _: private_key(tmp_path / "private"), range(16)))
    assert len(set(keys)) == 1
    assert len(keys[0]) >= 48


def _detach(handler) -> None:
    logging.getLogger().removeHandler(handler)
    handler.close()


def test_setup_gateway_logging_writes_timestamped_lines(tmp_path):
    """#1780: gateway.log lines must carry timestamps — bare lines hid a
    216k-entry error mass for days."""
    handler = setup_gateway_logging(tmp_path)
    try:
        logging.getLogger("gateway-log-test").warning("boom #1780")
        handler.flush()
        line = (tmp_path / "gateway.log").read_text()
        assert "boom #1780" in line
        assert re.search(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", line)
    finally:
        _detach(handler)


def test_setup_gateway_logging_rotates_and_keeps_backups(tmp_path):
    """#1780: size-based rotation, so the log cannot grow unbounded."""
    handler = setup_gateway_logging(tmp_path, max_bytes=200, backup_count=2)
    try:
        for index in range(30):
            logging.getLogger("gateway-log-test").warning(
                "rotation filler %03d %s", index, "x" * 40)
        handler.flush()
        assert (tmp_path / "gateway.log.1").exists()
        assert (tmp_path / "gateway.log").stat().st_size <= 200
    finally:
        _detach(handler)


def test_setup_gateway_logging_is_idempotent(tmp_path):
    """A repeated setup call must not double-attach (duplicate lines)."""
    first = setup_gateway_logging(tmp_path)
    second = setup_gateway_logging(tmp_path)
    try:
        attached = [h for h in logging.getLogger().handlers
                    if isinstance(h, RotatingFileHandler)
                    and getattr(h, "baseFilename", None) == str(tmp_path / "gateway.log")]
        assert len(attached) == 1
    finally:
        for handler in {first, second}:
            _detach(handler)


def test_concurrent_autostart_reuses_gateway_from_another_directory(tmp_path, monkeypatch):
    root = tmp_path / "private"
    configure(root, api_id=12345, api_hash="offline-test-api-hash")
    project = tmp_path / "another-project"
    project.mkdir()
    monkeypatch.chdir(project)
    record = None
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            urls = list(executor.map(lambda _: ensure_gateway(root, timeout=30), range(2)))
        assert urls[0] == urls[1]
        record = endpoint(root)
        assert record is not None
        assert record["role"] == "gateway"
        with httpx.Client(trust_env=False) as client:
            health = client.get(urls[0] + "/v1/health").json()
            denied = client.get(urls[0] + "/v1/status")
        assert health["instance"] == record["instance"]
        assert denied.status_code == 401
        assert ensure_gateway(root) == urls[0]
    finally:
        record = record or endpoint(root)
        if record is not None:
            os.kill(record["pid"], signal.SIGTERM)
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline and (root / "gateway.endpoint.json").exists():
                time.sleep(0.05)
            assert not (root / "gateway.endpoint.json").exists()
