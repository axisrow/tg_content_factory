"""Real local subprocesses and sockets; these tests make no Telegram requests."""

import concurrent.futures
import os
import signal
import time

import httpx
import pytest

from tg_agent.gateway.runtime import configure, endpoint, ensure_gateway
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
