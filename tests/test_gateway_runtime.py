"""Real local subprocesses and sockets; these tests make no Telegram requests."""

import concurrent.futures
import logging
import os
import re
import signal
import sys
import time
from logging.handlers import RotatingFileHandler
from pathlib import Path

import pytest

from tg_agent.gateway.runtime import configure, endpoint, ensure_gateway, setup_gateway_logging
from tg_agent.gateway.store import private_key
from tg_agent.telegram.relay import RELAY_PROTOCOL, RelayEndpoint, probe_endpoint


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
        # Rotation recreates the file: the 0600 mode must survive rollovers.
        assert (tmp_path / "gateway.log").stat().st_mode & 0o777 == 0o600
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
            records = list(executor.map(lambda _: ensure_gateway(root, timeout=30), range(2)))
        assert records[0]["port"] == records[1]["port"]
        record = endpoint(root)
        assert record is not None
        assert record["role"] == "gateway"
        assert record["protocol"] == RELAY_PROTOCOL
        admin = (root / "gateway-admin.token").read_text().strip()
        relay = RelayEndpoint(port=record["port"], token=admin, pid=record["pid"], protocol=RELAY_PROTOCOL)
        hello = probe_endpoint(relay)
        assert hello is not None and hello["pid"] == record["pid"]
        assert probe_endpoint(RelayEndpoint(port=record["port"], token="wrong", pid=record["pid"],
                                            protocol=RELAY_PROTOCOL)) is None
        assert ensure_gateway(root)["port"] == records[0]["port"]
    finally:
        record = record or endpoint(root)
        if record is not None:
            os.kill(record["pid"], signal.SIGTERM)
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline and (root / "gateway.endpoint.json").exists():
                time.sleep(0.05)
            assert not (root / "gateway.endpoint.json").exists()


def test_an_older_running_gateway_is_a_hard_error_asking_for_restart(tmp_path):
    from tg_agent.gateway.runtime import GatewayProtocolError, _atomic_record
    from tg_agent.utils.process_identity import process_start_identity

    root = tmp_path / "private"
    configure(root, api_id=12345, api_hash="offline-test-api-hash")
    # A pre-relay discovery record: no protocol, a live process behind it.
    _atomic_record(root / "gateway.endpoint.json", {
        "port": 1, "instance": "old", "pid": os.getpid(), "store_root": str(root), "role": "gateway",
        "start_identity": process_start_identity(os.getpid()),
    })
    with pytest.raises(GatewayProtocolError, match="gateway restart --shared"):
        endpoint(root)


def test_an_older_gateway_started_under_another_locale_still_asks_for_restart(tmp_path, monkeypatch):
    """`ps` prints a start time per locale; the record and the caller may disagree.

    Found on a live run: the caller's locale differed from the gateway's, the
    identities did not match and the running gateway was reported as down.
    """
    from tg_agent.gateway import runtime
    from tg_agent.gateway.runtime import GatewayProtocolError, _atomic_record

    root = tmp_path / "private"
    configure(root, api_id=12345, api_hash="offline-test-api-hash")
    _atomic_record(root / "gateway.endpoint.json", {
        "port": 1, "instance": "old", "pid": os.getpid(), "store_root": str(root), "role": "gateway",
        "start_identity": "пятница, 20 ноября 2026 г. 21:58:19",
    })
    monkeypatch.setattr(runtime, "process_start_identity", lambda pid: "Fri Nov 20 21:58:19 2026")

    with pytest.raises(GatewayProtocolError, match="gateway restart --shared"):
        endpoint(root)


@pytest.mark.parametrize("exits", [True, False], ids=["exits", "ignores-the-signal"])
def test_stopping_an_older_gateway_started_under_another_locale(tmp_path, monkeypatch, exits):
    """`gateway stop --shared` is the documented upgrade step; a locale mismatch must not refuse
    it, nor take a gateway that is still running for a stopped one."""
    from tg_agent.gateway import runtime
    from tg_agent.gateway.runtime import _atomic_record, stop_gateway

    root = tmp_path / "private"
    configure(root, api_id=12345, api_hash="offline-test-api-hash")
    _atomic_record(root / "gateway.endpoint.json", {
        "port": 1, "instance": "old", "pid": 4242, "store_root": str(root.resolve()), "role": "gateway",
        "python": sys.executable, "start_identity": "пятница, 20 ноября 2026 г. 21:58:19",
    })
    observed: dict[str, str | None] = {"identity": "Fri Nov 20 21:58:19 2026"}
    signalled = []

    def kill(pid, sig):
        signalled.append((pid, sig))
        if exits:
            observed["identity"] = None

    monkeypatch.setattr(runtime, "process_start_identity", lambda pid: observed["identity"])
    monkeypatch.setattr(runtime, "process_executable", lambda pid: str(Path(sys.executable).resolve()))
    monkeypatch.setattr(runtime.os, "kill", kill)

    if exits:
        stop_gateway(root, shared=True, timeout=1)
    else:
        with pytest.raises(TimeoutError, match="did not stop"):
            stop_gateway(root, shared=True, timeout=0.2)

    assert signalled == [(4242, signal.SIGTERM)]
