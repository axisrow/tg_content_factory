"""Offline CLI and worker sends against a restarted, stateful gateway."""

import asyncio
import json
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

import pytest

from tests.test_gateway_worker_processes import ROOT, rows, spawn
from tg_agent.database import Database
from tg_agent.gateway.runtime import configure, endpoint
from tg_agent.models import Account, IncomingDm

pytestmark = [pytest.mark.aiosqlite_serial, pytest.mark.mutmut_skip, pytest.mark.real_tg_never]

PHONE = "+10000000000"
LAUNCHER = Path(__file__).with_name("gateway_send_flood_stub.py")
WORKER_TEXT = "approved offline reply"


async def seed_draft(path: Path) -> int:
    db = Database(str(path), session_encryption_secret="offline-test-key")
    await db.initialize()
    try:
        await db.add_account(Account(phone=PHONE, session_string="offline-synthetic-session"))
        await db.repos.incoming_dms.record(IncomingDm(phone=PHONE, chat_id=42, message_id=1, text="offline inbound"))
        draft, _ = await db.repos.dm_reply_drafts.create_draft(
            incoming_dm_id=1, phone=PHONE, chat_id=42, reply_text=WORKER_TEXT
        )
        await db.repos.dm_reply_drafts.decide(draft, "approved")
        return draft
    finally:
        await db.close()


def wait_for(check, log: Path, *, seconds: float = 25):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if result := check():
            return result
        time.sleep(0.05)
    pytest.fail(f"Timed out waiting for {check.__name__}\n{log.read_text()[-3000:]}")


def stop(process) -> None:
    if process.poll() is None:
        process.terminate()
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


@pytest.mark.timeout(180)
def test_cli_and_worker_keep_send_pause_across_two_gateway_restarts(tmp_path):
    shared = tmp_path / "shared"
    project = tmp_path / "project"
    project.mkdir()
    (project / "config.yaml").write_text("database:\n  path: project.db\n")
    project_db = project / "project.db"
    draft = asyncio.run(seed_draft(project_db))
    configure(shared, api_id=12345, api_hash="offline-api-hash")
    (shared / "clock-offset").write_text("0")
    profile_path = shared / "gateway.json"
    profile = json.loads(profile_path.read_text())
    # Only the supervised offline gateway may start while the worker stays up.
    profile["python"] = str(shared / "no-automatic-launcher")
    profile_path.write_text(json.dumps(profile))
    env = {
        "PATH": os.environ["PATH"],
        "PYTHONPATH": os.pathsep.join((str(ROOT), str(ROOT / "packages/tg_shared/src"))),
        "PYTHONSAFEPATH": "1",
        "PYTHONDONTWRITEBYTECODE": "1",
        "TG_AGENT_GLOBAL_DIR": str(shared),
        "TG_AGENT_DATA_DIR": str(project / "data"),
        "TGCF_SESSION_LOCK_DIR": str(shared / "account-locks"),
        "TG_API_ID": "12345",
        "TG_API_HASH": "offline-api-hash",
        "SESSION_ENCRYPTION_KEY": "offline-test-key",
    }
    processes = []
    transport_log = shared / "transport.jsonl"
    gateway_log = shared / "gateway.log"
    worker_log = project / "worker.log"

    def start(log, mode, *args, cwd=project):
        with log.open("wb") as handle:
            process = spawn([sys.executable, str(LAUNCHER), str(shared), mode, *args], cwd, env, handle)
        processes.append(process)
        return process

    def cli(*args):
        return subprocess.run(
            [sys.executable, str(LAUNCHER), str(shared), "cli", *args],
            cwd=project, env=env, capture_output=True, text=True, timeout=25,
        )

    def start_gateway():
        process = start(shared / "gateway-child.log", "gateway", cwd=shared)

        def ready():
            record = endpoint(shared)
            return record and record["pid"] == process.pid

        wait_for(ready, shared / "gateway-child.log")
        return process

    def flood_until():
        return rows(shared / "gateway.db", "SELECT flood_until FROM accounts")[0][0]

    def check_worker():
        assert worker.poll() is None, worker_log.read_text()[-3000:]

    def command_state():
        check_worker()
        commands = rows(
            project_db, "SELECT id, status, run_after, error FROM telegram_commands ORDER BY id DESC LIMIT 1"
        )
        if commands and (commands[0][1] in {"failed", "succeeded"} or commands[0][2]):
            return commands[0]
        return None

    def delivered():
        check_worker()
        return rows(project_db, "SELECT status, sent_message_id FROM dm_reply_drafts") == [("sent", 88)]

    def transport_calls(kind):
        if not transport_log.exists():
            return []
        return [event for line in transport_log.read_text().splitlines() if (event := json.loads(line))["kind"] == kind]

    def connects(pid):
        return sum(call["pid"] == pid for call in transport_calls("connect"))

    def absolute_pause():
        until = flood_until()
        return until if until > time.time() else 0

    violations = []
    evidence = {}
    try:
        gateway = start_gateway()
        enabled = cli("global", "enable", "--account", PHONE)
        assert enabled.returncode == 0, enabled.stdout + enabled.stderr
        assert rows(project_db, "SELECT session_string FROM accounts") == [("",)]
        worker = start(worker_log, "cli", "worker")
        wait_for(
            lambda: rows(project_db, "SELECT 1 FROM runtime_snapshots WHERE snapshot_type='worker_heartbeat'"),
            worker_log,
        )

        regular_send = ("dialogs", "send", "me", "offline CLI send", "--phone", PHONE, "--yes")
        direct = start(project / "direct.log", "cli", *regular_send)
        queued = cli("dm", "drafts", "send", str(draft))
        assert queued.returncode == 0, queued.stdout + queued.stderr
        direct.wait(timeout=25)
        stored_deadline = wait_for(absolute_pause, gateway_log)
        flood_calls = [call for call in transport_calls("send") if call["outcome"] == "FLOOD_WAIT"]
        assert len(flood_calls) == 1
        assert stored_deadline == pytest.approx(flood_calls[0]["at"] + 600, abs=1, rel=0)
        initial_worker_state = wait_for(command_state, worker_log)
        evidence.update(
            flood_until=stored_deadline,
            initial_cli_exit=direct.returncode,
            initial_worker_state=initial_worker_state,
            restarts=[],
        )
        command_id, command_status, run_after, _ = initial_worker_state
        deferred = command_status == "pending" and run_after is not None
        if not deferred or datetime.fromisoformat(run_after).timestamp() < stored_deadline:
            violations.append(f"original worker command was not deferred until the pause ended: {initial_worker_state}")

        for _ in range(2):
            stop(gateway)
            gateway = start_gateway()
            check_worker()
            before_status = connects(gateway.pid)
            for _ in range(3):
                status = cli("global", "status")
                assert status.returncode == 0 and "Gateway relay: up" in status.stdout, status.stdout + status.stderr
            after_status = connects(gateway.pid)
            during_pause = cli(*regular_send)
            after_cli = connects(gateway.pid)
            evidence["restarts"].append(dict(
                pid=gateway.pid, deadline=flood_until(), connections_before_status=before_status,
                connections_after_status=after_status, connections_after_cli=after_cli,
                cli_exit=during_pause.returncode,
            ))
            if flood_until() != stored_deadline or flood_until() <= time.time():
                violations.append(f"restart PID {gateway.pid} changed the active flood deadline")
            if after_cli:
                violations.append(
                    f"restart PID {gateway.pid} attempted {after_cli} connections during the pause "
                    f"({before_status} at startup, {after_status - before_status} during status checks, "
                    f"{after_cli - after_status} during the CLI send)"
                )
            if during_pause.returncode == 0:
                violations.append(f"CLI reported success during the active pause after restart PID {gateway.pid}")

        evidence["before_expiry"] = transport_calls("send")
        if [call["outcome"] for call in evidence["before_expiry"]] != ["FLOOD_WAIT"]:
            violations.append("unexpected SendMessageRequest during the account pause")

        if deferred:
            new_clock = shared / "clock-next"
            new_clock.write_text("610")
            new_clock.replace(shared / "clock-offset")
            wait_for(delivered, worker_log)
            wait_for(lambda: rows(project_db, "SELECT status FROM telegram_commands") == [("succeeded",)], worker_log)
            again = cli("dm", "drafts", "send", str(draft))
            assert again.returncode == 0 and "message_id=88" in again.stdout, again.stdout + again.stderr
            evidence["final_commands"] = rows(project_db, "SELECT id, status FROM telegram_commands")
            assert evidence["final_commands"] == [(command_id, "succeeded")]
            evidence["after_expiry"] = transport_calls("send")
            if [call["text"] for call in evidence["after_expiry"] if call["outcome"] == "sent"] != [WORKER_TEXT]:
                violations.append("the approved worker reply was not sent exactly once after the pause")
    finally:
        for process in reversed(processes):
            stop(process)
        (tmp_path / "result.json").write_text(json.dumps(evidence, ensure_ascii=False, indent=2))
    assert not violations, "\n".join(violations)
