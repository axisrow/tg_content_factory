"""Exercise enrollment and two actual CLI workers against one offline gateway."""

import asyncio
import json
import os
import signal
import sqlite3
import subprocess
import sys
import threading
import time
from contextlib import closing
from pathlib import Path

import pytest

from tg_agent.database import Database
from tg_agent.gateway.runtime import configure, endpoint
from tg_agent.models import Account, Channel

pytestmark = [pytest.mark.aiosqlite_serial]
ROOT = Path(__file__).resolve().parents[1]


async def seed(path, *, account=False, cursor=0, filtered=False):
    db = Database(str(path), session_encryption_secret="worker-test-key")
    await db.initialize()
    try:
        if account:
            await db.add_account(Account(phone="+10000000000", session_string="offline-synthetic-session"))
        await db.add_channel(Channel(channel_id=123, title="source", username="source"))
        if cursor:
            await db.execute_write("UPDATE channels SET last_collected_id=? WHERE channel_id=123", (cursor,))
        if filtered:
            await db.repos.channels.set_filtered_bulk([(123, "operator-filter")], origin="human")
        await db.repos.tasks.create_collection_task(123, "source")
    finally:
        await db.close()


def spawn(command, cwd, env, log):
    process = subprocess.Popen(command, cwd=cwd, env=env, stdin=subprocess.DEVNULL, stdout=log, stderr=log)
    threading.Thread(target=process.wait, daemon=True).start()
    return process


def rows(path, sql):
    with closing(sqlite3.connect(path)) as db, db:
        return db.execute(sql).fetchall()


@pytest.mark.parametrize("filtered_a", [False, True])
def test_two_cli_workers_share_gateway_and_keep_project_databases(tmp_path, filtered_a):
    shared = tmp_path / "shared"
    configure(shared, api_id=12345, api_hash="offline-api-hash")
    env = {**os.environ, "TG_AGENT_GLOBAL_DIR": str(shared), "TG_API_ID": "12345",
           "TG_API_HASH": "offline-api-hash", "SESSION_ENCRYPTION_KEY": "worker-test-key",
           "PYTHONPATH": os.pathsep.join([str(ROOT), str(ROOT / "packages/tg_shared/src")])}
    processes = []
    handles = []
    try:
        gateway_log = (tmp_path / "gateway.log").open("wb")
        handles.append(gateway_log)
        gateway = spawn([sys.executable, str(ROOT / "tests/gateway_process_stub.py"), str(shared)], ROOT,
                        env, gateway_log)
        processes.append(gateway)
        deadline = time.monotonic() + 30
        while endpoint(shared) is None and time.monotonic() < deadline:
            assert gateway.poll() is None, (tmp_path / "gateway.log").read_text()
            time.sleep(0.1)
        assert endpoint(shared) is not None
        projects = []
        for index in range(2):
            project = tmp_path / f"project-{index}"
            project.mkdir()
            (project / "config.yaml").write_text("database:\n  path: project.db\n")
            asyncio.run(seed(project / "project.db", account=index == 0, cursor=5 if index else 0,
                             filtered=index == 0 and filtered_a))
            enabled = subprocess.run([sys.executable, "-m", "tg_agent.main", "global", "enable", "--account",
                                      "+10000000000"], cwd=project, env=env, capture_output=True, text=True, timeout=30)
            assert enabled.returncode == 0, enabled.stdout + enabled.stderr
            assert "Project database: project.db" in enabled.stdout
            log = (project / "worker.log").open("wb")
            handles.append(log)
            worker = spawn([sys.executable, "-m", "tg_agent.main", "worker"], project, env, log)
            processes.append(worker)
            projects.append((project, worker))
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            expected = [0 if filtered_a else 2, 1]
            if all(rows(project / "project.db", "SELECT COUNT(*) FROM messages")[0][0] == count
                   for (project, _), count in zip(projects, expected, strict=True)):
                break
            for project, worker in projects:
                assert worker.poll() is None, (project / "worker.log").read_text()[-5000:]
            time.sleep(0.1)
        for index, (project, _) in enumerate(projects):
            message_ids = rows(project / "project.db", "SELECT message_id FROM messages ORDER BY message_id")
            expected = ([(5,), (7,)] if not filtered_a else []) if index == 0 else [(7,)]
            assert message_ids == expected, (
                project / "worker.log").read_text()[-5000:]
            assert rows(project / "project.db", "SELECT last_collected_id FROM channels") == [
                (0 if index == 0 and filtered_a else 7,)]
            assert rows(project / "project.db", "SELECT session_string FROM accounts") == [("",)]
        assert len(json.loads((shared / "test-connections.json").read_text())) == 1
        stopped = subprocess.run([sys.executable, "-m", "tg_agent.main", "stop"], cwd=projects[0][0], env=env,
                                 capture_output=True, text=True, timeout=20)
        assert stopped.returncode == 0, stopped.stdout + stopped.stderr
        assert projects[1][1].poll() is None
        assert gateway.poll() is None
        delivered = subprocess.run([sys.executable, "-m", "tg_agent.main", "dialogs", "send", "123",
            "project B still sends", "--phone", "+10000000000", "--yes"], cwd=projects[1][0], env=env,
            capture_output=True, text=True, timeout=30)
        assert delivered.returncode == 0, delivered.stdout + delivered.stderr
        deadline = time.monotonic() + 20
        while not (shared / "test-sent.json").exists() and time.monotonic() < deadline:
            time.sleep(0.1)
        assert json.loads((shared / "test-sent.json").read_text())["text"] == "project B still sends"
    finally:
        for process in reversed(processes):
            if process.poll() is None:
                process.send_signal(signal.SIGTERM)
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        for handle in handles:
            handle.close()
