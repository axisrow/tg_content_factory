"""Fixed-installation, loopback-only gateway runtime and serialized autostart."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import secrets
import signal
import socket
import subprocess
import sys
import threading
import time
from contextlib import contextmanager
from pathlib import Path

import httpx
import uvicorn

import tg_shared
from tg_agent.gateway.server import create_app
from tg_agent.gateway.store import GatewayStore, private_key
from tg_agent.gateway.telegram import LiveTelegramAccess
from tg_agent.telegram.session_lock import SessionLock
from tg_agent.utils.process_identity import process_executable, process_start_identity
from tg_shared.auth import TelegramAuth


class GatewayServer(uvicorn.Server):
    @contextmanager
    def capture_signals(self):
        """Drain SQLite and remove discovery before a graceful process exit."""
        originals = {}
        try:
            for sig in (signal.SIGTERM, signal.SIGINT):
                originals[sig] = signal.signal(sig, self.handle_exit)
            yield
        finally:
            for sig, handler in originals.items():
                signal.signal(sig, handler)


class GatewayProtocolError(RuntimeError):
    """An existing gateway must be upgraded explicitly, never replaced by a caller."""


def configure(root: Path, *, api_id: int, api_hash: str, proxy: dict | None = None) -> None:
    """Initial enrollment pins the launcher and private Telegram credentials."""
    if (root / "gateway.json").exists():
        private_key(root)
        private_key(root, "gateway-admin.token")
        return
    if api_id <= 0 or not api_hash:
        raise ValueError("Gateway Telegram API credentials are required")
    private_key(root)
    private_key(root, "gateway-admin.token")
    profile = root / "gateway.json"
    if profile.exists():
        return
    contents = {"api_id": api_id, "api_hash": api_hash, "proxy": proxy, "python": sys.executable,
                "module_root": str(Path(__file__).resolve().parents[2]),
                "python_path": [str(Path(__file__).resolve().parents[2]),
                                str(Path(tg_shared.__file__).resolve().parent.parent)]}
    temporary = root / f".gateway-profile-{secrets.token_hex(16)}"
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(contents, handle)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temporary, profile)
        except FileExistsError:
            pass
    finally:
        temporary.unlink(missing_ok=True)


def _atomic_record(path: Path, contents: dict) -> None:
    temporary = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(contents, handle)
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def endpoint(root: Path) -> dict | None:
    """A nonce binds a health response to this private discovery record."""
    try:
        record = json.loads((root / "gateway.endpoint.json").read_text(encoding="utf-8"))
        port = record["port"]
        if type(port) is not int or not 0 < port < 65536:
            return None
        with httpx.Client(timeout=1, trust_env=False) as client:
            response = client.get(f"http://127.0.0.1:{port}/v1/health")
            response.raise_for_status()
            health = response.json()
        if health.get("instance") == record["instance"] and (
                health.get("protocol") != 1 or health.get("capabilities_version") != 2):
            raise GatewayProtocolError("Incompatible Telegram gateway protocol; upgrade the shared gateway explicitly")
        if health.get("instance") == record["instance"] and health.get("protocol") == 1:
            return record
    except (OSError, ValueError, KeyError, httpx.HTTPError):
        pass
    return None


def ensure_gateway(root: Path, *, timeout: float = 30) -> str:
    """All callers launch the enrolled binary; the project cwd is irrelevant."""
    root = root.resolve()
    current = endpoint(root)
    if current:
        return f"http://127.0.0.1:{current['port']}"
    launcher = SessionLock("gateway launcher", str(root / "launcher"), lock_dir=root / "locks")
    launcher.acquire(grace_seconds=timeout)
    try:
        current = endpoint(root)
        if current:
            return f"http://127.0.0.1:{current['port']}"
        profile = json.loads((root / "gateway.json").read_text(encoding="utf-8"))
        # Probe imports/config BEFORE starting; never stop an existing process here.
        with (root / "gateway.log").open("ab") as log:
            os.chmod(root / "gateway.log", 0o600)
            process = subprocess.Popen(
                [profile["python"], "-m", "tg_agent.gateway.runtime", "--root", str(root)],
                cwd=profile["module_root"], stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT,
                start_new_session=True,
                env={**os.environ, "PYTHONPATH": os.pathsep.join(profile["python_path"])},
            )
        # Keep/reap the child while this caller lives, without blocking CLI exit.
        threading.Thread(target=process.wait, daemon=True, name="gateway-reaper").start()
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            current = endpoint(root)
            if current:
                return f"http://127.0.0.1:{current['port']}"
            if process.poll() is not None:
                raise RuntimeError(f"Telegram gateway failed to start; see {root / 'gateway.log'}")
            time.sleep(0.1)
        raise TimeoutError("Telegram gateway startup timed out; no direct session fallback is permitted")
    finally:
        launcher.release()


async def serve(root: Path, *, access_factory=None) -> None:
    root = await asyncio.to_thread(root.resolve)
    owner = SessionLock("gateway", str(root / "gateway owner"), lock_dir=root / "locks")
    owner.acquire(grace_seconds=0)
    store = GatewayStore(root)
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    instance = secrets.token_urlsafe(32)
    path = root / "gateway.endpoint.json"
    try:
        profile = json.loads(await asyncio.to_thread((root / "gateway.json").read_text, encoding="utf-8"))
        await store.open()
        access = (access_factory(store) if access_factory is not None
                  else LiveTelegramAccess(store, TelegramAuth(profile["api_id"], profile["api_hash"],
                                                              proxy=profile.get("proxy"))))
        app = create_app(store, access)

        @app.get("/v1/health")
        async def health():
            return {"protocol": 1, "capabilities_version": 2, "instance": instance}

        listener.bind(("127.0.0.1", 0))
        listener.listen(128)
        listener.setblocking(False)
        record = {"port": listener.getsockname()[1], "instance": instance, "pid": os.getpid(),
                  "store_root": str(root), "python": sys.executable, "role": "gateway",
                  "start_identity": process_start_identity(os.getpid())}
        server = GatewayServer(uvicorn.Config(app, access_log=False, log_level="warning"))
        app.state.request_shutdown = lambda: setattr(server, "should_exit", True)

        async def publish():
            while not server.started:
                await asyncio.sleep(0.01)
            await asyncio.to_thread(_atomic_record, path, record)

        publication = asyncio.create_task(publish())
        try:
            await server.serve(sockets=[listener])
        finally:
            publication.cancel()
            await asyncio.gather(publication, return_exceptions=True)
    finally:
        listener.close()
        await store.close()
        await asyncio.to_thread(path.unlink, missing_ok=True)
        owner.release()


def preflight(root: Path) -> None:
    """Validate the enrolled binary/configuration before a shared restart."""
    profile = json.loads((root / "gateway.json").read_text(encoding="utf-8"))
    if not profile["api_id"] or not profile["api_hash"]:
        raise ValueError("Gateway Telegram credentials are missing")
    from tg_agent.security import SessionCipher

    cipher = SessionCipher((root / "gateway.key").read_text(encoding="utf-8").strip())
    database = root / "gateway.db"
    if database.exists():
        import sqlite3

        with sqlite3.connect(database.as_uri() + "?mode=ro", uri=True) as connection:
            for (session,) in connection.execute("SELECT session FROM accounts"):
                cipher.decrypt(session)


def stop_gateway(root: Path, *, shared: bool, timeout: float = 30) -> None:
    if not shared:
        raise ValueError("Stopping the common gateway requires --shared")
    try:
        record = endpoint(root)
    except GatewayProtocolError:
        record = None  # Explicit --shared control may stop an older verified installation.
    if record is None:
        path = root / "gateway.endpoint.json"
        if not path.exists():
            return
        record = json.loads(path.read_text())
        if (record.get("role") != "gateway" or record.get("store_root") != str(root.resolve())
                or not record.get("start_identity")
                or not record.get("python")
                or str(Path(record["python"]).resolve()) != process_executable(record["pid"])
                or process_start_identity(record["pid"]) != record["start_identity"]):
            raise RuntimeError("Cannot verify the gateway process owner; refusing to signal its PID")
        os.kill(record["pid"], signal.SIGTERM)
    else:
        token = (root / "gateway-admin.token").read_text().strip()
        with httpx.Client(timeout=5, trust_env=False) as client:
            response = client.post(f"http://127.0.0.1:{record['port']}/v1/admin/shutdown",
                                   headers={"Authorization": f"Bearer {token}"})
            response.raise_for_status()
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process_start_identity(record["pid"]) != record["start_identity"]:
            return
        time.sleep(0.05)
    raise TimeoutError("Shared gateway did not stop; no unrelated process was signalled")


def restart_gateway(root: Path, *, shared: bool) -> str:
    if not shared:
        raise ValueError("Restarting the common gateway requires --shared")
    profile = json.loads((root / "gateway.json").read_text())
    checked = subprocess.run([profile["python"], "-m", "tg_agent.gateway.runtime", "--root", str(root), "--check"],
                             cwd=profile["module_root"], capture_output=True, text=True,
                             env={**os.environ, "PYTHONPATH": os.pathsep.join(profile["python_path"])}, timeout=30)
    if checked.returncode:
        raise RuntimeError("Gateway restart preflight failed; the running gateway was left intact")
    stop_gateway(root, shared=True)
    try:
        return ensure_gateway(root)
    except Exception as exc:
        raise RuntimeError("Shared gateway restart failed after stop; gateway.log contains startup details") from exc


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Internal shared Telegram gateway process")
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    if args.check:
        preflight(args.root)
    else:
        asyncio.run(serve(args.root))
