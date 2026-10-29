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


def configure(root: Path, *, api_id: int, api_hash: str) -> None:
    """Initial enrollment pins the launcher and private Telegram credentials."""
    if api_id <= 0 or not api_hash:
        raise ValueError("Gateway Telegram API credentials are required")
    private_key(root)
    profile = root / "gateway.json"
    if profile.exists():
        return
    contents = {"api_id": api_id, "api_hash": api_hash, "python": sys.executable,
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
        if health.get("instance") == record["instance"] and health.get("protocol") != 1:
            raise GatewayProtocolError("Incompatible Telegram gateway protocol; upgrade the shared gateway explicitly")
        if health == {"protocol": 1, "instance": record["instance"]}:
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


async def serve(root: Path) -> None:
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
        access = LiveTelegramAccess(store, TelegramAuth(profile["api_id"], profile["api_hash"]))
        app = create_app(store, access)

        @app.get("/v1/health")
        async def health():
            return {"protocol": 1, "instance": instance}

        listener.bind(("127.0.0.1", 0))
        listener.listen(128)
        listener.setblocking(False)
        record = {"port": listener.getsockname()[1], "instance": instance, "pid": os.getpid(),
                  "store_root": str(root), "python": sys.executable, "role": "gateway"}
        server = GatewayServer(uvicorn.Config(app, access_log=False, log_level="warning"))

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


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Internal shared Telegram gateway process")
    parser.add_argument("--root", required=True, type=Path)
    asyncio.run(serve(parser.parse_args().root))
