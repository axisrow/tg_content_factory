"""Fixed-installation, loopback-only gateway runtime and serialized autostart.

The gateway process owns the shared accounts' connections and serves the
session-owner relay (#1868); the discovery record ``gateway.endpoint.json``
names its port.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import secrets
import signal
import subprocess
import sys
import threading
import time
from logging.handlers import RotatingFileHandler
from pathlib import Path

import tg_shared
from tg_agent.gateway.server import GatewayHub
from tg_agent.gateway.store import GatewayStore, private_key
from tg_agent.gateway.telegram import LiveTelegramAccess
from tg_agent.telegram.relay import (
    RELAY_PROTOCOL,
    RelayEndpoint,
    RelayLayerMismatchError,
    RelayProtocolError,
    RelayServer,
    probe_endpoint,
)
from tg_agent.telegram.session_lock import SessionLock
from tg_agent.utils.process_identity import process_executable, process_start_identity
from tg_shared.auth import TelegramAuth

logger = logging.getLogger(__name__)


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


def _admin_token(root: Path) -> str:
    return (root / "gateway-admin.token").read_text(encoding="utf-8").strip()


def endpoint(root: Path) -> dict | None:
    """The live gateway's discovery record, verified by a relay handshake.

    Raises :class:`GatewayProtocolError` when a gateway answers but runs another
    version (an older HTTP gateway, or another relay protocol): it must be
    restarted explicitly, never replaced by a caller.
    """
    try:
        record = json.loads((root / "gateway.endpoint.json").read_text(encoding="utf-8"))
        port = record["port"]
        if type(port) is not int or not 0 < port < 65536:
            return None
    except (OSError, ValueError, KeyError):
        return None
    if record.get("protocol") is None:
        if record.get("pid") and process_start_identity(record["pid"]) == record.get("start_identity"):
            raise GatewayProtocolError(
                "An older Telegram gateway is running (PID %s); restart it once: "
                "`tg-agent gateway restart --shared`" % record["pid"]
            )
        return None
    try:
        hello = probe_endpoint(
            RelayEndpoint(port=port, token=_admin_token(root), pid=int(record.get("pid") or 0),
                          protocol=int(record["protocol"])),
        )
    except RelayLayerMismatchError as exc:
        raise GatewayProtocolError(
            f"The shared Telegram gateway and this project run different Telethon versions ({exc}). "
            "Every project sharing the gateway must run the same tg-agent/Telethon; the gateway runs from "
            "the install it was enrolled with, so upgrade that install (or this project), then "
            "`tg-agent gateway restart --shared`"
        ) from None
    except RelayProtocolError as exc:
        raise GatewayProtocolError(
            f"The running Telegram gateway speaks another protocol ({exc}); restart it once: "
            "`tg-agent gateway restart --shared`"
        ) from None
    except OSError:
        return None
    if hello is None or hello.get("pid") != record.get("pid"):
        return None
    return record


def _enrolled_profile(root: Path) -> dict:
    """Read the pinned launcher profile without creating anything on disk.

    Validating before any lock or directory is created keeps a misconfigured or
    mock-config caller from leaving junk paths behind (PR #1715 review).
    """
    path = root / "gateway.json"
    if not path.exists():
        raise RuntimeError(f"Gateway store is not enrolled; expected {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def ensure_gateway(root: Path, *, timeout: float = 30) -> dict:
    """The live gateway's discovery record, starting it first when it is down.

    All callers launch the enrolled binary; the project cwd is irrelevant.
    """
    root = root.resolve()
    current = endpoint(root)
    if current:
        return current
    _enrolled_profile(root)
    launcher = SessionLock("gateway launcher", str(root / "launcher"), lock_dir=root / "locks")
    launcher.acquire(grace_seconds=timeout)
    try:
        current = endpoint(root)
        if current:
            return current
        profile = _enrolled_profile(root)
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
                return current
            if process.poll() is not None:
                raise RuntimeError(f"Telegram gateway failed to start; see {root / 'gateway.log'}")
            time.sleep(0.1)
        raise TimeoutError("Telegram gateway startup timed out; no direct session fallback is permitted")
    finally:
        launcher.release()


async def serve(root: Path, *, access_factory=None) -> None:
    root = await asyncio.to_thread(root.resolve)
    profile = _enrolled_profile(root)
    owner = SessionLock("gateway", str(root / "gateway owner"), lock_dir=root / "locks")
    owner.acquire(grace_seconds=0)
    store = GatewayStore(root)
    path = root / "gateway.endpoint.json"
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop.set)
        except (NotImplementedError, RuntimeError):
            pass
    server = None
    hub = None
    access = None
    cleanup = None
    try:
        await store.open()
        access = (access_factory(store) if access_factory is not None
                  else LiveTelegramAccess(store, TelegramAuth(profile["api_id"], profile["api_hash"],
                                                              proxy=profile.get("proxy"))))
        hub = GatewayHub(store, access, admin_token=await asyncio.to_thread(_admin_token, root),
                         request_shutdown=stop.set)
        start = getattr(access, "start", None)
        if start is not None:
            await start()

        async def expire_events():
            while True:
                try:
                    await store.expire_events(now=time.time())
                except Exception as exc:  # noqa: BLE001 - housekeeping must never stop the gateway
                    logger.warning("Gateway event expiry delayed: %s", type(exc).__name__)
                await asyncio.sleep(60)

        cleanup = asyncio.create_task(expire_events(), name="gateway-event-expiry")
        server = RelayServer(hub, ops=hub.ops)
        port = await server.start()
        record = {"port": port, "instance": secrets.token_urlsafe(32), "pid": os.getpid(),
                  "store_root": str(root), "python": sys.executable, "role": "gateway",
                  "protocol": RELAY_PROTOCOL, "start_identity": process_start_identity(os.getpid())}
        await asyncio.to_thread(_atomic_record, path, record)
        logger.info("Telegram gateway serving the relay on 127.0.0.1:%d", port)
        await stop.wait()
    finally:
        await asyncio.to_thread(path.unlink, missing_ok=True)
        if cleanup is not None:
            cleanup.cancel()
            await asyncio.gather(cleanup, return_exceptions=True)
        if server is not None:
            await server.stop()
        if hub is not None:
            await hub.close()
        if access is not None:
            await access.close()
        await store.close()
        owner.release()


def setup_gateway_logging(root: Path, *, max_bytes: int = 10 * 1024 * 1024,
                          backup_count: int = 3) -> RotatingFileHandler:
    """Timestamped, rotated ``gateway.log`` handler (#1780).

    The gateway process logs bare WARNING lines to stderr, which the launcher
    redirects into ``gateway.log`` — no timestamps, no rotation, which is how a
    216k-line error mass accumulated unnoticed. This handler routes the same
    records into the same file with timestamps and size-based rotation; the
    launcher's stdout redirect stays for pre-logging crash output (after a
    rotation those rare writes land in the rotated ``.log.N`` file). The file
    keeps the launcher's 0600 mode across rollovers.
    """

    class _PrivateRotatingHandler(RotatingFileHandler):
        # FileHandler._open recreates the file with umask perms on every
        # rollover, silently dropping 0600 — re-apply it at reopen time.
        def _open(self):
            stream = super()._open()
            os.chmod(self.baseFilename, 0o600)
            return stream

    handler = _PrivateRotatingHandler(str(root / "gateway.log"), maxBytes=max_bytes,
                                      backupCount=backup_count, encoding="utf-8")
    handler.setFormatter(logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
    attached = any(isinstance(h, RotatingFileHandler)
                   and getattr(h, "baseFilename", None) == str(root / "gateway.log")
                   for h in logging.getLogger().handlers)
    if not attached:
        logging.getLogger().addHandler(handler)
        os.chmod(root / "gateway.log", 0o600)
    return handler


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
        asyncio.run(_relay_admin_call(root, record, "admin.shutdown"))
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process_start_identity(record["pid"]) != record["start_identity"]:
            return
        time.sleep(0.05)
    raise TimeoutError("Shared gateway did not stop; no unrelated process was signalled")


async def _relay_admin_call(root: Path, record: dict, op: str, **fields):
    from tg_agent.telegram.relay import RelayConnection

    connection = await RelayConnection.open(
        RelayEndpoint(port=record["port"], token=_admin_token(root), pid=int(record["pid"]),
                      protocol=int(record["protocol"]))
    )
    try:
        return await connection.call(op, **fields)
    finally:
        await connection.close()


def restart_gateway(root: Path, *, shared: bool) -> dict:
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
        setup_gateway_logging(args.root)
        asyncio.run(serve(args.root))
