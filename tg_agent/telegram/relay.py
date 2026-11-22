"""Session-owner relay: one Telegram connection per account, any number of clients (#1868).

The process that owns an account's Telegram connection — the worker daemon, or the
shared gateway — listens on 127.0.0.1 behind a random token. Every other process
builds an ordinary :class:`~tg_agent.telegram.client_pool.ClientPool` whose Telethon
clients have no auth key and no network connection: each MTProto request is
serialized (``bytes(request)``), executed by the owner on its own connected client
(through the owner's flood guards, which wrap ``sender.send``), and the owner sends
back the *raw* TL bytes of the answer, which the client parses with the request's
own ``read_result``. One authorization, one connection, one flood budget, and the
client keeps running the exact native code paths.

Wire format: one JSON object per line. Requests carry an ``id``; replies echo it
with ``ok`` or ``error``. The first message must be ``hello`` (token + protocol
version). Owners may register extra operations and push messages (``push`` key,
no id) for control-plane use.
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import hmac
import json
import logging
import os
import secrets
import socket
import sys
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from telethon import TelegramClient, errors, utils
from telethon.errors import RPCError
from telethon.extensions import BinaryReader
from telethon.tl import functions
from telethon.tl.alltlobjects import LAYER
from telethon.tl.tlobject import TLRequest
from telethon_floodgate import TelegramRateLimitedError, TelegramRpcDeferredError

from tg_shared.sessions import FreshEntityStringSession

logger = logging.getLogger(__name__)

RELAY_PROTOCOL = 1
_LINE_LIMIT = 64 * 1024 * 1024
_HANDSHAKE_TIMEOUT_SEC = 10.0
_DRAIN_TIMEOUT_SEC = 30.0

# Requests a relayed client must never run on the owner's authorization: they
# would log the owner out, tear down the account's sessions, or take the account
# itself (phone number, 2FA password, self-destruct period) away from the owner.
_FORBIDDEN_REQUESTS = (
    functions.account.ResetAuthorizationRequest,
    functions.account.SetAuthorizationTTLRequest,
    functions.account.DeleteAccountRequest,
    functions.account.SetAccountTTLRequest,
    functions.account.SendChangePhoneCodeRequest,
    functions.account.ChangePhoneRequest,
    functions.account.UpdatePasswordSettingsRequest,
    functions.account.ResetPasswordRequest,
)
_FORBIDDEN_MODULE_SUFFIX = ".functions.auth"
# Wrappers a client may put around a request; the wrapped query is what gets
# checked. Every other wrapper (InvokeWithLayer, InitConnection, device and
# business attestations, anything a future layer adds) would re-initialize or
# re-route the owner's connection and is refused outright.
_PASS_THROUGH_WRAPPERS = tuple(
    wrapper
    for wrapper in (
        getattr(functions, name, None)
        for name in (
            "InvokeWithoutUpdatesRequest",
            "InvokeAfterMsgRequest",
            "InvokeAfterMsgsRequest",
            "InvokeWithTakeoutRequest",
            "InvokeWithMessagesRangeRequest",
        )
    )
    if wrapper is not None
)
_MAX_WRAPPER_DEPTH = 8
# Requests whose repeat changes nothing visible on Telegram: reads, file parts (a
# part re-sent under the same file id overwrites itself) and album media uploads
# (no message until SendMultiMedia). Any other request may have taken effect when
# the link broke, so its loss is "delivery unconfirmed".
_READ_PREFIXES = ("Get", "Search", "Resolve", "Check")
_REPEATABLE_WRITES = (
    functions.upload.SaveFilePartRequest,
    functions.upload.SaveBigFilePartRequest,
    functions.messages.UploadMediaRequest,
)
_ACTION_READS = (functions.messages.GetBotCallbackAnswerRequest,)  # presses an inline button


def _repeatable(query: Any) -> bool:
    if isinstance(query, _REPEATABLE_WRITES):
        return True
    return type(query).__name__.startswith(_READ_PREFIXES) and not isinstance(query, _ACTION_READS)


class RelayError(RuntimeError):
    """A relay failure that is not a Telegram error."""


class RelayProtocolError(RelayError):
    """The two sides speak different relay protocols, or the handshake broke."""


class RelayLayerMismatchError(RelayProtocolError):
    """Owner and client run different Telegram TL layers, i.e. different Telethon.

    Raw MTProto only round-trips between equal layers. Restarting a daemon fixes
    it (it restarts from the project's current install); a shared gateway runs
    from the install it was enrolled with, so that install must be upgraded.
    """


class RelayUnauthorizedError(RelayError):
    """The relay token was refused."""


class RelayForbiddenError(RelayError):
    """The owner refuses this request type on its authorization."""


class RelayAccountUnavailableError(ConnectionError):
    """The owner has no live connection for the requested account."""


class RelayDeliveryUnconfirmedError(ConnectionError):
    """The relay link broke after a write request could have reached the owner."""


class RelayRemoteError(RelayError):
    """An error reply from the owner; ``payload`` carries the serialized cause."""

    def __init__(self, payload: dict):
        super().__init__(str(payload.get("message") or payload.get("kind") or "relay error"))
        self.payload = payload

    @property
    def status(self) -> int | None:
        status = self.payload.get("status")
        return status if isinstance(status, int) else None


# --------------------------------------------------------------------------- #
# Endpoint discovery
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class RelayEndpoint:
    port: int
    token: str
    pid: int
    protocol: int


def write_endpoint(path: Path, endpoint: RelayEndpoint) -> None:
    """Publish the endpoint atomically, readable by the owning user only (0600)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(endpoint.__dict__, handle)
        os.replace(temporary, path)
    finally:
        with contextlib.suppress(FileNotFoundError):
            temporary.unlink()


def read_endpoint(path: Path) -> RelayEndpoint | None:
    try:
        record = json.loads(path.read_text(encoding="utf-8"))
        return RelayEndpoint(
            port=int(record["port"]),
            token=str(record["token"]),
            pid=int(record["pid"]),
            protocol=int(record["protocol"]),
        )
    except (OSError, ValueError, KeyError, TypeError):
        return None


def remove_endpoint(path: Path, endpoint: RelayEndpoint) -> None:
    """Remove the endpoint file only if it still describes this owner."""
    current = read_endpoint(path)
    if current is not None and current.port == endpoint.port and current.token == endpoint.token:
        with contextlib.suppress(FileNotFoundError):
            path.unlink()


# --------------------------------------------------------------------------- #
# Error transport
# --------------------------------------------------------------------------- #


def _plain(values: Iterable[Any]) -> list | None:
    out = list(values)
    return out if all(isinstance(v, (str, int, float, bool, type(None))) for v in out) else None


def error_payload(exc: BaseException) -> dict:
    """Serialize an exception so the client can raise an equivalent one."""
    if isinstance(exc, RPCError):
        reduced = exc.__reduce__()
        args = _plain(reduced[1][1:]) if len(reduced) > 1 and reduced[1] else []
        if args is not None:
            return {"kind": "rpc", "name": type(exc).__name__, "args": args, "message": str(exc)}
        return {"kind": "rpc", "name": "RPCError", "args": [str(exc), getattr(exc, "code", None)],
                "message": str(exc)}
    if isinstance(exc, TelegramRateLimitedError):
        # Which of the owner's limiters refused (gate, resolve_username, flood_backoff, …):
        # the client names it, a username-budget refusal is not a gate one.
        refused_by = ({"rpc": exc.rpc, "operation": exc.operation, "budget": exc.budget}
                      if isinstance(exc, TelegramRpcDeferredError) else {})
        return {"kind": "rate_limited", "phone": exc.phone, "category": exc.category,
                "retry_after_sec": float(exc.retry_after_sec), "message": str(exc), **refused_by}
    if isinstance(exc, RelayAccountUnavailableError):
        return {"kind": "unavailable", "message": str(exc)}
    if isinstance(exc, RelayForbiddenError):
        return {"kind": "forbidden", "message": str(exc)}
    if isinstance(exc, RelayLayerMismatchError):
        return {"kind": "layer", "message": str(exc)}
    if isinstance(exc, RelayProtocolError):
        return {"kind": "protocol", "message": str(exc)}
    if isinstance(exc, RelayUnauthorizedError):
        return {"kind": "unauthorized", "message": str(exc)}
    if isinstance(exc, RelayRemoteError):
        return exc.payload
    if isinstance(exc, (ConnectionError, TimeoutError)):
        return {"kind": "connection", "message": str(exc) or type(exc).__name__}
    payload = {"kind": "error", "name": type(exc).__name__, "message": str(exc)}
    status = getattr(exc, "status", None)
    if isinstance(status, int):
        payload["status"] = status
    return payload


def rebuild_error(payload: dict, request: Any = None) -> BaseException:
    """The client-side twin of :func:`error_payload`."""
    kind = payload.get("kind")
    message = str(payload.get("message") or "")
    if kind == "rpc":
        cls = getattr(errors, str(payload.get("name") or ""), None)
        if isinstance(cls, type) and issubclass(cls, RPCError):
            with contextlib.suppress(TypeError):
                return cls(request, *(payload.get("args") or []))
        return RPCError(request, message or "RPC error")
    if kind == "rate_limited":
        phone = str(payload.get("phone") or "")
        category = str(payload.get("category") or "relay")
        retry_after = float(payload.get("retry_after_sec") or 0.0)
        budget = payload.get("budget")
        if isinstance(budget, str):
            return TelegramRpcDeferredError(
                phone, str(payload.get("operation") or category), retry_after,
                str(payload.get("rpc") or ""), category=category, budget=budget,
            )
        return TelegramRateLimitedError(phone, category, retry_after, message=message or None)
    if kind == "unavailable":
        return RelayAccountUnavailableError(message)
    if kind == "forbidden":
        return RelayForbiddenError(message)
    if kind == "layer":
        return RelayLayerMismatchError(message)
    if kind == "protocol":
        return RelayProtocolError(message)
    if kind == "unauthorized":
        return RelayUnauthorizedError(message)
    if kind == "connection":
        return ConnectionError(message)
    return RelayRemoteError(payload)


def _dump(message: dict) -> bytes:
    return json.dumps(message, separators=(",", ":")).encode("utf-8") + b"\n"


# --------------------------------------------------------------------------- #
# Owner side
# --------------------------------------------------------------------------- #


class RelayOwner(Protocol):
    """What a relay server needs from the process that owns the connections.

    ``authorize`` turns a token into a grant (``None`` refuses it); ``phones``
    lists the accounts a grant reaches; ``client`` returns the live Telethon
    client of one of them and raises :class:`RelayAccountUnavailableError` for a
    phone the grant does not reach. ``client`` runs on every request, so it is
    the one place that enforces access — a revoked grant stops the next request.
    """

    async def authorize(self, token: str) -> Any | None: ...

    async def phones(self, grant: Any) -> list[str]: ...

    async def client(self, grant: Any, phone: str) -> Any: ...


OpHandler = Callable[[Any, dict, "RelayPeer"], Awaitable[Any]]


def _read_raw(reader: BinaryReader) -> bytes:
    return reader.read()


async def _remember_entities(client: Any, read_result: Callable[[BinaryReader], Any], raw: bytes) -> None:
    """Cache the answer's users and chats on the owner, as Telethon does after a local call.

    The owner's client saw only raw bytes, and every relayed lease seeds its cache
    from the owner's: without this each lease resolves the same peers again.
    """
    try:
        with BinaryReader(raw) as reader:
            await utils.maybe_async(client.session.process_entities(read_result(reader)))
    except Exception:  # the client parses the same bytes and reports a failure itself
        logger.debug("relay: the owner could not cache an answer's entities", exc_info=True)


def decode_request(body: str) -> TLRequest:
    with BinaryReader(base64.b64decode(body)) as reader:
        request = reader.tgread_object()
    if not isinstance(request, TLRequest):
        raise RelayProtocolError(f"not a Telegram request: {type(request).__name__}")
    return request


def check_allowed(request: TLRequest) -> None:
    """Refuse what must not run on the owner's authorization, wrapped or not."""
    current = request
    depth = 0
    while isinstance(getattr(current, "query", None), TLRequest):
        if not isinstance(current, _PASS_THROUGH_WRAPPERS):
            raise RelayForbiddenError(
                f"{type(current).__name__} would re-initialize the session owner's connection; refused"
            )
        depth += 1
        if depth > _MAX_WRAPPER_DEPTH:
            raise RelayForbiddenError("request wrappers nest too deeply; refused")
        current = current.query
    if isinstance(current, _FORBIDDEN_REQUESTS) or type(current).__module__.endswith(_FORBIDDEN_MODULE_SUFFIX):
        raise RelayForbiddenError(
            f"{type(current).__name__} is refused through the session owner relay"
        )


def entity_rows(session: Any) -> list[list]:
    """Rows of a Telethon session's entity cache: ``[id, hash, username, phone, name]``."""
    entities = getattr(session, "_entities", None)
    if callable(getattr(session, "_cursor", None)):
        # SQLiteSession keeps its cache in the database; the inherited
        # in-memory set stays empty.
        cursor = session._cursor()
        try:
            rows = cursor.execute("select id, hash, username, phone, name from entities").fetchall()
        finally:
            cursor.close()
    elif isinstance(entities, (set, list, tuple)):
        rows = list(entities)
    else:
        rows = []
    return [[row[0], row[1], row[2], None if row[3] is None else str(row[3]), row[4]] for row in rows]


class RelayPeer:
    """One client connection as owner operations see it: push messages, check liveness."""

    def __init__(self, writer: asyncio.StreamWriter):
        self._writer = writer
        self._lock = asyncio.Lock()

    @property
    def closed(self) -> bool:
        return self._writer.is_closing()

    async def push(self, kind: str, **fields: Any) -> None:
        await self.send({"push": kind, **fields})

    async def send(self, message: dict) -> None:
        async with self._lock:
            if self._writer.is_closing():
                return
            self._writer.write(_dump(message))
            try:
                async with asyncio.timeout(_DRAIN_TIMEOUT_SEC):
                    await self._writer.drain()
            except TimeoutError:
                # A client that stopped reading (hung, SIGSTOPped) must not hold the
                # owner's tasks hostage: drop it, as if its connection broke.
                self._writer.transport.abort()
                raise ConnectionError("the relay client stopped reading; connection dropped") from None


class RelayServer:
    """Serve relayed MTProto requests (and owner-specific operations) on loopback."""

    def __init__(self, owner: RelayOwner, *, ops: dict[str, OpHandler] | None = None):
        self._owner = owner
        self._ops: dict[str, OpHandler] = {
            "botfather.create_bot": self._botfather_create_bot,
            "botfather.delete_bot": self._botfather_delete_bot,
            **(ops or {}),
        }
        self._server: asyncio.base_events.Server | None = None
        self._connections: set[asyncio.Task] = set()

    @property
    def port(self) -> int:
        if self._server is None:
            raise RuntimeError("relay server is not running")
        return self._server.sockets[0].getsockname()[1]

    async def start(self) -> int:
        self._server = await asyncio.start_server(self._accept, host="127.0.0.1", port=0, limit=_LINE_LIMIT)
        return self.port

    async def stop(self) -> None:
        if self._server is not None:
            self._server.close()
            for task in tuple(self._connections):
                task.cancel()
            await asyncio.gather(*self._connections, return_exceptions=True)
            await self._server.wait_closed()
            self._server = None

    async def _accept(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        task = asyncio.current_task()
        if task is not None:
            self._connections.add(task)
        peer = RelayPeer(writer)
        running: set[asyncio.Task] = set()
        try:
            grant = await self._handshake(reader, peer)
            if grant is None:
                return
            while True:
                line = await reader.readline()
                if not line:
                    return
                message = json.loads(line)
                job = asyncio.create_task(self._serve(grant, message, peer))
                running.add(job)
                job.add_done_callback(running.discard)
        except (asyncio.CancelledError, ConnectionError, asyncio.IncompleteReadError, ValueError):
            pass
        finally:
            for job in tuple(running):
                job.cancel()
            await asyncio.gather(*running, return_exceptions=True)
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()
            if task is not None:
                self._connections.discard(task)

    async def _handshake(self, reader: asyncio.StreamReader, peer: RelayPeer) -> Any | None:
        async with asyncio.timeout(_HANDSHAKE_TIMEOUT_SEC):
            line = await reader.readline()
        if not line:
            return None
        message = json.loads(line)
        reply_id = message.get("id", 0)
        if message.get("op") != "hello":
            await peer.send({"id": reply_id, "error": error_payload(RelayProtocolError("expected hello"))})
            return None
        if message.get("protocol") != RELAY_PROTOCOL:
            await peer.send({"id": reply_id, "error": error_payload(RelayProtocolError(
                f"the session owner speaks relay protocol {RELAY_PROTOCOL}, this client "
                f"{message.get('protocol')}: restart the owner so both run the same tg-agent version"
            ))})
            return None
        if message.get("layer") != LAYER:
            await peer.send({"id": reply_id, "error": error_payload(RelayLayerMismatchError(
                f"the session owner runs Telegram TL layer {LAYER} (Python {sys.executable}), this client "
                f"layer {message.get('layer')}: owner and clients must run the same Telethon — upgrade the "
                "older install, then restart the owner"
            ))})
            return None
        grant = await self._owner.authorize(str(message.get("token") or ""))
        if grant is None:
            await peer.send({"id": reply_id, "error": error_payload(RelayUnauthorizedError("relay token refused"))})
            return None
        await peer.send({"id": reply_id, "ok": {
            "protocol": RELAY_PROTOCOL,
            "layer": LAYER,
            "pid": os.getpid(),
            "phones": await self._owner.phones(grant),
        }})
        return grant

    async def _serve(self, grant: Any, message: dict, peer: RelayPeer) -> None:
        reply_id = message.get("id")
        try:
            op = message.get("op")
            if op == "invoke":
                result = await self._invoke(grant, message)
            elif op == "entities":
                result = await self._entities(grant, message)
            elif op == "phones":
                result = await self._owner.phones(grant)
            elif op in self._ops:
                result = await self._ops[op](grant, message, peer)
            else:
                raise RelayProtocolError(f"unknown relay operation {op!r}")
            await peer.send({"id": reply_id, "ok": result})
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001 - every failure must reach the client
            await peer.send({"id": reply_id, "error": error_payload(exc)})

    async def _invoke(self, grant: Any, message: dict) -> str:
        request = decode_request(str(message.get("body") or ""))
        check_allowed(request)
        client = await self._owner.client(grant, str(message.get("phone") or ""))
        read_result = request.read_result
        request.read_result = _read_raw  # the client parses the answer itself
        dc_id = message.get("dc")
        home = getattr(getattr(client, "session", None), "dc_id", None)
        if dc_id and dc_id != home:
            sender = await client._borrow_exported_sender(int(dc_id))
            try:
                raw = await client._call(sender, request)
            finally:
                await client._return_exported_sender(sender)
        else:
            raw = await client(request)
        if not isinstance(raw, (bytes, bytearray)):
            raise RelayError(f"the owner returned a parsed {type(raw).__name__}, not raw bytes")
        await _remember_entities(client, read_result, bytes(raw))
        return base64.b64encode(bytes(raw)).decode("ascii")

    # BotFather setup is a conversation: it waits for replies, which Telegram
    # pushes as updates to the session owner only. The owner runs the flow on its
    # own client and only the outcome crosses the relay.

    async def _botfather_create_bot(self, grant: Any, message: dict, _peer: RelayPeer) -> str:
        from tg_shared import botfather

        client = await self._owner.client(grant, str(message.get("phone") or ""))
        return await botfather.create_bot(client, str(message.get("name") or ""), str(message.get("username") or ""))

    async def _botfather_delete_bot(self, grant: Any, message: dict, _peer: RelayPeer) -> bool:
        from tg_shared import botfather

        client = await self._owner.client(grant, str(message.get("phone") or ""))
        try:
            await botfather.delete_bot(client, str(message.get("bot_username") or ""))
        except botfather.BotNotFoundError:
            return False
        return True

    async def _entities(self, grant: Any, message: dict) -> list[list]:
        client = await self._owner.client(grant, str(message.get("phone") or ""))
        return entity_rows(getattr(client, "session", None))


class PoolRelayOwner:
    """Relay owner backed by a live :class:`ClientPool` (the worker daemon)."""

    def __init__(self, pool: Any, token: str):
        self._pool = pool
        self._token = token

    async def authorize(self, token: str) -> bool | None:
        return True if hmac.compare_digest(token.encode(), self._token.encode()) else None

    async def phones(self, grant: Any) -> list[str]:
        return sorted(getattr(self._pool, "clients", {}))

    async def client(self, grant: Any, phone: str) -> Any:
        session = getattr(self._pool, "clients", {}).get(phone)
        if session is None:
            raise RelayAccountUnavailableError(f"the session owner has no live connection for {phone}")
        return getattr(session, "raw_client", session)


@dataclass
class RelayHandle:
    """A running relay plus its published endpoint; ``stop()`` retracts both."""

    server: RelayServer
    endpoint: RelayEndpoint
    endpoint_path: Path | None

    async def stop(self) -> None:
        if self.endpoint_path is not None:
            remove_endpoint(self.endpoint_path, self.endpoint)
        await self.server.stop()


async def serve_pool_relay(
    pool: Any, endpoint_path: Path | None, *, ops: dict[str, OpHandler] | None = None
) -> RelayHandle:
    """Start the worker's relay over ``pool`` and publish its endpoint file."""
    token = secrets.token_urlsafe(32)
    server = RelayServer(PoolRelayOwner(pool, token), ops=ops)
    port = await server.start()
    endpoint = RelayEndpoint(port=port, token=token, pid=os.getpid(), protocol=RELAY_PROTOCOL)
    if endpoint_path is not None:
        write_endpoint(endpoint_path, endpoint)
    logger.info("session owner relay listening on 127.0.0.1:%d", port)
    return RelayHandle(server=server, endpoint=endpoint, endpoint_path=endpoint_path)


def relay_endpoint_path(database_path: str | Path) -> Path:
    """Where a project's worker publishes its relay: next to the database."""
    return Path(database_path).with_suffix(".relay.json")


# --------------------------------------------------------------------------- #
# Client side
# --------------------------------------------------------------------------- #


async def _handshake(endpoint: RelayEndpoint, host: str):
    if endpoint.protocol != RELAY_PROTOCOL:
        raise RelayProtocolError(
            f"the session owner (PID {endpoint.pid}) speaks relay protocol {endpoint.protocol}, "
            f"this CLI {RELAY_PROTOCOL}: restart it (`tg-agent restart`)"
        )
    reader, writer = await asyncio.open_connection(host, endpoint.port, limit=_LINE_LIMIT)
    try:
        writer.write(_dump({"id": 0, "op": "hello", "token": endpoint.token,
                            "protocol": RELAY_PROTOCOL, "layer": LAYER}))
        await writer.drain()
        async with asyncio.timeout(_HANDSHAKE_TIMEOUT_SEC):
            line = await reader.readline()
        if not line:
            raise RelayProtocolError("the session owner closed the relay during the handshake")
        reply = json.loads(line)
        if "error" in reply:
            raise rebuild_error(reply["error"])
        hello = reply.get("ok") or {}
        if hello.get("protocol") != RELAY_PROTOCOL or hello.get("layer") != LAYER:
            raise RelayProtocolError(
                "the session owner answered with another relay protocol or TL layer: "
                "restart it (`tg-agent restart`)"
            )
    except BaseException:
        writer.close()
        with contextlib.suppress(Exception):
            await writer.wait_closed()
        raise
    return reader, writer, hello


def probe_endpoint(endpoint: RelayEndpoint, *, timeout: float = 1.0) -> dict | None:
    """Blocking handshake for synchronous callers: the owner's hello, or ``None``.

    Raises :class:`RelayProtocolError` when an owner answers but speaks another
    relay protocol or TL layer — that one needs a restart, not a second owner.
    """
    try:
        with socket.create_connection(("127.0.0.1", endpoint.port), timeout=timeout) as conn:
            conn.sendall(_dump({"id": 0, "op": "hello", "token": endpoint.token,
                                "protocol": RELAY_PROTOCOL, "layer": LAYER}))
            with conn.makefile("rb") as stream:
                line = stream.readline()
    except OSError:
        return None
    if not line:
        return None
    try:
        reply = json.loads(line)
    except ValueError:
        return None
    if "error" in reply:
        error = rebuild_error(reply["error"])
        if isinstance(error, RelayProtocolError):
            raise error
        return None
    return reply.get("ok") or None


class RelayConnection:
    """One multiplexed connection to the owner, shared by every relay client of a process.

    A connection opened with ``reconnect`` (an async callable returning a fresh
    endpoint) reopens itself on the next call after the owner went away — for
    long-lived clients such as a project worker in gateway mode. A request that
    was in flight when the link dropped still fails: writes are never replayed.
    """

    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        hello: dict,
        *,
        host: str = "127.0.0.1",
        reconnect: Callable[[], Awaitable[RelayEndpoint]] | None = None,
    ):
        self._reader = reader
        self._writer = writer
        self.hello = hello
        self._host = host
        self._reconnect = reconnect
        self._reopen_lock = asyncio.Lock()
        self._pending: dict[int, asyncio.Future] = {}
        self._next_id = 0
        self._write_lock = asyncio.Lock()
        self._closed = False
        self._explicitly_closed = False
        self._push_handlers: list[Callable[[dict], Any]] = []
        self._replay: list[tuple[str, dict]] = []
        self._reader_task: asyncio.Task | None = None

    @property
    def phones(self) -> list[str]:
        return list(self.hello.get("phones") or [])

    @property
    def is_open(self) -> bool:
        return not self._closed or (self._reconnect is not None and not self._explicitly_closed)

    @classmethod
    async def open(
        cls,
        endpoint: RelayEndpoint,
        *,
        host: str = "127.0.0.1",
        reconnect: Callable[[], Awaitable[RelayEndpoint]] | None = None,
    ) -> RelayConnection:
        reader, writer, hello = await _handshake(endpoint, host)
        connection = cls(reader, writer, hello, host=host, reconnect=reconnect)
        connection._reader_task = asyncio.create_task(connection._read_loop(), name="relay-reader")
        return connection

    def on_push(self, handler: Callable[[dict], Any]) -> None:
        self._push_handlers.append(handler)

    async def refresh_phones(self) -> list[str]:
        """Re-read the accounts the owner serves (grants change while connected)."""
        self.hello = {**self.hello, "phones": await self.call("phones")}
        return self.phones

    async def subscribe(self, op: str, **fields: Any) -> Any:
        """Call ``op`` now and again after every reconnect (push subscriptions)."""
        self._replay.append((op, fields))
        return await self.call(op, **fields)

    async def _ensure_open(self) -> None:
        if not self._closed:
            return
        if self._reconnect is None or self._explicitly_closed:
            raise ConnectionError("the relay connection to the session owner is closed")
        async with self._reopen_lock:
            if not self._closed:
                return
            endpoint = await self._reconnect()
            self._reader, self._writer, self.hello = await _handshake(endpoint, self._host)
            self._closed = False
            self._reader_task = asyncio.create_task(self._read_loop(), name="relay-reader")
            for op, fields in self._replay:
                await self._send_and_wait(op, fields)

    async def call(self, op: str, *, delivery_sensitive: bool = False, **fields: Any) -> Any:
        await self._ensure_open()
        return await self._send_and_wait(op, fields, delivery_sensitive=delivery_sensitive)

    async def _send_and_wait(self, op: str, fields: dict, *, delivery_sensitive: bool = False) -> Any:
        self._next_id += 1
        request_id = self._next_id
        frame = _dump({"id": request_id, "op": op, **fields})
        future = None
        try:
            async with self._write_lock:
                if self._closed or self._writer.is_closing():
                    raise ConnectionError("the relay connection to the session owner is closed")
                future = asyncio.get_running_loop().create_future()
                self._pending[request_id] = future
                self._writer.write(frame)
                try:
                    await self._writer.drain()
                except OSError:
                    if not future.done():
                        raise
            return await future
        except OSError as exc:
            if delivery_sensitive and future is not None:
                raise RelayDeliveryUnconfirmedError("Relay response lost after sending — delivery unconfirmed") from exc
            raise
        finally:
            self._pending.pop(request_id, None)
            if future is not None and future.done() and not future.cancelled():
                future.exception()

    async def _read_loop(self) -> None:
        reader = self._reader
        try:
            while True:
                line = await reader.readline()
                if not line:
                    return
                message = json.loads(line)
                if "push" in message:
                    for handler in tuple(self._push_handlers):
                        try:
                            outcome = handler(message)
                            if asyncio.iscoroutine(outcome):
                                await outcome
                        except Exception:
                            logger.exception("relay push handler failed")
                    continue
                future = self._pending.get(message.get("id"))
                if future is None or future.done():
                    continue
                if "error" in message:
                    future.set_exception(RelayRemoteError(message["error"]))
                else:
                    future.set_result(message.get("ok"))
        except (ConnectionError, asyncio.IncompleteReadError, ValueError):
            pass
        finally:
            if reader is self._reader:
                self._closed = True
                for future in self._pending.values():
                    if not future.done():
                        future.set_exception(ConnectionError("the relay connection to the session owner was lost"))

    async def close(self) -> None:
        self._explicitly_closed = True
        self._closed = True
        self._writer.close()
        with contextlib.suppress(Exception):
            await self._writer.wait_closed()
        if self._reader_task is not None:
            self._reader_task.cancel()
            await asyncio.gather(self._reader_task, return_exceptions=True)


class RelaySender:
    """Stands in for Telethon's ``MTProtoSender``: every send goes to the owner."""

    def __init__(self, connection: RelayConnection, phone: str, *, dc_id: int | None = None):
        self._connection = connection
        self._phone = phone
        self.dc_id = dc_id
        self._connected = dc_id is not None

    def is_connected(self) -> bool:
        return self._connected and self._connection.is_open

    async def connect(self, *_args: Any, **_kwargs: Any) -> bool:
        self._connected = True
        return True

    async def disconnect(self) -> None:
        self._connected = False

    def send(self, request: Any, ordered: bool = False) -> Any:
        loop = asyncio.get_running_loop()
        if isinstance(request, list):
            futures = [loop.create_future() for _ in request]
            loop.create_task(self._send_batch(request, futures, ordered))
            return futures
        return loop.create_task(self._invoke(request))

    async def _send_batch(self, requests: list, futures: list[asyncio.Future], ordered: bool) -> None:
        async def settle(request: Any, future: asyncio.Future) -> None:
            try:
                result = await self._invoke(request)
            except BaseException as exc:  # noqa: BLE001 - delivered through the future
                if not future.done():
                    future.set_exception(exc)
            else:
                if not future.done():
                    future.set_result(result)

        if ordered:
            for request, future in zip(requests, futures, strict=True):
                await settle(request, future)
        else:
            await asyncio.gather(*(settle(r, f) for r, f in zip(requests, futures, strict=True)))

    async def _invoke(self, request: Any) -> Any:
        body = base64.b64encode(bytes(request)).decode("ascii")
        query = request
        while isinstance(query, _PASS_THROUGH_WRAPPERS):
            query = query.query
        try:
            raw = await self._connection.call(
                "invoke", delivery_sensitive=not _repeatable(query),
                phone=self._phone, dc=self.dc_id, body=body,
            )
        except RelayRemoteError as exc:
            raise rebuild_error(exc.payload, request) from None
        with BinaryReader(base64.b64decode(raw)) as reader:
            return request.read_result(reader)


class RelayTelegramClient(TelegramClient):
    """A Telethon client without an auth key: the owner's connection does the I/O."""

    def __init__(self, connection: RelayConnection, phone: str, api_id: int = 0, api_hash: str = "", **kwargs: Any):
        super().__init__(FreshEntityStringSession(), api_id or 1, api_hash or "relay", **kwargs)
        self._relay_connection = connection
        self._relay_phone = phone
        self._sender = RelaySender(connection, phone)
        self._relay_dc_senders: dict[int, RelaySender] = {}

    async def connect(self) -> None:
        if self._loop is None:
            self._loop = asyncio.get_running_loop()
        await self._sender.connect()

    async def _disconnect_coro(self) -> None:
        await self._sender.disconnect()

    async def _borrow_exported_sender(self, dc_id: int) -> RelaySender:
        # One sender per DC, like Telethon's own cache: the client-side flood gate
        # wraps every sender it is handed, so a fresh one per download would grow
        # its patched set for the client's whole life.
        sender = self._relay_dc_senders.get(dc_id)
        if sender is None:
            sender = self._relay_dc_senders[dc_id] = RelaySender(self._relay_connection, self._relay_phone,
                                                                 dc_id=dc_id)
        return sender

    async def _return_exported_sender(self, sender: Any) -> None:
        return None

    async def _get_cdn_client(self, _cdn_redirect: Any) -> Any:
        raise RelayError("CDN file downloads are not available through the session owner relay")

    def conversation(self, *args: Any, **kwargs: Any) -> Any:
        # A conversation waits for updates, and updates reach only the session
        # owner: fail at once instead of timing out a minute later.
        raise RelayError("conversations need the account's updates, which only the session owner receives; "
                         "run the flow on the owner (tg_agent.telegram.botfather_flows)")

    async def _owner_call(self, op: str, **fields: Any) -> Any:
        # Owner-side failures come back as the errors a local call would raise
        # (Telegram RPC errors, local rate limits, an unavailable account).
        try:
            return await self._relay_connection.call(op, phone=self._relay_phone, **fields)
        except RelayRemoteError as exc:
            raise rebuild_error(exc.payload) from None

    async def owner_create_bot(self, name: str, username: str) -> str:
        """Create a bot through BotFather on the owner's client; returns its token."""
        return await self._owner_call("botfather.create_bot", name=name, username=username)

    async def owner_delete_bot(self, bot_username: str) -> bool:
        """Delete a bot through BotFather on the owner's client; False if BotFather does not list it."""
        return await self._owner_call("botfather.delete_bot", bot_username=bot_username)

    async def load_owner_entities(self) -> int:
        """Seed the entity cache from the owner's, so cached lookups stay offline."""
        rows = await self._owner_call("entities")
        self.session.add_entity_rows(rows)
        return len(rows)
