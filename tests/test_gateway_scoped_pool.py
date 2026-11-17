"""Gateway-side ScopedPool keeps the pool's call contract (#1841) — offline.

``TelegramActionService`` and ``TelegramSearch`` run inside the gateway over a
``ScopedPool`` instead of the real pool. Every wrapper there must accept the
calls core code makes on the pool: a wrapper that lags behind a pool signature
raises ``TypeError``, and the numeric resolver treats ``TypeError`` as a cache
miss — so the mismatch surfaced as "entity not found" with zero requests
dispatched for every numeric recipient (#1841, regression from #1838).

The executed tests drive a REAL ``TelegramClient`` (never connected) whose
MTProto sender is a recording stub, through the real gateway entry point
``LiveTelegramAccess.execute``; ``get_entity`` / ``get_input_entity`` are never
stubbed.
"""

import inspect
import secrets
from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import uuid4

import pytest
from telethon.sessions import StringSession
from telethon.tl.functions.messages import SendMessageRequest
from telethon.tl.functions.users import GetUsersRequest
from telethon.tl.types import Channel, InputUserSelf, UpdateShortSentMessage, User

from tg_agent.gateway.actions import ScopedPool
from tg_agent.gateway.protocol import ACTION_MODELS
from tg_agent.gateway.store import GatewayStore
from tg_agent.gateway.telegram import LiveTelegramAccess
from tg_agent.telegram.client_pool import ClientPool
from tg_agent.telegram.session_lock import release_all_session_locks

ACCOUNT = "scoped-account"
BARE = 1111111111
MARKED = -1_001_111_111_111
USER_ID = 4242


# ---------------------------------------------------------------------------
# Contract: a wrapper accepts every call shape its pool method accepts
# ---------------------------------------------------------------------------


class _SignatureCheckingPool:
    """Stands in for ``ClientPool``: each call must bind to the real signature."""

    def __init__(self):
        self.calls = []
        self.clients = {ACCOUNT: object()}

    def __getattr__(self, name):
        target = getattr(ClientPool, name)
        signature = inspect.signature(target)

        def call(*args, **kwargs):
            # Raises TypeError exactly when the real pool method would.
            signature.bind(self, *args, **kwargs)
            self.calls.append(name)
            return _resolved() if inspect.iscoroutinefunction(target) else None

        return call


async def _resolved():
    return None


def _wrapped_pool_methods():
    """Public ScopedPool methods that shadow a same-named ClientPool method."""
    return sorted(
        name
        for name, _ in inspect.getmembers(ScopedPool, inspect.isfunction)
        if not name.startswith("_") and callable(getattr(ClientPool, name, None))
    )


def _value_for(parameter_name):
    # Wrappers guard the account scope: any phone argument must be the scoped one.
    return ACCOUNT if parameter_name == "phone" else object()


def _pool_call_shapes(name):
    """The two call shapes a caller may use for the pool method: as declared, and all-keyword."""
    parameters = [
        parameter
        for parameter in inspect.signature(getattr(ClientPool, name)).parameters.values()
        if parameter.name != "self"
        and parameter.kind not in (parameter.VAR_POSITIONAL, parameter.VAR_KEYWORD)
    ]
    declared_args = [
        _value_for(p.name) for p in parameters if p.kind == p.POSITIONAL_OR_KEYWORD
    ]
    declared_kwargs = {p.name: _value_for(p.name) for p in parameters if p.kind == p.KEYWORD_ONLY}
    keyword_only = {p.name: _value_for(p.name) for p in parameters}
    return [(declared_args, declared_kwargs), ([], keyword_only)]


def test_scoped_pool_shadows_pool_methods():
    """The contract below must not silently run over an empty list."""
    wrapped = _wrapped_pool_methods()
    assert "resolve_dialog_entity" in wrapped
    assert "resolve_entity_with_warm" in wrapped
    assert "run_live_username_resolve" in wrapped


@pytest.mark.anyio
@pytest.mark.parametrize("name", _wrapped_pool_methods())
async def test_scoped_pool_wrapper_accepts_the_pool_call_shapes(name):
    """Any call valid for the pool method is valid for its ScopedPool wrapper.

    Red before the fix for ``resolve_dialog_entity`` (no ``allow_warm``) and
    ``run_live_username_resolve`` (keyword-only ``phone`` / ``username``
    forwarded positionally).
    """
    for args, kwargs in _pool_call_shapes(name):
        pool = _SignatureCheckingPool()
        scoped = ScopedPool(SimpleNamespace(_pool=pool, auth=object()), ACCOUNT)
        result = getattr(scoped, name)(*args, **kwargs)
        if inspect.isawaitable(result):
            await result


# ---------------------------------------------------------------------------
# Executed: business actions with numeric recipients through the gateway
# ---------------------------------------------------------------------------


def _recording_client(*seeded):
    """A real TelegramClient whose sender records every dispatched request."""
    from telethon import TelegramClient

    client = TelegramClient(StringSession(), 1, "x")
    dispatched = []

    async def send(request, ordered=False):
        dispatched.append(request)
        if isinstance(request, SendMessageRequest):
            return UpdateShortSentMessage(
                id=77, date=datetime.now(timezone.utc), out=True, pts=1, pts_count=1
            )
        return None

    client._sender.send = send
    # Offline: never attempt a real connect() from the pool's liveness check.
    client.is_connected = lambda: True
    client.session.process_entities(list(seeded))
    return client, dispatched


def _action_requests(dispatched):
    """Requests the action itself dispatched.

    Connecting a gateway account schedules a premium re-check (#1749) — one
    ``users.getUsers(self)`` that belongs to the connection, not to the send.
    Any other ``getUsers`` would be a resolve attempt and stays in the trace.
    """
    return [
        type(request).__name__
        for request in dispatched
        if not (isinstance(request, GetUsersRequest) and request.id == [InputUserSelf()])
    ]


def _channel():
    return Channel(id=BARE, title="known", access_hash=5, photo=None, date=None)


def _user():
    return User(id=USER_ID, access_hash=9, first_name="Pen")


async def _execute_send(tmp_path, monkeypatch, client, **arguments):
    monkeypatch.setenv("TGCF_SESSION_LOCK_DIR", str(tmp_path / "locks"))

    class RecordingAuth:
        async def create_client_from_session(self, session, **kwargs):
            return client

        async def cleanup(self):
            pass

    store = GatewayStore(tmp_path / "gateway")
    await store.open()
    access = LiveTelegramAccess(store, RecordingAuth())
    try:
        account = await store.import_account("+100", "local-transport-session")
        project = str(uuid4())
        await store.register_project(project, tmp_path / project, secrets.token_urlsafe(32))
        await store.grant(project, account)
        # Built exactly as the project-side RemoteActionService builds it.
        operation = ACTION_MODELS["send_message"](account_id=account, arguments=arguments)
        return await access.execute(project, operation)
    finally:
        await access.close()
        await store.close()
        release_all_session_locks()


@pytest.mark.anyio
@pytest.mark.native_backend_allowed
@pytest.mark.parametrize(
    "recipient",
    [str(MARKED), MARKED, str(BARE), BARE],
    ids=["marked-str", "marked-int", "bare-str", "bare-int"],
)
async def test_gateway_send_message_resolves_numeric_chat_ids(tmp_path, monkeypatch, recipient):
    """A known chat addressed by any numeric form costs exactly its SendMessage."""
    client, dispatched = _recording_client(_channel())

    result = await _execute_send(tmp_path, monkeypatch, client, recipient=recipient, text="hi")

    assert result["message_id"] == 77
    assert _action_requests(dispatched) == ["SendMessageRequest"]
    assert dispatched[-1].peer.channel_id == BARE


@pytest.mark.anyio
@pytest.mark.native_backend_allowed
async def test_gateway_send_message_resolves_numeric_user_id(tmp_path, monkeypatch):
    """The numeric-DM callers (ping, DM drafts) pass is_user=True through the gateway."""
    client, dispatched = _recording_client(_user())

    result = await _execute_send(
        tmp_path, monkeypatch, client, recipient=USER_ID, text="hi", is_user=True
    )

    assert result["message_id"] == 77
    assert _action_requests(dispatched) == ["SendMessageRequest"]
    assert dispatched[-1].peer.user_id == USER_ID
