from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from telethon.tl.types import InputPeerChannel

from src.config import AppConfig
from src.services.channel_service import ChannelService
from tests.helpers import AsyncIterMessages, FakeCliTelethonClient, build_web_app, make_auth_client

_FAKE_DIALOGS = [
    {
        "channel_id": -100111,
        "title": "My Channel",
        "username": "mychan",
        "channel_type": "channel",
        "deactivate": False,
        "is_own": False,
    },
    {
        "channel_id": -100222,
        "title": "My Group",
        "username": None,
        "channel_type": "supergroup",
        "deactivate": False,
        "is_own": False,
    },
    {
        "channel_id": 999,
        "title": "Some User",
        "username": "someuser",
        "channel_type": "dm",
        "deactivate": False,
        "is_own": False,
    },
    {
        "channel_id": 888,
        "title": "My Bot",
        "username": "mybot",
        "channel_type": "bot",
        "deactivate": False,
        "is_own": False,
    },
]


def _dialog_from_spec(spec: dict) -> MagicMock:
    entity = SimpleNamespace(
        id=spec["channel_id"],
        username=spec.get("username"),
        creator=spec.get("is_own", False),
        bot=spec.get("channel_type") == "bot",
        broadcast=spec.get("channel_type") == "channel",
        megagroup=spec.get("channel_type") == "supergroup",
        gigagroup=spec.get("channel_type") == "gigagroup",
        forum=spec.get("channel_type") == "forum",
        monoforum=spec.get("channel_type") == "monoforum",
        scam=spec.get("channel_type") == "scam",
        fake=spec.get("channel_type") == "fake",
        restricted=spec.get("channel_type") == "restricted",
    )
    dialog = MagicMock()
    dialog.entity = entity
    dialog.title = spec["title"]
    dialog.is_channel = spec.get("channel_type") not in ("dm", "bot")
    dialog.is_group = spec.get("channel_type") in ("group", "supergroup", "gigagroup", "forum")
    return dialog


def _make_dialog_client(dialogs: list[dict]) -> FakeCliTelethonClient:
    prepared = [_dialog_from_spec(dialog) for dialog in dialogs]
    return FakeCliTelethonClient(
        iter_dialogs_factory=lambda *a, **kw: AsyncIterMessages(prepared),
        entity_resolver=lambda _peer: MagicMock(),
    )


def _bind_dialog_cache_methods(pool):
    from src.telegram.client_pool import ClientPool

    pool._dialogs_cache = {}
    pool._dialogs_cache_ttl_sec = 60.0
    pool._dialogs_db_cache_ttl_sec = 3600.0
    pool.invalidate_dialogs_cache = ClientPool.invalidate_dialogs_cache.__get__(pool, ClientPool)
    pool._get_cached_dialogs = ClientPool._get_cached_dialogs.__get__(pool, ClientPool)
    pool._get_db_cached_dialogs = ClientPool._get_db_cached_dialogs.__get__(pool, ClientPool)
    pool._store_cached_dialogs = ClientPool._store_cached_dialogs.__get__(pool, ClientPool)
    # The #1046 decomposition rewrote get_dialogs_for_phone's internal dispatch
    # from the class-qualified ``ClientPool._fetch_dialogs_for_phone(self, …)`` to
    # ``self._fetch_dialogs_for_phone(…)`` (equivalent on a real instance). On a
    # MagicMock(spec=ClientPool) the latter resolves to a mock, so bind the real
    # collaborators these tests rely on (same pattern as the cache helpers above).
    pool._fetch_dialogs_for_phone = ClientPool._fetch_dialogs_for_phone.__get__(pool, ClientPool)
    pool._mark_degraded_cached_dialogs = ClientPool._mark_degraded_cached_dialogs.__get__(
        pool, ClientPool
    )
    # #1359 added the resumable-sweep collaborators; bind them for the same
    # reason as above, or the sweep silently runs against mocks.
    pool._flush_dialog_chunk = ClientPool._flush_dialog_chunk.__get__(pool, ClientPool)
    pool._dialog_refresh_tasks = {}


def _make_channel_dialog(
    channel_id: int,
    title: str = "Cached Channel",
    username: str = "cachedchan",
    *,
    message_id: int | None = None,
    message_date: datetime | None = None,
    with_message: bool = True,
):
    channel_entity = MagicMock()
    channel_entity.id = channel_id
    channel_entity.username = username
    channel_entity.creator = False

    dialog = MagicMock()
    dialog.entity = channel_entity
    dialog.title = title
    dialog.is_channel = True
    dialog.is_group = False
    # Real values (not MagicMock) so the resume cursor can actually anchor on
    # this dialog -- _advance_cursor deliberately ignores non-int/non-datetime
    # attributes, so a bare MagicMock would silently disable resume in tests.
    if with_message:
        dialog.message = SimpleNamespace(
            id=message_id if message_id is not None else abs(channel_id) % 100000,
            date=message_date or datetime(2026, 1, 1, tzinfo=timezone.utc),
        )
        dialog.input_entity = InputPeerChannel(channel_id=abs(channel_id), access_hash=0)
    else:
        dialog.message = None
        dialog.input_entity = InputPeerChannel(channel_id=abs(channel_id), access_hash=0)
    return dialog


def _strip_extra_dialog_fields(dialogs: list[dict]) -> list[dict]:
    return [
        {
            key: value
            for key, value in dialog.items()
            if key != "already_added" and not str(key).startswith("_")
        }
        for dialog in dialogs
    ]


async def _build_dialogs_app(db, real_pool_harness_factory, *, with_account=True):
    """Build app for dialogs tests with optional account."""
    config = AppConfig()
    config.telegram.api_id = 12345
    config.telegram.api_hash = "test_hash"
    config.web.password = "testpass"

    harness = real_pool_harness_factory()
    if with_account:
        harness.queue_cli_client(
            phone="+1234567890",
            client=_make_dialog_client(_FAKE_DIALOGS),
        )
        await harness.connect_account(
            "+1234567890",
            session_string="test_session",
            is_primary=True,
        )

    app, db = await build_web_app(config, harness, db=db)
    return app, db, harness


@pytest.fixture
async def client(db, real_pool_harness_factory):
    app, db, harness = await _build_dialogs_app(db, real_pool_harness_factory)

    async with make_auth_client(app) as c:
        yield c

    await app.state.collection_queue.shutdown()


@pytest.mark.anyio
async def test_dialogs_page_renders(client):
    resp = await client.get("/dialogs/")
    assert resp.status_code == 200
    assert "Диалоги" in resp.text
    assert 'href="/dialogs/photos"' in resp.text
    assert "Если аккаунт не выбран, откроется первый доступный профиль." in resp.text
    assert "Выберите аккаунт" in resp.text
    # The dialog list (incl. the empty-state prompt) now loads in the lazy fragment (#756).
    assert 'hx-get="/dialogs/fragments/list' in resp.text
    # The account selector swaps only #dialogs-list, so the Photo Loader link (an
    # account-scoped control outside the fragment) is kept in sync via JS (#878 review).
    assert 'id="photo-loader-link"' in resp.text
    assert 'id="account-select"' in resp.text
    assert "link.href = '/dialogs/photos'" in resp.text
    frag = await client.get("/dialogs/fragments/list")
    assert "загрузить список диалогов" in frag.text


@pytest.mark.anyio
async def test_dialogs_page_shows_dialogs(db, real_pool_harness_factory):
    # Under the queued-command model, the dialogs page reads from dialog_cache;
    # seed the cache so it has something to render.
    app, db, harness = await _build_dialogs_app(db, real_pool_harness_factory)
    await db.repos.dialog_cache.replace_dialogs("+1234567890", list(_FAKE_DIALOGS))
    async with make_auth_client(app) as client:
        # The dialog list renders in the lazy fragment (#756).
        resp = await client.get("/dialogs/fragments/list?phone=%2B1234567890")
    assert resp.status_code == 200
    assert "My Channel" in resp.text
    assert "My Group" in resp.text
    assert "Some User" in resp.text
    assert "My Bot" in resp.text
    # All 4 tabs present
    assert "tab-channels" in resp.text
    assert "tab-groups" in resp.text
    assert "tab-dms" in resp.text
    assert "tab-bots" in resp.text
    assert 'action="/dialogs/refresh"' in resp.text
    assert "Обновить диалоги" in resp.text
    assert "Показан сохранённый список диалогов" in resp.text
    # The "Удалить" button posts to /dialogs/delete (formaction) and carries its OWN
    # data-confirm warning about irreversibility — the form's data-confirm (leave) must
    # not be what the user sees when clicking delete (see app.js submitter-confirm fix).
    assert 'formaction="/dialogs/delete"' in resp.text
    assert "НАВСЕГДА удалить" in resp.text

    await app.state.collection_queue.shutdown()


def test_submit_confirm_reads_submitter_data_confirm():
    """The global submit-confirm handler must read data-confirm from the clicked
    submitter (e.submitter), not only from the form. A submitter button with a
    distinct data-confirm (e.g. the "Удалить" button posting via formaction) must
    show ITS own confirmation, not the form's — otherwise an irreversible delete
    shows the form's softer "leave" prompt."""
    from pathlib import Path

    app_js = (
        Path(__file__).resolve().parent.parent / "src" / "web" / "static" / "js" / "app.js"
    ).read_text(encoding="utf-8")
    assert "submitter.dataset.confirm" in app_js


@pytest.mark.anyio
async def test_dialogs_page_requires_auth(db, real_pool_harness_factory):
    app, db, harness = await _build_dialogs_app(
        db,
        real_pool_harness_factory,
        with_account=False,
    )

    async with make_auth_client(app, with_auth=False) as c:
        resp = await c.get("/dialogs/", follow_redirects=False)
    assert resp.status_code == 401

    await app.state.collection_queue.shutdown()


@pytest.mark.anyio
async def test_leave_channels_success():
    """All dialogs → True; resolve_entity_with_warm called with the right peer types."""
    from telethon.tl.types import PeerChannel, PeerUser

    from src.telegram.client_pool import ClientPool

    pool = MagicMock(spec=ClientPool)
    mock_client = MagicMock()
    mock_client.delete_dialog = AsyncMock()
    received_peers = []

    async def _resolve(session, phone, peer, *, operation, use_input_entity):
        received_peers.append(peer)
        return MagicMock()

    pool.resolve_entity_with_warm = _resolve
    pool.get_client_by_phone = AsyncMock(return_value=(mock_client, "+1234567890"))
    pool.release_client = AsyncMock()
    pool._db = MagicMock()
    pool._db.repos.dialog_cache.clear_dialogs = AsyncMock()
    pool.invalidate_dialogs_cache = MagicMock()

    dialogs = [(-100111, "channel"), (999, "dm")]
    with patch("src.telegram.pool_dialogs.asyncio.sleep", AsyncMock()):
        result = await ClientPool.leave_channels(pool, "+1234567890", dialogs)

    assert result == {-100111: True, 999: True}
    assert mock_client.delete_dialog.await_count == 2
    assert isinstance(received_peers[0], PeerChannel)
    assert received_peers[0].channel_id == 100111
    assert isinstance(received_peers[1], PeerUser)
    assert received_peers[1].user_id == 999
    pool.invalidate_dialogs_cache.assert_called_once_with("+1234567890")
    pool._db.repos.dialog_cache.clear_dialogs.assert_awaited_once_with("+1234567890")


@pytest.mark.anyio
async def test_leave_channels_partial_failure():
    """One remove_dialog raises RuntimeError → that id is False, others True."""
    from src.telegram.client_pool import ClientPool

    pool = MagicMock(spec=ClientPool)
    mock_client = MagicMock()

    call_count = 0

    async def _delete_dialog(entity):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise RuntimeError("some error")

    mock_client.delete_dialog = _delete_dialog

    pool.resolve_entity_with_warm = AsyncMock(return_value=MagicMock())
    pool.get_client_by_phone = AsyncMock(return_value=(mock_client, "+1234567890"))
    pool.release_client = AsyncMock()
    pool._db = MagicMock()
    pool._db.repos.dialog_cache.clear_dialogs = AsyncMock()
    pool.invalidate_dialogs_cache = MagicMock()

    dialogs = [(-100111, "channel"), (-100222, "supergroup")]
    with patch("src.telegram.pool_dialogs.asyncio.sleep", AsyncMock()):
        result = await ClientPool.leave_channels(pool, "+1234567890", dialogs)

    assert result[-100111] is False
    assert result[-100222] is True
    pool._db.repos.dialog_cache.clear_dialogs.assert_awaited_once_with("+1234567890")


@pytest.mark.anyio
async def test_leave_channels_transient_flood_retries():
    """Transient FloodWait(5s) → run_with_flood_wait_retry waits and retries; the
    element succeeds on the 2nd attempt and the remaining dialogs are still
    processed (no break). #1176."""
    from telethon.errors import FloodWaitError

    from src.telegram.client_pool import ClientPool

    pool = MagicMock(spec=ClientPool)
    mock_client = MagicMock()
    mock_client.delete_dialog = AsyncMock()

    attempts = {"n": 0}

    async def _resolve(session, phone, peer, *, operation, use_input_entity):
        attempts["n"] += 1
        # First attempt of the FIRST dialog floods (transient 5s); retries succeed.
        if attempts["n"] == 1:
            err = FloodWaitError(request=None)
            err.seconds = 5
            raise err
        return MagicMock()

    pool.resolve_entity_with_warm = _resolve
    pool.get_client_by_phone = AsyncMock(return_value=(mock_client, "+1234567890"))
    pool.release_client = AsyncMock()
    pool.report_flood = AsyncMock()
    pool._db = MagicMock()
    pool._db.repos.dialog_cache.clear_dialogs = AsyncMock()
    pool.invalidate_dialogs_cache = MagicMock()

    dialogs = [(-100111, "channel"), (-100222, "supergroup")]
    with (
        patch("telethon_floodgate.flood_wait.asyncio.sleep", AsyncMock()),
        patch("src.telegram.pool_dialogs.asyncio.sleep", AsyncMock()),
    ):
        result = await ClientPool.leave_channels(pool, "+1234567890", dialogs)

    # The flooded dialog was retried and succeeded; the 2nd dialog also processed.
    assert result == {-100111: True, -100222: True}
    assert attempts["n"] >= 2  # retry happened
    assert mock_client.delete_dialog.await_count == 2  # both dialogs removed
    pool.report_flood.assert_awaited_once_with("+1234567890", 5)


@pytest.mark.anyio
async def test_leave_channels_cold_resolve_warms_then_succeeds():
    """Cold resolve (ValueError on 1st resolve_input_entity) → warm_dialog_cache
    invoked → resolve passes on the 2nd attempt → element True. #1176.

    Exercises the REAL resolve_entity_with_warm (wired onto the pool) so the
    warm-then-retry path is integration-tested, not just the delegation.
    """
    from src.telegram.client_pool import ClientPool
    from src.telegram.pool_dialogs import DialogsMixin

    pool = MagicMock(spec=ClientPool)
    # Wire the real warm-then-retry resolver; its only `self` deps are mocked below.
    pool.resolve_entity_with_warm = DialogsMixin.resolve_entity_with_warm.__get__(pool, ClientPool)
    pool._is_live_username_peer = lambda peer: False
    pool.mark_dialogs_fetched = MagicMock()
    pool.report_flood = AsyncMock()

    mock_client = MagicMock()
    resolve_calls = {"n": 0}

    async def _get_input_entity(peer):
        resolve_calls["n"] += 1
        if resolve_calls["n"] == 1:
            raise ValueError("cold entity cache miss")
        return SimpleNamespace(id=100111)

    mock_client.get_input_entity = _get_input_entity
    mock_client.get_dialogs = AsyncMock(return_value=[])  # warm_dialog_cache
    mock_client.delete_dialog = AsyncMock()

    pool.get_client_by_phone = AsyncMock(return_value=(mock_client, "+1234567890"))
    pool.release_client = AsyncMock()
    pool._db = MagicMock()
    pool._db.repos.dialog_cache.clear_dialogs = AsyncMock()
    pool.invalidate_dialogs_cache = MagicMock()

    dialogs = [(-100111, "channel")]
    with patch("src.telegram.pool_dialogs.asyncio.sleep", AsyncMock()):
        result = await ClientPool.leave_channels(pool, "+1234567890", dialogs)

    assert result == {-100111: True}
    # resolve_input_entity called twice: 1st (cold ValueError) → warm → 2nd (ok).
    assert resolve_calls["n"] == 2
    mock_client.get_dialogs.assert_awaited_once()  # warm happened
    mock_client.delete_dialog.assert_awaited_once()  # remove_dialog after warm


@pytest.mark.anyio
async def test_leave_channels_blocking_flood_defers_and_continues():
    """Blocking FloodWait (>60s) → this dialog is deferred (False), but the loop
    does NOT break: remaining dialogs are still processed. #1176 (no-break)."""
    from telethon.errors import FloodWaitError

    from src.telegram.client_pool import ClientPool

    pool = MagicMock(spec=ClientPool)
    mock_client = MagicMock()
    mock_client.delete_dialog = AsyncMock()

    async def _resolve(session, phone, peer, *, operation, use_input_entity):
        if operation.endswith(":-100111"):
            err = FloodWaitError(request=None)
            err.seconds = 3600  # blocking (>60s)
            raise err
        return MagicMock()  # 2nd dialog resolves fine

    pool.resolve_entity_with_warm = _resolve
    pool.get_client_by_phone = AsyncMock(return_value=(mock_client, "+1234567890"))
    pool.release_client = AsyncMock()
    pool.report_flood = AsyncMock()
    pool._db = MagicMock()
    pool._db.repos.dialog_cache.clear_dialogs = AsyncMock()
    pool.invalidate_dialogs_cache = MagicMock()

    dialogs = [(-100111, "channel"), (-100222, "supergroup")]
    with patch("src.telegram.pool_dialogs.asyncio.sleep", AsyncMock()):
        result = await ClientPool.leave_channels(pool, "+1234567890", dialogs)

    assert result == {-100111: False, -100222: True}
    pool.report_flood.assert_awaited_once_with("+1234567890", 3600)
    # 2nd dialog's removal happened despite the 1st's blocking flood (no break).
    assert mock_client.delete_dialog.await_count == 1


@pytest.mark.anyio
async def test_delete_dialogs_dispatches_request_per_type():
    """delete_dialogs picks the right TL request per dialog type:
    channel/supergroup → DeleteChannelRequest, legacy group → DeleteChatRequest,
    dm → delete_dialog (history clear)."""
    from telethon.tl.functions.channels import DeleteChannelRequest
    from telethon.tl.functions.messages import DeleteChatRequest

    from src.telegram.client_pool import ClientPool

    pool = MagicMock(spec=ClientPool)
    invoked_requests = []

    async def _invoke(request):
        invoked_requests.append(request)

    # The session wrapper calls ``self._client(request)`` to invoke a TL request,
    # so the client itself must be awaitable-on-call → side_effect returns a coroutine.
    mock_client = MagicMock(side_effect=_invoke)
    mock_client.delete_dialog = AsyncMock()

    pool.resolve_entity_with_warm = AsyncMock(return_value=MagicMock())
    pool.get_client_by_phone = AsyncMock(return_value=(mock_client, "+1234567890"))
    pool.release_client = AsyncMock()
    pool._db = MagicMock()
    pool._db.repos.dialog_cache.clear_dialogs = AsyncMock()
    pool.invalidate_dialogs_cache = MagicMock()

    dialogs = [(-100111, "channel"), (-100222, "supergroup"), (333, "group"), (999, "dm")]
    with patch("src.telegram.pool_dialogs.asyncio.sleep", AsyncMock()):
        result = await ClientPool.delete_dialogs(pool, "+1234567890", dialogs)

    assert result == {-100111: True, -100222: True, 333: True, 999: True}
    # channel + supergroup → 2× DeleteChannelRequest; group → 1× DeleteChatRequest; dm → delete_dialog
    channel_reqs = [r for r in invoked_requests if isinstance(r, DeleteChannelRequest)]
    chat_reqs = [r for r in invoked_requests if isinstance(r, DeleteChatRequest)]
    assert len(channel_reqs) == 2
    assert len(chat_reqs) == 1
    assert chat_reqs[0].chat_id == 333
    mock_client.delete_dialog.assert_awaited_once()
    pool.invalidate_dialogs_cache.assert_called_once_with("+1234567890")
    pool._db.repos.dialog_cache.clear_dialogs.assert_awaited_once_with("+1234567890")


@pytest.mark.anyio
async def test_delete_dialogs_group_skips_resolve_entity():
    """Deleting a legacy group must NOT require resolving the entity: DeleteChatRequest
    takes a bare chat_id. A group whose entity can't be resolved (migrated to supergroup,
    stale entity cache) must still be deleted, not fail on a needless resolve_entity."""
    from telethon.tl.functions.messages import DeleteChatRequest

    from src.telegram.client_pool import ClientPool

    pool = MagicMock(spec=ClientPool)
    invoked_requests = []

    async def _invoke(request):
        invoked_requests.append(request)

    mock_client = MagicMock(side_effect=_invoke)
    mock_client.delete_dialog = AsyncMock()

    # Group deletion must never resolve the entity (#1176 warm-resolve change).
    resolve_mock = pool.resolve_entity_with_warm = AsyncMock(
        side_effect=AssertionError("group deletion must not resolve entity")
    )
    pool.get_client_by_phone = AsyncMock(return_value=(mock_client, "+1234567890"))
    pool.release_client = AsyncMock()
    pool._db = MagicMock()
    pool._db.repos.dialog_cache.clear_dialogs = AsyncMock()
    pool.invalidate_dialogs_cache = MagicMock()

    dialogs = [(333, "group")]
    with patch("src.telegram.pool_dialogs.asyncio.sleep", AsyncMock()):
        result = await ClientPool.delete_dialogs(pool, "+1234567890", dialogs)

    assert result == {333: True}
    chat_reqs = [r for r in invoked_requests if isinstance(r, DeleteChatRequest)]
    assert len(chat_reqs) == 1
    assert chat_reqs[0].chat_id == 333
    resolve_mock.assert_not_awaited()


@pytest.mark.anyio
async def test_delete_dialogs_transient_flood_retries():
    """Transient FloodWait(5s) during delete → retried in place, dialog succeeds;
    remaining dialogs still processed (no break). #1176."""
    from telethon.errors import FloodWaitError

    from src.telegram.client_pool import ClientPool

    pool = MagicMock(spec=ClientPool)
    invoked_requests = []

    async def _invoke(request):
        invoked_requests.append(request)

    mock_client = MagicMock(side_effect=_invoke)
    delete_calls = {"n": 0}

    async def _delete_dialog(entity):
        delete_calls["n"] += 1
        if delete_calls["n"] == 1:
            err = FloodWaitError(request=None)
            err.seconds = 5
            raise err

    mock_client.delete_dialog = _delete_dialog

    pool.resolve_entity_with_warm = AsyncMock(return_value=MagicMock())
    pool.get_client_by_phone = AsyncMock(return_value=(mock_client, "+1234567890"))
    pool.release_client = AsyncMock()
    pool.report_flood = AsyncMock()
    pool._db = MagicMock()
    pool._db.repos.dialog_cache.clear_dialogs = AsyncMock()
    pool.invalidate_dialogs_cache = MagicMock()

    dialogs = [(999, "dm"), (888, "bot")]
    with (
        patch("telethon_floodgate.flood_wait.asyncio.sleep", AsyncMock()),
        patch("src.telegram.pool_dialogs.asyncio.sleep", AsyncMock()),
    ):
        result = await ClientPool.delete_dialogs(pool, "+1234567890", dialogs)

    assert result == {999: True, 888: True}
    assert delete_calls["n"] >= 2  # 1st dm retried after transient flood
    pool.report_flood.assert_awaited_once_with("+1234567890", 5)


@pytest.mark.anyio
async def test_leave_dialogs_post(client):
    """POST /dialogs/leave enqueues a dialogs.leave command."""
    resp = await client.post(
        "/dialogs/leave",
        data={"phone": "+1234567890", "channel_ids": ["-100111:channel", "-100222:supergroup"]},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "command_id=" in resp.headers["location"]
    assert "phone=" in resp.headers["location"]


@pytest.mark.anyio
async def test_refresh_dialogs_post_enqueues_command(db, real_pool_harness_factory):
    """POST /dialogs/refresh enqueues a dialogs.refresh command (queued model)."""
    app, db, harness = await _build_dialogs_app(db, real_pool_harness_factory)

    async with make_auth_client(app) as c:
        resp = await c.post(
            "/dialogs/refresh",
            data={"phone": "+1234567890"},
            follow_redirects=False,
        )

    assert resp.status_code == 303
    assert "command_id=" in resp.headers["location"]
    commands = await db.repos.telegram_commands.list_commands(limit=1)
    assert len(commands) == 1
    assert commands[0].command_type == "dialogs.refresh"
    assert commands[0].payload.get("phone") == "+1234567890"

    await app.state.collection_queue.shutdown()


@pytest.mark.anyio
async def test_leave_dialogs_flash_message(client):
    """GET with left/failed params shows flash banner (now in the list fragment, #756)."""
    resp = await client.get("/dialogs/fragments/list?phone=%2B1234567890&left=2&failed=1")
    assert resp.status_code == 200
    assert "Отписались" in resp.text
    assert "<strong>2</strong>" in resp.text
    assert "<strong>1</strong>" in resp.text


@pytest.mark.anyio
async def test_get_my_dialogs_enriches_already_added(db):
    """get_my_dialogs() marks dialogs already in the channel DB.

    Under the queued-command model, get_my_dialogs reads from dialog_cache
    when refresh=False; the pool is not consulted. Seed the cache directly.
    """
    from src.models import Channel

    await db.add_channel(
        Channel(
            channel_id=-100111,
            title="My Channel",
            username="mychan",
            channel_type="channel",
            is_active=True,
        )
    )
    await db.repos.dialog_cache.replace_dialogs("+1234567890", list(_FAKE_DIALOGS))

    pool = MagicMock()
    pool.get_dialogs_for_phone = AsyncMock(return_value=list(_FAKE_DIALOGS))
    queue = MagicMock()

    service = ChannelService(db, pool, queue)
    dialogs = await service.get_my_dialogs("+1234567890")

    # pool must NOT be called in the read path (queued model)
    pool.get_dialogs_for_phone.assert_not_called()
    by_id = {d["channel_id"]: d for d in dialogs}
    assert by_id[-100111]["already_added"] is True
    assert by_id[-100222]["already_added"] is False
    assert by_id[999]["already_added"] is False
    assert by_id[888]["already_added"] is False


@pytest.mark.anyio
async def test_get_my_dialogs_passes_refresh_flag(db):
    pool = MagicMock()
    pool.get_dialogs_for_phone = AsyncMock(return_value=list(_FAKE_DIALOGS))
    queue = MagicMock()

    service = ChannelService(db, pool, queue)
    await service.get_my_dialogs("+1234567890", refresh=True)

    pool.get_dialogs_for_phone.assert_awaited_once_with(
        "+1234567890",
        include_dm=True,
        mode="full",
        refresh=True,
    )


@pytest.mark.anyio
async def test_get_my_dialogs_bot_type():
    """entity with bot=True → channel_type='bot'."""
    from src.telegram.client_pool import ClientPool

    pool = MagicMock(spec=ClientPool)

    bot_entity = MagicMock()
    bot_entity.id = 777
    bot_entity.username = "testbot"
    bot_entity.bot = True

    bot_dialog = MagicMock()
    bot_dialog.entity = bot_entity
    bot_dialog.title = "Test Bot"
    bot_dialog.is_channel = False
    bot_dialog.is_group = False

    async def _fake_iter_dialogs():
        yield bot_dialog

    mock_client = MagicMock()
    mock_client.iter_dialogs.return_value = _fake_iter_dialogs()

    pool.get_client_by_phone = AsyncMock(return_value=(mock_client, "+1234567890"))
    pool.release_client = AsyncMock()
    pool._db = MagicMock()
    pool._db.repos.dialog_cache.list_dialogs = AsyncMock(return_value=[])
    pool._db.repos.dialog_cache.replace_dialogs = AsyncMock()
    _bind_dialog_cache_methods(pool)

    # Call the real method
    result = await ClientPool.get_dialogs_for_phone(
        pool,
        "+1234567890",
        include_dm=True,
        mode="full",
    )

    assert len(result) == 1
    assert result[0]["channel_type"] == "bot"
    assert result[0]["channel_id"] == 777


@pytest.mark.anyio
async def test_get_dialogs_for_phone_partial_on_timeout():
    """When iter_dialogs times out, partial accumulated results are returned."""
    from src.telegram.client_pool import ClientPool

    pool = MagicMock(spec=ClientPool)

    async def _slow_iter_dialogs():
        # Yield one dialog then hang indefinitely
        chan_entity = MagicMock()
        chan_entity.id = -100999
        chan_entity.username = "fastchan"
        chan_entity.megagroup = False
        chan_entity.broadcast = True
        chan_entity.gigagroup = False
        chan_entity.forum = False
        chan_entity.scam = False
        chan_entity.fake = False
        chan_entity.restricted = False

        dialog = MagicMock()
        dialog.entity = chan_entity
        dialog.title = "Fast Channel"
        dialog.is_channel = True
        dialog.is_group = False
        yield dialog

        await asyncio.sleep(120)

    mock_client = MagicMock()
    mock_client.iter_dialogs.return_value = _slow_iter_dialogs()

    pool.get_client_by_phone = AsyncMock(return_value=(mock_client, "+1234567890"))
    pool.release_client = AsyncMock()
    pool._classify_entity = MagicMock(return_value=("channel", False))
    pool._db = MagicMock()
    pool._db.repos.dialog_cache.list_dialogs = AsyncMock(return_value=[])
    pool._db.repos.dialog_cache.replace_dialogs = AsyncMock()
    pool._db.repos.dialog_cache.upsert_dialogs = AsyncMock()
    _bind_dialog_cache_methods(pool)

    # Patch wait_for to use a tiny timeout so we don't wait 60 s in tests
    original_wait_for = asyncio.wait_for

    async def fast_wait_for(coro, timeout):
        return await original_wait_for(coro, timeout=min(timeout, 0.05))

    with patch("src.telegram.pool_dialogs.asyncio.wait_for", fast_wait_for):
        result = await ClientPool.get_dialogs_for_phone(pool, "+1234567890")

    assert len(result) == 1
    assert result[0]["channel_id"] == -100999


@pytest.mark.anyio
async def test_get_dialogs_for_phone_transient_flood_is_retried_not_propagated(db):
    """A short flood must be waited out and the sweep completed (#1359).

    Before this change the same 23s flood propagated to the caller and an outer
    handler wrote the partial progress. Now run_with_flood_wait_retry waits the
    short flood out and re-runs the pass from the cursor, so the sweep finishes
    normally (partial=False) instead of surfacing an error.

    Note the retry happens INSIDE one pass, not by starting another one — the
    multi-pass loop is for floods/timeouts that survive the retry. A BLOCKING
    flood still ends the sweep partial: see
    test_sweep_keeps_progress_on_blocking_flood.
    """
    from datetime import datetime, timezone

    from src.telegram.client_pool import ClientPool
    from src.telegram.flood_wait import FloodWaitInfo, HandledFloodWaitError

    pool = _sweep_pool(db)
    partial_dialog = _make_channel_dialog(-100999, title="Partial Channel", message_id=7)
    rest = _make_channel_dialog(-100888, title="Rest", message_id=8)

    async def _floods_once():
        yield partial_dialog
        raise HandledFloodWaitError(
            FloodWaitInfo(
                operation="get_dialogs_for_phone",
                phone="+1234567890",
                wait_seconds=23,
                next_available_at_utc=datetime.now(timezone.utc),
                detail="Flood wait 23s",
            )
        )

    mock_client = MagicMock()
    mock_client.iter_dialogs.side_effect = [_floods_once(), _dialog_stream([rest])]
    pool.get_client_by_phone = AsyncMock(return_value=(mock_client, "+1234567890"))

    slept: list[int] = []

    async def _record_sleep(info, *, logger_=None):
        # Don't actually wait out the 23s flood — record that we would have.
        slept.append(info.wait_seconds)

    with patch("telethon_floodgate.flood_wait.sleep_for_handled_flood_wait", _record_sleep):
        result = await ClientPool._fetch_dialogs_for_phone(
            pool, "+1234567890", True, "full", "full"
        )

    # The short flood was waited out (not propagated, not treated as blocking).
    assert slept == [23]

    # Decisive: the flood was waited out and the pass re-run from the cursor,
    # so the sweep completed rather than ending partial at the flood.
    assert mock_client.iter_dialogs.call_count == 2
    assert result.partial is False
    # The flood did not reach the caller, and both passes' dialogs are present.
    assert [row["channel_id"] for row in result] == [-100999, -100888]
    cached = await db.repos.dialog_cache.list_dialogs("+1234567890")
    assert -100999 in {row["channel_id"] for row in cached}


@pytest.mark.anyio
async def test_dialogs_page_without_phone_does_not_fetch_dialogs(
    db,
    real_pool_harness_factory,
):
    app, db, harness = await _build_dialogs_app(db, real_pool_harness_factory)

    async with make_auth_client(app) as c:
        resp = await c.get("/dialogs/")

    assert resp.status_code == 200
    assert "Выберите аккаунт" in resp.text
    assert 'href="/dialogs/photos"' in resp.text
    assert len(harness.telethon_cli_spy.created) == 1

    await app.state.collection_queue.shutdown()


@pytest.mark.anyio
async def test_dialogs_page_without_accounts_shows_disabled_photo_loader(
    db,
    real_pool_harness_factory,
):
    app, db, harness = await _build_dialogs_app(
        db,
        real_pool_harness_factory,
        with_account=False,
    )

    async with make_auth_client(app) as c:
        resp = await c.get("/dialogs/")

    assert resp.status_code == 200
    assert "Сначала добавьте Telegram-аккаунт в настройках." in resp.text
    assert 'href="/dialogs/photos"' not in resp.text
    assert 'aria-disabled="true"' in resp.text

    await app.state.collection_queue.shutdown()


@pytest.mark.anyio
async def test_get_dialogs_for_phone_uses_manual_cache():
    from src.telegram.client_pool import ClientPool

    pool = MagicMock(spec=ClientPool)
    mock_client = MagicMock()
    dialog = _make_channel_dialog(-100123)

    async def _fake_iter_dialogs():
        yield dialog

    mock_client.iter_dialogs.return_value = _fake_iter_dialogs()
    pool.get_client_by_phone = AsyncMock(return_value=(mock_client, "+1234567890"))
    pool.release_client = AsyncMock()
    pool._classify_entity = MagicMock(return_value=("channel", False))
    pool._db = MagicMock()
    pool._db.repos.dialog_cache.list_dialogs = AsyncMock(return_value=[])
    pool._db.repos.dialog_cache.replace_dialogs = AsyncMock()
    _bind_dialog_cache_methods(pool)

    result1 = await ClientPool.get_dialogs_for_phone(pool, "+1234567890")
    result2 = await ClientPool.get_dialogs_for_phone(pool, "+1234567890")

    assert result1 == result2
    assert mock_client.iter_dialogs.call_count == 1


@pytest.mark.anyio
async def test_get_dialogs_for_phone_refresh_bypasses_cache():
    from src.telegram.client_pool import ClientPool

    pool = MagicMock(spec=ClientPool)
    mock_client = MagicMock()
    dialog = _make_channel_dialog(-100123)

    async def _fake_iter_dialogs():
        yield dialog

    mock_client.iter_dialogs.side_effect = [_fake_iter_dialogs(), _fake_iter_dialogs()]
    pool.get_client_by_phone = AsyncMock(return_value=(mock_client, "+1234567890"))
    pool.release_client = AsyncMock()
    pool._classify_entity = MagicMock(return_value=("channel", False))
    pool._db = MagicMock()
    pool._db.repos.dialog_cache.list_dialogs = AsyncMock(return_value=[])
    pool._db.repos.dialog_cache.replace_dialogs = AsyncMock()
    _bind_dialog_cache_methods(pool)

    await ClientPool.get_dialogs_for_phone(pool, "+1234567890")
    await ClientPool.get_dialogs_for_phone(pool, "+1234567890", refresh=True)

    assert mock_client.iter_dialogs.call_count == 2


@pytest.mark.anyio
async def test_get_dialogs_for_phone_refetches_after_ttl_expiry():
    from src.telegram.client_pool import ClientPool

    pool = MagicMock(spec=ClientPool)
    mock_client = MagicMock()
    dialog = _make_channel_dialog(-100123)

    async def _fake_iter_dialogs():
        yield dialog

    mock_client.iter_dialogs.side_effect = [_fake_iter_dialogs(), _fake_iter_dialogs()]
    pool.get_client_by_phone = AsyncMock(return_value=(mock_client, "+1234567890"))
    pool.release_client = AsyncMock()
    pool._classify_entity = MagicMock(return_value=("channel", False))
    pool._db = MagicMock()
    pool._db.repos.dialog_cache.list_dialogs = AsyncMock(return_value=[])
    pool._db.repos.dialog_cache.replace_dialogs = AsyncMock()
    _bind_dialog_cache_methods(pool)
    pool._dialogs_cache_ttl_sec = -1.0

    await ClientPool.get_dialogs_for_phone(pool, "+1234567890")
    await ClientPool.get_dialogs_for_phone(pool, "+1234567890")

    assert mock_client.iter_dialogs.call_count == 2


@pytest.mark.anyio
async def test_get_dialogs_for_phone_uses_db_cache_across_pool_instances(db):
    from src.telegram.client_pool import ClientPool

    await db.repos.dialog_cache.replace_dialogs("+1234567890", list(_FAKE_DIALOGS))

    pool = MagicMock(spec=ClientPool)
    pool._db = db
    pool.get_client_by_phone = AsyncMock()
    pool.release_client = AsyncMock()
    _bind_dialog_cache_methods(pool)

    dialogs = await ClientPool.get_dialogs_for_phone(
        pool,
        "+1234567890",
        include_dm=True,
        mode="full",
    )

    assert _strip_extra_dialog_fields(dialogs) == _strip_extra_dialog_fields(_FAKE_DIALOGS)
    pool.get_client_by_phone.assert_not_awaited()


@pytest.mark.anyio
async def test_get_dialogs_for_phone_refresh_replaces_db_cache(db):
    from src.telegram.client_pool import ClientPool

    await db.repos.dialog_cache.replace_dialogs(
        "+1234567890",
        [dict(_FAKE_DIALOGS[0], title="Stale Title")],
    )

    pool = MagicMock(spec=ClientPool)
    mock_client = MagicMock()
    dialog = _make_channel_dialog(-100111, title="Fresh Title", username="freshchan")

    async def _fake_iter_dialogs():
        yield dialog

    mock_client.iter_dialogs.return_value = _fake_iter_dialogs()
    pool._db = db
    pool.get_client_by_phone = AsyncMock(return_value=(mock_client, "+1234567890"))
    pool.release_client = AsyncMock()
    pool._classify_entity = MagicMock(return_value=("channel", False))
    _bind_dialog_cache_methods(pool)

    dialogs = await ClientPool.get_dialogs_for_phone(
        pool,
        "+1234567890",
        include_dm=True,
        mode="full",
        refresh=True,
    )

    assert dialogs[0]["title"] == "Fresh Title"
    cached = await db.repos.dialog_cache.list_dialogs("+1234567890")
    assert cached[0]["title"] == "Fresh Title"


@pytest.mark.anyio
async def test_get_dialogs_for_phone_failed_refresh_keeps_existing_db_cache(db):
    from src.telegram.client_pool import ClientPool

    await db.repos.dialog_cache.replace_dialogs("+1234567890", list(_FAKE_DIALOGS))

    pool = MagicMock(spec=ClientPool)
    pool._db = db
    pool.get_client_by_phone = AsyncMock(return_value=None)
    pool.release_client = AsyncMock()
    _bind_dialog_cache_methods(pool)

    dialogs = await ClientPool.get_dialogs_for_phone(
        pool,
        "+1234567890",
        include_dm=True,
        mode="full",
        refresh=True,
    )

    assert dialogs
    assert all(dialog.get("_degraded") for dialog in dialogs)
    cached = await db.repos.dialog_cache.list_dialogs("+1234567890")
    assert _strip_extra_dialog_fields(cached) == _strip_extra_dialog_fields(_FAKE_DIALOGS)


@pytest.mark.anyio
async def test_get_dialogs_for_phone_flood_stale_cache_is_flagged_partial(db):
    """A FloodWait that falls back to the stale cache must be flagged partial.

    #1350: the flood path returned the stale snapshot as a plain list, so callers
    printed "Dialogs refreshed: N total" over a cache that was never updated —
    the exact silent-success the partial flag exists to prevent.
    """
    from src.telegram.client_pool import ClientPool

    await db.repos.dialog_cache.replace_dialogs("+1234567890", list(_FAKE_DIALOGS))

    pool = MagicMock(spec=ClientPool)
    pool._db = db
    pool.get_client_by_phone = AsyncMock(side_effect=RuntimeError("boom"))
    pool.release_client = AsyncMock()
    _bind_dialog_cache_methods(pool)

    result = await ClientPool.get_dialogs_for_phone(
        pool,
        "+1234567890",
        include_dm=True,
        mode="full",
        refresh=True,
    )

    assert result.partial is True
    # The stale snapshot is still handed back — degraded, not empty.
    assert _strip_extra_dialog_fields(list(result)) == _strip_extra_dialog_fields(_FAKE_DIALOGS)
    cached = await db.repos.dialog_cache.list_dialogs("+1234567890")
    assert _strip_extra_dialog_fields(cached) == _strip_extra_dialog_fields(_FAKE_DIALOGS)


@pytest.mark.anyio
async def test_get_dialogs_for_phone_empty_partial_uses_stale_cache(db):
    """A flood before the first dialog must not turn a populated cache empty (#1379)."""
    from src.telegram.client_pool import ClientPool
    from src.telegram.flood_wait import FloodWaitInfo, HandledFloodWaitError

    phone = "+1234567890"
    await db.repos.dialog_cache.replace_dialogs(phone, list(_FAKE_DIALOGS))

    pool = _sweep_pool(db, phone)

    async def _floods_before_first_dialog():
        if False:  # Keep this an async generator while yielding no dialogs.
            yield None
        raise HandledFloodWaitError(
            FloodWaitInfo(
                operation="get_dialogs_for_phone",
                phone=phone,
                wait_seconds=3600,
                next_available_at_utc=datetime(2026, 1, 1, tzinfo=timezone.utc),
                detail="blocking",
            )
        )

    mock_client = MagicMock()
    mock_client.fetch_me = AsyncMock(return_value=SimpleNamespace(id=1))
    mock_client.iter_dialogs.return_value = _floods_before_first_dialog()
    pool.get_client_by_phone = AsyncMock(return_value=(mock_client, phone))

    result = await ClientPool.get_dialogs_for_phone(
        pool,
        phone,
        include_dm=True,
        mode="full",
        refresh=True,
    )

    assert result.partial is True
    assert _strip_extra_dialog_fields(list(result)) == _strip_extra_dialog_fields(_FAKE_DIALOGS)
    assert all(dialog.get("_degraded") for dialog in result)


@pytest.mark.anyio
async def test_partial_sweep_over_a_still_fresh_snapshot_is_not_fresh(db):
    """A partial walk must age the snapshot even when it was just refreshed.

    Freshness is MAX(cached_at) over the whole phone. If a partial walk inherits
    a still-fresh predecessor, the truncated union — stale rows plus whatever the
    interrupted pass never reached — is served as a complete current snapshot for
    the rest of the TTL. The stale-predecessor case alone does not cover this:
    the dangerous one is a manual refresh started while the cache is still fresh.
    """
    from src.telegram.client_pool import ClientPool

    phone = "+79995550000"
    # A snapshot written just now — comfortably inside the TTL.
    await db.repos.dialog_cache.replace_dialogs(
        phone,
        [
            {"channel_id": 1, "channel_type": "channel", "title": "one"},
            {"channel_id": 2, "channel_type": "channel", "title": "two"},
        ],
    )
    fresh_at = await db.repos.dialog_cache.get_cached_at(phone)
    assert (datetime.now(timezone.utc) - fresh_at).total_seconds() < 60

    # A refresh starts and is cut short after reaching one dialog.
    await db.repos.dialog_cache.upsert_dialogs(
        phone, [{"channel_id": 3, "channel_type": "channel", "title": "three"}]
    )

    pool = MagicMock(spec=ClientPool)
    pool._db = db
    pool._dialogs_db_cache_ttl_sec = 3600.0
    pool._dialogs_cache = {}
    _bind_dialog_cache_methods(pool)

    # Must refuse the cache and force a fresh fetch, not serve the union.
    assert await ClientPool._get_db_cached_dialogs(pool, phone, "full") is None


@pytest.mark.anyio
async def test_partial_sweep_is_not_served_as_a_fresh_cache(db):
    """A resumed fragment must not read back as a fresh snapshot (#1359).

    If a partial walk stamped the touched rows with a current timestamp, the
    freshness check (MAX(cached_at) vs the TTL) would hand that truncated set
    to every later non-refresh caller as an ordinary current cache. That is a
    worse failure than the frozen cache this PR fixes: incomplete AND looking
    current. upsert_dialogs therefore keeps the PREVIOUS cached_at
    (#1360/#1347), so the fragment stays stale and is re-fetched.
    """
    from src.telegram.client_pool import ClientPool

    phone = "+1234567890"
    # An OLD complete snapshot, well past the TTL.
    await db.execute_write(
        "INSERT INTO dialog_cache (phone, dialog_id, title, channel_type, cached_at) "
        "VALUES (?, ?, ?, ?, ?)",
        (phone, 1, "one", "channel", "2026-01-01T00:00:00+00:00"),
    )
    # A resumed sweep reaches only part of the picture.
    await db.repos.dialog_cache.upsert_dialogs(
        phone, [{"channel_id": 2, "channel_type": "channel", "title": "two"}]
    )

    # The fragment must NOT have made the snapshot look current: an incomplete
    # snapshot is aged unconditionally, so it can never read as fresh.
    cached_at = await db.repos.dialog_cache.get_cached_at(phone)
    assert (datetime.now(timezone.utc) - cached_at).total_seconds() > 3600, (
        f"partial sweep left the snapshot looking current: {cached_at}"
    )

    pool = MagicMock(spec=ClientPool)
    pool._db = db
    pool._dialogs_db_cache_ttl_sec = 3600.0
    pool._dialogs_cache = {}
    _bind_dialog_cache_methods(pool)

    # Stale beyond the TTL -> refuse the cache and force a fresh fetch.
    assert await ClientPool._get_db_cached_dialogs(pool, phone, "full") is None


# --- #1359: resumable dialog sweep -------------------------------------------


def _sweep_pool(db, phone: str = "+1234567890"):
    """MagicMock pool wired for _fetch_dialogs_for_phone sweeps.

    The pool carries a REAL rate-limit gate. ``_rate_limit_gate`` is set in
    ``ClientPool.__init__``, so it is not part of ``spec=ClientPool`` and a bare
    MagicMock silently answers None — which makes ``_reserve_gate_slot`` return
    early and every sweep test run in a world where the gate does not exist.
    That blind spot is exactly why the gate refusing pass 2 went unnoticed
    (#1359): the multi-pass tests passed while the feature was inert in
    production. Wiring the real gate keeps these tests honest.
    """
    from src.telegram.client_pool import ClientPool
    from src.telegram.rate_limit_gate import TelegramRateLimitGate

    pool = MagicMock(spec=ClientPool)
    pool._db = db
    pool.release_client = AsyncMock()
    pool._classify_entity = MagicMock(return_value=("channel", False))
    pool._rate_limit_gate = TelegramRateLimitGate()
    pool._flood_breaker = None
    _bind_dialog_cache_methods(pool)
    return pool


def _dialog_stream(dialogs):
    async def _iter():
        for dialog in dialogs:
            yield dialog

    return _iter()


async def test_stream_dialogs_forwards_offset_kwargs():
    """Offsets reach Telethon; a plain sweep still calls it with none (#1359)."""
    from src.telegram.backends import TelegramTransportSession

    client = MagicMock()
    client.iter_dialogs.return_value = _dialog_stream([])
    session = MagicMock(spec=TelegramTransportSession)
    session._client = client
    session._stream = MagicMock(side_effect=lambda _op, it: it)

    TelegramTransportSession.stream_dialogs(session)
    assert client.iter_dialogs.call_args.kwargs == {}

    client.iter_dialogs.return_value = _dialog_stream([])
    TelegramTransportSession.stream_dialogs(session, offset_id=42, offset_peer="peer")
    assert client.iter_dialogs.call_args.kwargs == {"offset_id": 42, "offset_peer": "peer"}


def test_advance_cursor_mirrors_telethon_split_offsets():
    """offset_peer advances per dialog; offset_id/date only on message-bearing ones.

    Telethon's `_DialogsIter._load_next_chunk` takes offset_id/offset_date from
    the last dialog that HAS a message, but offset_peer from `buffer[-1]` — the
    last dialog regardless. Advancing all three together left offset_peer behind
    whenever a chunk ended with message-less dialogs, so the resumed request
    replayed a seen tail, made no progress, and stopped the sweep early (#1359).
    """
    from src.telegram.pool_dialogs import _advance_cursor, _DialogCursor

    cursor = _DialogCursor()

    # A bare MagicMock has no usable input_entity anchor at all.
    _advance_cursor(cursor, MagicMock())
    assert cursor.is_start

    _advance_cursor(cursor, _make_channel_dialog(-1002, message_id=77))
    assert cursor.offset_id == 77
    assert cursor.offset_peer.channel_id == 1002
    assert not cursor.is_start

    # A trailing message-less dialog still moves the peer (Telethon's buffer[-1])
    # while keeping the last real message id/date.
    _advance_cursor(cursor, _make_channel_dialog(-1003, with_message=False))
    assert cursor.offset_peer.channel_id == 1003
    assert cursor.offset_id == 77


@pytest.mark.anyio
async def test_sweep_caps_each_pass_and_retry_to_total_budget(db):
    """The total sweep deadline also bounds a pass's retry settings (#1379)."""
    from src.telegram.client_pool import ClientPool

    pool = _sweep_pool(db)
    mock_client = MagicMock()
    mock_client.iter_dialogs.return_value = _dialog_stream([])
    pool.get_client_by_phone = AsyncMock(return_value=(mock_client, "+1234567890"))
    observed: dict[str, float | int] = {}

    async def _fake_retry(_factory, **kwargs):
        observed.update(
            timeout=kwargs["timeout"],
            transient_wait_budget_sec=kwargs["transient_wait_budget_sec"],
        )

    with (
        patch("src.telegram.pool_dialogs.DIALOG_FETCH_TOTAL_BUDGET_SEC", 1.0),
        patch("src.telegram.pool_dialogs.time.perf_counter", side_effect=[100.0, 100.5, 100.5]),
        patch("src.telegram.pool_dialogs.run_with_flood_wait_retry", new=_fake_retry),
    ):
        result = await ClientPool._fetch_dialogs_for_phone(
            pool, "+1234567890", True, "full", "full"
        )

    assert result.partial is False
    assert observed == {"timeout": 0.5, "transient_wait_budget_sec": 0}


@pytest.mark.anyio
async def test_sweep_progress_survives_an_unhandled_error(db):
    """Progress must be persisted even when the sweep dies on an error it does
    not handle (#1359).

    The pass loop only catches timeout / rate-limit / flood. A network error or
    a cancellation unwinds straight past it, so without a flush on the way out
    everything the chunk flushes had accumulated since the last chunk boundary
    is discarded — exactly the loss this PR exists to prevent.
    """
    from src.telegram.client_pool import ClientPool

    pool = _sweep_pool(db)
    seen = _make_channel_dialog(-1001, title="Seen", message_id=11)

    async def _boom():
        yield seen
        raise ConnectionError("network died mid-sweep")

    mock_client = MagicMock()
    mock_client.iter_dialogs.side_effect = [_boom()]
    pool.get_client_by_phone = AsyncMock(return_value=(mock_client, "+1234567890"))

    with pytest.raises(ConnectionError):
        await ClientPool._fetch_dialogs_for_phone(
            pool, "+1234567890", True, "full", "full"
        )

    cached = await db.repos.dialog_cache.list_dialogs("+1234567890")
    assert -1001 in {row["channel_id"] for row in cached}
    # The lease is released on this path too (finally), not only on success.
    pool.release_client.assert_awaited_once_with("+1234567890")


@pytest.mark.anyio
async def test_multi_pass_sweep_survives_the_rate_limit_gate(db):
    """The resume loop must not be strangled by our own proactive gate (#1359).

    A sweep is ONE user-facing operation that Telethon splits into ~N/100
    paginated requests under a single reservation, and a flood mid-pagination
    means "slow down", not "stop". While the sweep shared the warm-up's 1/min
    bucket, pass 2 was refused on every account with enough dialogs to need
    one, so the feature was inert in production while the tests — which never
    wired a gate onto the pool — reported success.
    """
    from src.telegram.client_pool import ClientPool
    from src.telegram.rate_limit_gate import TelegramRateLimitGate

    pool = _sweep_pool(db)
    assert isinstance(pool._rate_limit_gate, TelegramRateLimitGate), (
        "the fixture must carry a real gate, otherwise this proves nothing"
    )

    first = _make_channel_dialog(-1001, title="First", message_id=11)
    second = _make_channel_dialog(-1002, title="Second", message_id=22)

    async def _stalling():
        yield first
        await asyncio.sleep(120)

    mock_client = MagicMock()
    mock_client.iter_dialogs.side_effect = [_stalling(), _dialog_stream([second])]
    pool.get_client_by_phone = AsyncMock(return_value=(mock_client, "+1234567890"))

    original_wait_for = asyncio.wait_for

    async def fast_wait_for(coro, timeout):
        return await original_wait_for(coro, timeout=min(timeout, 0.05))

    with patch("src.telegram.pool_dialogs.asyncio.wait_for", fast_wait_for):
        result = await ClientPool._fetch_dialogs_for_phone(
            pool, "+1234567890", True, "full", "full"
        )

    # Decisive: pass 2 actually ran and the sweep completed.
    assert mock_client.iter_dialogs.call_count == 2
    assert result.partial is False
    assert [row["channel_id"] for row in result] == [-1001, -1002]


@pytest.mark.anyio
async def test_partial_sweep_reports_rows_actually_written(db):
    """"Saved N" must count persisted rows, not the length of the result.

    A channels_only sweep never fills the write buffer, so it persists nothing
    — yet the warning used to announce the whole result as saved, which is the
    same misleading success #1350 exists to remove.
    """
    from src.telegram.client_pool import ClientPool
    from src.telegram.flood_wait import FloodWaitInfo, HandledFloodWaitError

    pool = _sweep_pool(db)
    reached = _make_channel_dialog(-1001, title="Reached", message_id=11)

    async def _floods():
        yield reached
        raise HandledFloodWaitError(
            FloodWaitInfo(
                operation="get_dialogs_for_phone",
                phone="+1234567890",
                wait_seconds=3600,
                next_available_at_utc=datetime(2026, 1, 1, tzinfo=timezone.utc),
                detail="blocking",
            )
        )

    mock_client = MagicMock()
    mock_client.iter_dialogs.side_effect = [_floods()]
    pool.get_client_by_phone = AsyncMock(return_value=(mock_client, "+1234567890"))

    result = await ClientPool._fetch_dialogs_for_phone(
        pool, "+1234567890", False, "channels_only", "channels_only"
    )

    assert result.partial is True
    assert len(result) == 1
    # Nothing was written for channels_only, so nothing may be claimed as saved.
    assert result.saved == 0
    assert await db.repos.dialog_cache.list_dialogs("+1234567890") == []


async def test_sweep_keeps_progress_on_blocking_flood(db):
    """A long flood ends the sweep partial, but what arrived is persisted."""
    from src.telegram.client_pool import ClientPool
    from src.telegram.flood_wait import FloodWaitInfo, HandledFloodWaitError

    pool = _sweep_pool(db)
    kept = _make_channel_dialog(-1001, title="Kept", message_id=11)

    async def _floods():
        yield kept
        raise HandledFloodWaitError(
            FloodWaitInfo(
                operation="get_dialogs_for_phone",
                phone="+1234567890",
                wait_seconds=3600,
                next_available_at_utc=datetime(2026, 1, 1, tzinfo=timezone.utc),
                detail="blocking",
            )
        )

    mock_client = MagicMock()
    mock_client.iter_dialogs.side_effect = [_floods(), _dialog_stream([])]
    pool.get_client_by_phone = AsyncMock(return_value=(mock_client, "+1234567890"))

    result = await ClientPool._fetch_dialogs_for_phone(
        pool, "+1234567890", True, "full", "full"
    )

    assert result.partial is True
    # A blocking flood must not be retried, and the progress must be saved.
    assert mock_client.iter_dialogs.call_count == 1
    cached = await db.repos.dialog_cache.list_dialogs("+1234567890")
    assert [row["channel_id"] for row in cached] == [-1001]


async def test_sweep_stops_when_a_resumed_pass_adds_nothing(db):
    """No new dialogs on resume means stop -- and stay honest about it."""
    from src.telegram.client_pool import ClientPool

    pool = _sweep_pool(db)
    only = _make_channel_dialog(-1001, title="Only", message_id=11)

    async def _stalling():
        yield only
        await asyncio.sleep(120)

    mock_client = MagicMock()
    mock_client.iter_dialogs.side_effect = [_stalling(), _dialog_stream([])]
    pool.get_client_by_phone = AsyncMock(return_value=(mock_client, "+1234567890"))

    original_wait_for = asyncio.wait_for

    async def fast_wait_for(coro, timeout):
        return await original_wait_for(coro, timeout=min(timeout, 0.05))

    with patch("src.telegram.pool_dialogs.asyncio.wait_for", fast_wait_for):
        result = await ClientPool._fetch_dialogs_for_phone(
            pool, "+1234567890", True, "full", "full"
        )

    assert mock_client.iter_dialogs.call_count == 2
    # An empty resumed pass proves nothing about completeness -> stays partial,
    # so a truncated snapshot can never overwrite the cache as if it were full.
    assert result.partial is True


async def test_sweep_dedupes_overlapping_boundary_page(db):
    """Resuming re-reads the boundary dialog; it must appear once."""
    from src.telegram.client_pool import ClientPool

    pool = _sweep_pool(db)
    first = _make_channel_dialog(-1001, title="First", message_id=11)
    overlap = _make_channel_dialog(-1001, title="First again", message_id=11)
    fresh = _make_channel_dialog(-1002, title="Second", message_id=22)

    async def _stalling():
        yield first
        await asyncio.sleep(120)

    mock_client = MagicMock()
    mock_client.iter_dialogs.side_effect = [_stalling(), _dialog_stream([overlap, fresh])]
    pool.get_client_by_phone = AsyncMock(return_value=(mock_client, "+1234567890"))

    original_wait_for = asyncio.wait_for

    async def fast_wait_for(coro, timeout):
        return await original_wait_for(coro, timeout=min(timeout, 0.05))

    with patch("src.telegram.pool_dialogs.asyncio.wait_for", fast_wait_for):
        result = await ClientPool._fetch_dialogs_for_phone(
            pool, "+1234567890", True, "full", "full"
        )

    assert [row["channel_id"] for row in result] == [-1001, -1002]


async def test_partial_sweep_does_not_poison_the_memory_cache(db):
    """A partial snapshot must not be served as complete for the whole TTL."""
    from src.telegram.client_pool import ClientPool

    pool = _sweep_pool(db)
    only = _make_channel_dialog(-1001, title="Only", message_id=11)

    async def _stalling():
        yield only
        await asyncio.sleep(120)

    mock_client = MagicMock()
    mock_client.iter_dialogs.side_effect = [_stalling(), _dialog_stream([])]
    pool.get_client_by_phone = AsyncMock(return_value=(mock_client, "+1234567890"))

    original_wait_for = asyncio.wait_for

    async def fast_wait_for(coro, timeout):
        return await original_wait_for(coro, timeout=min(timeout, 0.05))

    with patch("src.telegram.pool_dialogs.asyncio.wait_for", fast_wait_for):
        result = await ClientPool._fetch_dialogs_for_phone(
            pool, "+1234567890", True, "full", "full"
        )

    assert result.partial is True
    assert pool._get_cached_dialogs("+1234567890", "full") is None


async def test_upsert_dialogs_updates_in_place_and_isolates_phones(db):
    """upsert refreshes a row without duplicating it or touching other phones."""
    repo = db.repos.dialog_cache
    await repo.replace_dialogs("+1111111111", [dict(_FAKE_DIALOGS[0])])
    await repo.replace_dialogs("+2222222222", [dict(_FAKE_DIALOGS[1])])

    renamed = dict(_FAKE_DIALOGS[0])
    renamed["title"] = "Renamed"
    added = {
        "channel_id": -100777,
        "title": "Added",
        "username": "added",
        "channel_type": "channel",
        "deactivate": False,
        "is_own": False,
    }
    await repo.upsert_dialogs("+1111111111", [renamed, added])

    rows = {row["channel_id"]: row for row in await repo.list_dialogs("+1111111111")}
    assert len(rows) == 2
    assert rows[_FAKE_DIALOGS[0]["channel_id"]]["title"] == "Renamed"
    assert rows[-100777]["title"] == "Added"

    other = await repo.list_dialogs("+2222222222")
    assert [row["channel_id"] for row in other] == [_FAKE_DIALOGS[1]["channel_id"]]


async def test_sweep_ends_partial_when_the_gate_refuses_the_next_pass(db):
    """Our own gate stops the sweep; progress is kept, not discarded (#1359).

    DIALOGS_SPEC allows one stream_dialogs per minute and the limiter refuses
    reservations beyond max_calls, so a second pass is genuinely unavailable.
    """
    from src.telegram.client_pool import ClientPool
    from src.telegram.rate_limit_gate import TelegramRateLimitedError

    pool = _sweep_pool(db)
    kept = _make_channel_dialog(-1001, title="Kept", message_id=11)

    async def _then_refused():
        yield kept
        raise TelegramRateLimitedError("+1234567890", "dialogs", 42.0)

    mock_client = MagicMock()
    mock_client.iter_dialogs.side_effect = [_then_refused(), _dialog_stream([])]
    pool.get_client_by_phone = AsyncMock(return_value=(mock_client, "+1234567890"))

    result = await ClientPool._fetch_dialogs_for_phone(
        pool, "+1234567890", True, "full", "full"
    )

    assert result.partial is True
    assert mock_client.iter_dialogs.call_count == 1
    cached = await db.repos.dialog_cache.list_dialogs("+1234567890")
    assert [row["channel_id"] for row in cached] == [-1001]


@pytest.mark.anyio
async def test_completed_sweep_leaves_a_fresh_snapshot(db):
    """A sweep that finishes must leave the cache readable as fresh (#1359).

    The chunk flushes run on the success path too — they accumulate long before
    anyone knows the sweep will finish. Each one ages the WHOLE phone on
    purpose, so an incomplete snapshot can never read as current. The success
    path then rewrites everything with replace_dialogs and a current timestamp.

    But the ``finally`` flush fires on the success path as well, and the tail
    left in ``pending`` (whatever did not fill the last chunk) went through
    upsert_dialogs AFTER replace_dialogs had already stamped the snapshot —
    re-ageing all of it to the epoch. A completed full sweep therefore marked
    itself stale, and the very next read re-fetched every dialog from Telegram:
    observed live as 2367 dialogs all sitting at 1970-01-01.
    """
    from src.telegram.client_pool import ClientPool
    from src.telegram.pool_dialogs import DIALOG_FETCH_UPSERT_CHUNK

    phone = "+1234567890"
    pool = _sweep_pool(db, phone)
    # More than one chunk, and deliberately NOT a multiple of the chunk size:
    # the remainder is what the finally-flush picks up.
    total = DIALOG_FETCH_UPSERT_CHUNK + 7
    dialogs = [
        _make_channel_dialog(-(1000 + i), title=f"Chan {i}", message_id=i + 1)
        for i in range(total)
    ]

    mock_client = MagicMock()
    mock_client.iter_dialogs.return_value = _dialog_stream(dialogs)
    pool.get_client_by_phone = AsyncMock(return_value=(mock_client, phone))

    result = await ClientPool._fetch_dialogs_for_phone(pool, phone, True, "full", "full")

    assert result.partial is False, "the sweep drained without interruption"
    assert len(result) == total

    cached_at = await db.repos.dialog_cache.get_cached_at(phone)
    age_sec = (datetime.now(timezone.utc) - cached_at).total_seconds()
    assert age_sec < 3600, (
        f"a completed sweep left the snapshot looking stale: cached_at={cached_at}"
    )
