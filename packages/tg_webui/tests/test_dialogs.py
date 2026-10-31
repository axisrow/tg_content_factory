# Web-panel tests split out of tests/test_dialogs.py (#1720 wave 3).
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from webui_helpers import build_web_app, make_auth_client

from tests.helpers import AsyncIterMessages, FakeCliTelethonClient
from tg_agent.config import AppConfig

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


def _dialog_stream(dialogs):
    async def _iter():
        for dialog in dialogs:
            yield dialog

    return _iter()


async def test_stream_dialogs_forwards_offset_kwargs():
    """Offsets reach Telethon; a plain sweep still calls it with none (#1359)."""
    from tg_agent.telegram.backends import TelegramTransportSession

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


def test_submit_confirm_reads_submitter_data_confirm():
    """The global submit-confirm handler must read data-confirm from the clicked
    submitter (e.submitter), not only from the form. A submitter button with a
    distinct data-confirm (e.g. the "Удалить" button posting via formaction) must
    show ITS own confirmation, not the form's — otherwise an irreversible delete
    shows the form's softer "leave" prompt."""
    from tg_webui.paths import STATIC_DIR

    app_js = (STATIC_DIR / "js" / "app.js").read_text(encoding="utf-8")
    assert "submitter.dataset.confirm" in app_js
