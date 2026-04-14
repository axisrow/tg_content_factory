"""Integration tests for /channels/renames keep/filter endpoints.

Covers the honest state transitions required by issue #365:
- keep endpoint yields distinct flash codes for 3 outcomes
- filter endpoint ensures filtered state even after manual unfilter
- both endpoints are idempotent on already-decided events
"""
from __future__ import annotations

import base64

import pytest
from httpx import ASGITransport, AsyncClient

from src.database import Database
from src.models import Channel
from tests.helpers import make_test_config


async def _build_app_with_db(tmp_path):
    from src.scheduler.service import SchedulerManager
    from src.search.ai_search import AISearchEngine
    from src.search.engine import SearchEngine
    from src.telegram.auth import TelegramAuth
    from src.web.app import create_app

    config = make_test_config(tmp_path)
    app = create_app(config)
    db = Database(config.database.path)
    await db.initialize()
    app.state.db = db
    app.state.auth = TelegramAuth(12345, "test_hash")
    app.state.notifier = None
    # The rename-review endpoints don't call collector, but deps.get_container
    # requires a non-None attribute to build AppContainer.
    app.state.collector = object()
    app.state.search_engine = SearchEngine(db)
    app.state.ai_search = AISearchEngine(config.llm, db)
    app.state.scheduler = SchedulerManager(config.scheduler)
    app.state.session_secret = "test_secret_key"
    app.state.pool = type("Pool", (), {"clients": {}})()
    return app, db


def _auth_headers() -> dict[str, str]:
    return {
        "Authorization": "Basic " + base64.b64encode(b":testpass").decode(),
        "Origin": "http://test",
    }


async def _post(app, path: str):
    transport = ASGITransport(app=app)
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        follow_redirects=False,
        headers=_auth_headers(),
    ) as c:
        return await c.post(path)


async def _create_channel_with_flags(db, channel_id: int, *, filter_flags: str = "") -> int:
    ch = Channel(
        channel_id=channel_id,
        title="Test",
        username="test_chan",
        is_filtered=bool(filter_flags),
        filter_flags=filter_flags,
    )
    await db.add_channel(ch)
    # add_channel doesn't persist is_filtered/filter_flags; set them explicitly
    if filter_flags:
        await db.set_channels_filtered_bulk([(channel_id, filter_flags)])
    channels = await db.get_channels(include_filtered=True)
    return next(c for c in channels if c.channel_id == channel_id).id


# ---------- keep endpoint ----------


@pytest.mark.asyncio
async def test_keep_accepted_clean(tmp_path):
    """Channel has only rename-related flags → unfilter → flash rename_accepted."""
    app, db = await _build_app_with_db(tmp_path)
    try:
        pk = await _create_channel_with_flags(
            db, -100200, filter_flags="username_changed,title_changed"
        )
        event_id = await db.create_rename_event(-100200, "Old", "New", "old", "new")

        resp = await _post(app, f"/channels/renames/{event_id}/keep")
        assert resp.status_code == 303
        assert "msg=rename_accepted" in resp.headers["location"]
        assert "still_filtered" not in resp.headers["location"]

        channels = await db.get_channels(include_filtered=True)
        ch = next(c for c in channels if c.channel_id == -100200)
        assert ch.is_filtered is False
        assert ch.filter_flags == ""
        # Event marked decided
        event = await db.get_rename_event(event_id)
        assert event["decision"] == "keep"
        assert pk is not None  # sanity
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_keep_accepted_still_filtered(tmp_path):
    """Channel has other flags → stays filtered with non-rename flags, honest flash."""
    app, db = await _build_app_with_db(tmp_path)
    try:
        await _create_channel_with_flags(
            db, -100201, filter_flags="cross_channel_spam,username_changed"
        )
        event_id = await db.create_rename_event(-100201, "Old", "Old", "old", "new")

        resp = await _post(app, f"/channels/renames/{event_id}/keep")
        assert resp.status_code == 303
        assert "msg=rename_accepted_still_filtered" in resp.headers["location"]

        channels = await db.get_channels(include_filtered=True)
        ch = next(c for c in channels if c.channel_id == -100201)
        assert ch.is_filtered is True
        assert ch.filter_flags == "cross_channel_spam"
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_keep_already_decided(tmp_path):
    """Second POST on the same event is a no-op and returns rename_already_decided."""
    app, db = await _build_app_with_db(tmp_path)
    try:
        await _create_channel_with_flags(db, -100202, filter_flags="username_changed")
        event_id = await db.create_rename_event(-100202, "A", "A", "a", "b")

        first = await _post(app, f"/channels/renames/{event_id}/keep")
        assert "msg=rename_accepted" in first.headers["location"]

        second = await _post(app, f"/channels/renames/{event_id}/keep")
        assert second.status_code == 303
        assert "msg=rename_already_decided" in second.headers["location"]

        # State unchanged by the second call
        channels = await db.get_channels(include_filtered=True)
        ch = next(c for c in channels if c.channel_id == -100202)
        assert ch.is_filtered is False
    finally:
        await db.close()


# ---------- filter endpoint ----------


@pytest.mark.asyncio
async def test_filter_ensures_state_even_after_manual_unfilter(tmp_path):
    """Admin manually unfiltered a channel between detection and the filter click.
    The filter endpoint must still leave it filtered with the correct flags."""
    app, db = await _build_app_with_db(tmp_path)
    try:
        pk = await _create_channel_with_flags(
            db, -100203, filter_flags="username_changed"
        )
        event_id = await db.create_rename_event(-100203, "Old", "Old", "old", "new")

        # Manually unfilter the channel (simulates an admin poke).
        await db.set_channel_filtered(pk, False)
        channels = await db.get_channels(include_filtered=True)
        pre = next(c for c in channels if c.channel_id == -100203)
        assert pre.is_filtered is False  # sanity

        resp = await _post(app, f"/channels/renames/{event_id}/filter")
        assert resp.status_code == 303
        assert "msg=rename_filtered" in resp.headers["location"]

        channels = await db.get_channels(include_filtered=True)
        ch = next(c for c in channels if c.channel_id == -100203)
        assert ch.is_filtered is True
        # Username changed between old/new in our event, so flag is present.
        assert "username_changed" in ch.filter_flags
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_filter_already_decided(tmp_path):
    app, db = await _build_app_with_db(tmp_path)
    try:
        await _create_channel_with_flags(db, -100204, filter_flags="username_changed")
        event_id = await db.create_rename_event(-100204, "A", "A", "old", "new")

        first = await _post(app, f"/channels/renames/{event_id}/filter")
        assert "msg=rename_filtered" in first.headers["location"]

        second = await _post(app, f"/channels/renames/{event_id}/filter")
        assert "msg=rename_already_decided" in second.headers["location"]
    finally:
        await db.close()


# ---------- rename_events_page ----------


@pytest.mark.asyncio
async def test_rename_events_page_renders(tmp_path):
    """Test rename events page renders with events list."""
    app, db = await _build_app_with_db(tmp_path)
    try:
        await db.create_rename_event(-100300, "OldName", "NewName", "old_un", "new_un")
        transport = ASGITransport(app=app)
        async with AsyncClient(
            transport=transport,
            base_url="http://test",
            follow_redirects=True,
            headers=_auth_headers(),
        ) as c:
            resp = await c.get("/channels/renames")
        assert resp.status_code == 200
        assert "OldName" in resp.text or "NewName" in resp.text or "renames" in resp.text.lower()
    finally:
        await db.close()


# ---------- rename_events_count ----------


@pytest.mark.asyncio
async def test_rename_events_count_zero(tmp_path):
    """Test rename events count returns empty when zero."""
    app, db = await _build_app_with_db(tmp_path)
    try:
        transport = ASGITransport(app=app)
        async with AsyncClient(
            transport=transport,
            base_url="http://test",
            headers=_auth_headers(),
        ) as c:
            resp = await c.get("/channels/renames/count")
        assert resp.status_code == 200
        assert resp.text == ""
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_rename_events_count_nonzero(tmp_path):
    """Test rename events count returns badge when nonzero."""
    app, db = await _build_app_with_db(tmp_path)
    try:
        await db.create_rename_event(-100301, "A", "B", "a", "b")
        transport = ASGITransport(app=app)
        async with AsyncClient(
            transport=transport,
            base_url="http://test",
            headers=_auth_headers(),
        ) as c:
            resp = await c.get("/channels/renames/count")
        assert resp.status_code == 200
        assert "badge" in resp.text
    finally:
        await db.close()


# ---------- _rename_required_flags unit test ----------


def test_rename_required_flags_title_and_username():
    """Both title and username changed."""
    from src.web.routes.channel_renames import _rename_required_flags
    flags = _rename_required_flags({
        "old_title": "A", "new_title": "B",
        "old_username": "a", "new_username": "b",
    })
    assert flags == {"title_changed", "username_changed"}


def test_rename_required_flags_title_only():
    """Only title changed."""
    from src.web.routes.channel_renames import _rename_required_flags
    flags = _rename_required_flags({
        "old_title": "A", "new_title": "B",
        "old_username": "same", "new_username": "same",
    })
    assert flags == {"title_changed"}


def test_rename_required_flags_username_only():
    """Only username changed."""
    from src.web.routes.channel_renames import _rename_required_flags
    flags = _rename_required_flags({
        "old_title": "same", "new_title": "same",
        "old_username": "a", "new_username": "b",
    })
    assert flags == {"username_changed"}


def test_rename_required_flags_no_change():
    """No change detected — fallback to username_changed."""
    from src.web.routes.channel_renames import _rename_required_flags
    flags = _rename_required_flags({
        "old_title": "same", "new_title": "same",
        "old_username": "same", "new_username": "same",
    })
    assert flags == {"username_changed"}


# ---------- keep endpoint — channel removed ----------


@pytest.mark.asyncio
async def test_keep_channel_removed(tmp_path):
    """Channel was deleted while event was pending — event closed."""
    app, db = await _build_app_with_db(tmp_path)
    try:
        # Create event for a channel that doesn't exist in DB
        event_id = await db.create_rename_event(-100999, "Old", "New", "old", "new")

        resp = await _post(app, f"/channels/renames/{event_id}/keep")
        assert resp.status_code == 303
        assert "msg=rename_already_decided" in resp.headers["location"]

        event = await db.get_rename_event(event_id)
        assert event["decision"] == "keep"
    finally:
        await db.close()
