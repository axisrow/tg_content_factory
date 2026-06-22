"""Tests for filter routes."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from src.models import ChannelStats
from src.services.filter_deletion_service import PurgeResult
from tests.routes.conftest import _add_channel, _add_filtered_channel, _enable_dev_mode


@pytest.fixture
async def db(base_app):
    """Get db from base_app."""
    _, db, _ = base_app
    return db


@pytest.mark.anyio
async def test_filter_manage_page_is_skeleton(route_client):
    """Lazyload (#952): the page shell defers the filtered-channels table."""
    resp = await route_client.get("/channels/filter/manage")
    assert resp.status_code == 200
    assert "/channels/filter/manage/fragments/table" in resp.text
    assert 'hx-trigger="load"' in resp.text


@pytest.mark.anyio
async def test_filter_manage_renders_empty(route_client):
    """Test filter manage fragment renders empty."""
    resp = await route_client.get("/channels/filter/manage/fragments/table")
    assert resp.status_code == 200
    assert 'onclick="refreshFilters(this)"' in resp.text
    assert "async function refreshFilters(button)" in resp.text
    assert "Сейчас запустится только сбор статистики." in resp.text


@pytest.mark.anyio
async def test_filter_manage_shows_filtered(route_client, db):
    """Test filter manage fragment shows filtered channels."""
    await _add_filtered_channel(db, channel_id=300, title="Filtered Channel")

    resp = await route_client.get("/channels/filter/manage/fragments/table")
    assert resp.status_code == 200
    assert "Filtered Channel" in resp.text


@pytest.mark.anyio
async def test_purge_selected_no_pks(route_client):
    """Test purge selected with no PKs returns error."""
    resp = await route_client.post("/channels/filter/purge-selected", follow_redirects=False)
    assert resp.status_code == 303
    assert "error=no_filtered_channels" in resp.headers["location"]


@pytest.mark.anyio
async def test_purge_selected_success(route_client, db):
    """Test purge selected channels."""
    pk = await _add_filtered_channel(db, channel_id=400, title="To Purge")

    with patch("src.web.filter.handlers.deps.filter_deletion_service") as mock_svc:
        mock_svc.return_value.purge_channels_by_pks = AsyncMock(
            return_value=PurgeResult(purged_count=1)
        )
        resp = await route_client.post(
            "/channels/filter/purge-selected",
            data={"pks": [str(pk)]},
            follow_redirects=False,
        )
        assert resp.status_code == 303
        assert "msg=purged_selected" in resp.headers["location"]


@pytest.mark.anyio
async def test_purge_selected_removes_messages(route_client, db):
    """Test purge selected removes messages from DB."""
    pk = await _add_filtered_channel(db, channel_id=500, title="Purge Messages")

    with patch("src.web.filter.handlers.deps.filter_deletion_service") as mock_svc:
        mock_svc.return_value.purge_channels_by_pks = AsyncMock(
            return_value=PurgeResult(purged_count=1)
        )
        resp = await route_client.post(
            "/channels/filter/purge-selected",
            data={"pks": [str(pk)]},
            follow_redirects=False,
        )

    assert resp.status_code == 303
    assert "msg=purged_selected" in resp.headers["location"]
    mock_svc.return_value.purge_channels_by_pks.assert_awaited_once_with([pk])


@pytest.mark.anyio
async def test_purge_selected_partial_failure_surfaces_error(route_client, db):
    """A purge that records errors must report purge_partial, not success (#676)."""
    pk = await _add_filtered_channel(db, channel_id=600, title="Boom")

    with patch("src.web.filter.handlers.deps.filter_deletion_service") as mock_svc:
        mock_svc.return_value.purge_channels_by_pks = AsyncMock(
            return_value=PurgeResult(skipped_count=1, errors=[f"pk={pk}: DB error"])
        )
        resp = await route_client.post(
            "/channels/filter/purge-selected",
            data={"pks": [str(pk)]},
            follow_redirects=False,
        )

    assert resp.status_code == 303
    assert "error=purge_partial" in resp.headers["location"]
    assert "msg=purged_selected" not in resp.headers["location"]


@pytest.mark.anyio
async def test_purge_all_no_filtered(route_client):
    """Test purge all with no filtered channels."""
    with patch("src.web.filter.handlers.deps.filter_deletion_service") as mock_svc:
        from src.services.filter_deletion_service import PurgeResult
        mock_svc.return_value.purge_all_filtered = AsyncMock(
            return_value=PurgeResult(purged_count=0)
        )
        resp = await route_client.post("/channels/filter/purge-all", follow_redirects=False)
        assert resp.status_code == 303
        assert "error=no_filtered_channels" in resp.headers["location"]


@pytest.mark.anyio
async def test_purge_all_success(route_client, db):
    """Test purge all filtered channels."""
    await _add_filtered_channel(db, channel_id=600, title="Purge All")

    with patch("src.web.filter.handlers.deps.filter_deletion_service") as mock_svc:
        from src.services.filter_deletion_service import PurgeResult
        mock_svc.return_value.purge_all_filtered = AsyncMock(
            return_value=PurgeResult(purged_count=1)
        )
        resp = await route_client.post("/channels/filter/purge-all", follow_redirects=False)
        assert resp.status_code == 303
        assert "msg=purged_all_filtered" in resp.headers["location"]


@pytest.mark.anyio
async def test_purge_all_partial_failure_surfaces_error(route_client, db):
    """purge-all with a real per-channel error must report purge_partial, not success (#676 review)."""
    with patch("src.web.filter.handlers.deps.filter_deletion_service") as mock_svc:
        mock_svc.return_value.purge_all_filtered = AsyncMock(
            return_value=PurgeResult(purged_count=1, skipped_count=1, errors=["pk=2: DB error"])
        )
        resp = await route_client.post("/channels/filter/purge-all", follow_redirects=False)
        assert resp.status_code == 303
        assert "error=purge_partial" in resp.headers["location"]
        assert "msg=purged_all_filtered" not in resp.headers["location"]


@pytest.mark.anyio
async def test_hard_delete_blocked_without_dev_mode(route_client, db):
    """Test hard delete blocked without dev mode."""
    pk = await _add_filtered_channel(db, channel_id=700, title="Hard Delete")
    resp = await route_client.post(
        "/channels/filter/hard-delete-selected",
        data={"pks": [str(pk)]},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "error=dev_mode_required_for_hard_delete" in resp.headers["location"]


@pytest.mark.anyio
async def test_hard_delete_no_pks(route_client, db):
    """Test hard delete with no PKs."""
    await _enable_dev_mode(db)
    resp = await route_client.post("/channels/filter/hard-delete-selected", follow_redirects=False)
    assert resp.status_code == 303
    assert "error=no_filtered_channels" in resp.headers["location"]


@pytest.mark.anyio
async def test_hard_delete_selected_partial_failure_reports_error(route_client, db):
    """Codex round 9 follow-up: the selected hard-delete route used to
    always redirect with msg=deleted_filtered even when the service
    reported skipped rows.  After the round-9 fix the route must
    surface skipped_count and any purged/expected mismatch the same way
    /hard-delete-all does."""
    pk1 = await _add_filtered_channel(db, channel_id=810, title="Sel OK")
    pk2 = await _add_filtered_channel(db, channel_id=811, title="Sel Skip")
    await _enable_dev_mode(db)
    with patch("src.web.filter.handlers.deps.filter_deletion_service") as mock_svc:
        mock_svc.return_value.hard_delete_channels_by_pks = AsyncMock(
            return_value=PurgeResult(purged_count=1, skipped_count=1)
        )
        resp = await route_client.post(
            "/channels/filter/hard-delete-selected",
            data={"pks": [str(pk1), str(pk2)]},
            follow_redirects=False,
        )
        assert resp.status_code == 303
        loc = resp.headers["location"]
        assert "error=hard_delete_partial" in loc
        assert "purged=1" in loc
        assert "skipped=1" in loc
        assert "expected=2" in loc


@pytest.mark.anyio
async def test_hard_delete_success(route_client, db):
    """Test hard delete channels."""
    pk = await _add_filtered_channel(db, channel_id=800, title="Hard Delete OK")
    await _enable_dev_mode(db)

    with patch("src.web.filter.handlers.deps.filter_deletion_service") as mock_svc:
        from src.services.filter_deletion_service import PurgeResult
        mock_svc.return_value.hard_delete_channels_by_pks = AsyncMock(
            return_value=PurgeResult(purged_count=1)
        )
        resp = await route_client.post(
            "/channels/filter/hard-delete-selected",
            data={"pks": [str(pk)]},
            follow_redirects=False,
        )
        assert resp.status_code == 303
        assert "msg=deleted_filtered" in resp.headers["location"]


@pytest.mark.anyio
async def test_hard_delete_all_blocked_without_dev_mode(route_client, db):
    """Direct POST without dev mode bounces with the dev-mode error."""
    pk = await _add_filtered_channel(db, channel_id=900, title="No DevMode")
    resp = await route_client.post(
        "/channels/filter/hard-delete-all",
        data={"confirm": "DELETE_ALL_FILTERED", "confirm_pks": f"{pk}:900"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "error=dev_mode_required_for_hard_delete" in resp.headers["location"]


@pytest.mark.anyio
async def test_hard_delete_all_rejects_without_confirm_phrase(route_client, db):
    """Codex round 5 finding: server must require the confirm phrase even
    when dev mode is on — a direct POST without it must NOT delete."""
    pk = await _add_filtered_channel(db, channel_id=901, title="No Confirm")
    await _enable_dev_mode(db)
    with patch("src.web.filter.handlers.deps.filter_deletion_service") as mock_svc:
        mock_svc.return_value.hard_delete_channels_by_pks = AsyncMock(
            return_value=PurgeResult(purged_count=1)
        )
        resp = await route_client.post(
            "/channels/filter/hard-delete-all",
            data={"confirm_pks": f"{pk}:901"},  # confirm missing
            follow_redirects=False,
        )
        assert resp.status_code == 303
        assert "error=hard_delete_confirm_required" in resp.headers["location"]
        mock_svc.return_value.hard_delete_channels_by_pks.assert_not_called()
    remaining = await db.get_channel_by_pk(pk)
    assert remaining is not None


@pytest.mark.anyio
async def test_hard_delete_all_rejects_wrong_confirm_phrase(route_client, db):
    """A non-matching confirm value must be rejected (no fuzzy match)."""
    pk = await _add_filtered_channel(db, channel_id=902, title="Wrong Confirm")
    await _enable_dev_mode(db)
    with patch("src.web.filter.handlers.deps.filter_deletion_service") as mock_svc:
        mock_svc.return_value.hard_delete_channels_by_pks = AsyncMock(
            return_value=PurgeResult(purged_count=1)
        )
        resp = await route_client.post(
            "/channels/filter/hard-delete-all",
            data={"confirm": "yes", "confirm_pks": f"{pk}:902"},
            follow_redirects=False,
        )
        assert resp.status_code == 303
        assert "error=hard_delete_confirm_required" in resp.headers["location"]
        mock_svc.return_value.hard_delete_channels_by_pks.assert_not_called()


@pytest.mark.anyio
async def test_hard_delete_all_rejects_missing_confirm_pks(route_client, db):
    """confirm_pks is required: a request without it cannot reach the delete."""
    await _add_filtered_channel(db, channel_id=903, title="No PKs")
    await _enable_dev_mode(db)
    with patch("src.web.filter.handlers.deps.filter_deletion_service") as mock_svc:
        mock_svc.return_value.hard_delete_channels_by_pks = AsyncMock(
            return_value=PurgeResult(purged_count=1)
        )
        resp = await route_client.post(
            "/channels/filter/hard-delete-all",
            data={"confirm": "DELETE_ALL_FILTERED"},  # confirm_pks missing
            follow_redirects=False,
        )
        assert resp.status_code == 303
        assert "error=hard_delete_confirm_required" in resp.headers["location"]
        mock_svc.return_value.hard_delete_channels_by_pks.assert_not_called()


@pytest.mark.anyio
async def test_hard_delete_all_rejects_malformed_confirm_pks(route_client, db):
    """confirm_pks with malformed tokens must be rejected before delete."""
    await _add_filtered_channel(db, channel_id=904, title="Bad PKs")
    await _enable_dev_mode(db)
    cases = [
        "1,2,3",         # bare PKs without :channel_id
        "1:abc",         # non-int channel_id
        "abc:1",         # non-int pk
        "1:2:3",         # too many fields
        "1:",            # empty channel_id
        ":1",            # empty pk
    ]
    for raw in cases:
        with patch("src.web.filter.handlers.deps.filter_deletion_service") as mock_svc:
            mock_svc.return_value.hard_delete_channels_by_pks = AsyncMock(
                return_value=PurgeResult(purged_count=1)
            )
            resp = await route_client.post(
                "/channels/filter/hard-delete-all",
                data={"confirm": "DELETE_ALL_FILTERED", "confirm_pks": raw},
                follow_redirects=False,
            )
            assert resp.status_code == 303, f"failed for raw={raw!r}"
            assert "error=hard_delete_confirm_required" in resp.headers["location"], raw
            mock_svc.return_value.hard_delete_channels_by_pks.assert_not_called()


@pytest.mark.anyio
async def test_hard_delete_all_rejects_duplicate_confirm_pks(route_client, db):
    """Codex round 7 regression: duplicate PK or channel_id tokens in the
    snapshot must be rejected so a crafted '1:1001,1:1001' cannot smuggle
    extra delete attempts past the set comparison."""
    pk = await _add_filtered_channel(db, channel_id=970, title="Will Stay")
    await _enable_dev_mode(db)
    cases = [
        f"{pk}:970,{pk}:970",   # exact duplicate
        f"{pk}:970,{pk}:971",   # duplicate pk, different chid
        f"{pk}:970,99:970",     # duplicate chid, different pk
    ]
    for raw in cases:
        with patch("src.web.filter.handlers.deps.filter_deletion_service") as mock_svc:
            mock_svc.return_value.hard_delete_channels_by_pks = AsyncMock(
                return_value=PurgeResult(purged_count=1)
            )
            resp = await route_client.post(
                "/channels/filter/hard-delete-all",
                data={"confirm": "DELETE_ALL_FILTERED", "confirm_pks": raw},
                follow_redirects=False,
            )
            assert resp.status_code == 303, f"failed for raw={raw!r}"
            assert "error=hard_delete_confirm_required" in resp.headers["location"], raw
            mock_svc.return_value.hard_delete_channels_by_pks.assert_not_called()
    # The original filtered channel must still exist.
    remaining = await db.get_channel_by_pk(pk)
    assert remaining is not None


@pytest.mark.anyio
async def test_hard_delete_all_rejects_pk_with_wrong_channel_id(route_client, db):
    """Codex round 7 regression: PK reuse / channel-id mismatch must reject.

    The snapshot includes the Telegram channel_id alongside the rowid. If a
    confirm row carries the right pk but a stale or fabricated channel_id
    (or vice versa), the (pk, channel_id) comparison must reject — this
    closes the PK-reuse-after-delete window.
    """
    pk = await _add_filtered_channel(db, channel_id=975, title="Stable")
    await _enable_dev_mode(db)
    with patch("src.web.filter.handlers.deps.filter_deletion_service") as mock_svc:
        mock_svc.return_value.hard_delete_channels_by_pks = AsyncMock(
            return_value=PurgeResult(purged_count=1)
        )
        # right pk, wrong channel_id (admin's snapshot is out of date)
        resp = await route_client.post(
            "/channels/filter/hard-delete-all",
            data={"confirm": "DELETE_ALL_FILTERED", "confirm_pks": f"{pk}:9999"},
            follow_redirects=False,
        )
        assert resp.status_code == 303
        assert "error=hard_delete_set_changed" in resp.headers["location"]
        mock_svc.return_value.hard_delete_channels_by_pks.assert_not_called()
    remaining = await db.get_channel_by_pk(pk)
    assert remaining is not None


@pytest.mark.anyio
async def test_hard_delete_all_rejects_set_mismatch(route_client, db):
    """Stale page snapshots a different filtered set — bounce."""
    pk = await _add_filtered_channel(db, channel_id=920, title="OnlyMe")
    await _enable_dev_mode(db)
    with patch("src.web.filter.handlers.deps.filter_deletion_service") as mock_svc:
        mock_svc.return_value.hard_delete_channels_by_pks = AsyncMock(
            return_value=PurgeResult(purged_count=1)
        )
        resp = await route_client.post(
            "/channels/filter/hard-delete-all",
            data={
                "confirm": "DELETE_ALL_FILTERED",
                "confirm_pks": "910:910,911:911,912:912",
            },
            follow_redirects=False,
        )
        assert resp.status_code == 303
        assert "error=hard_delete_set_changed" in resp.headers["location"]
        mock_svc.return_value.hard_delete_channels_by_pks.assert_not_called()
    remaining = await db.get_channel_by_pk(pk)
    assert remaining is not None


@pytest.mark.anyio
async def test_hard_delete_all_rejects_same_count_stale_swap(route_client, db):
    """Codex round 6 regression: same count, different (pk, channel_id) pair must not delete.

    Page rendered filtered=[A]. Between render and submit, A becomes unfiltered
    and B becomes filtered. len(filtered) is still 1, so a count-only check
    would let the delete fire on B — a channel the admin never confirmed.
    """
    a_pk = await _add_filtered_channel(db, channel_id=940, title="ChA")
    await db.set_channel_filtered(a_pk, False)
    b_pk = await _add_filtered_channel(db, channel_id=941, title="ChB")
    await _enable_dev_mode(db)
    with patch("src.web.filter.handlers.deps.filter_deletion_service") as mock_svc:
        mock_svc.return_value.hard_delete_channels_by_pks = AsyncMock(
            return_value=PurgeResult(purged_count=1)
        )
        resp = await route_client.post(
            "/channels/filter/hard-delete-all",
            data={
                "confirm": "DELETE_ALL_FILTERED",
                "confirm_pks": f"{a_pk}:940",  # stale: confirmed A
            },
            follow_redirects=False,
        )
        assert resp.status_code == 303
        assert "error=hard_delete_set_changed" in resp.headers["location"]
        mock_svc.return_value.hard_delete_channels_by_pks.assert_not_called()
    remaining = await db.get_channel_by_pk(b_pk)
    assert remaining is not None


@pytest.mark.anyio
async def test_hard_delete_all_success_with_matching_snapshot(route_client, db):
    """Correct confirm phrase + matching (pk, channel_id) snapshot allows
    the delete, and the service is called with exactly the confirmed PKs."""
    pk = await _add_filtered_channel(db, channel_id=950, title="Will Delete")
    await _enable_dev_mode(db)
    with patch("src.web.filter.handlers.deps.filter_deletion_service") as mock_svc:
        mock_svc.return_value.hard_delete_channels_by_pks = AsyncMock(
            return_value=PurgeResult(purged_count=1)
        )
        resp = await route_client.post(
            "/channels/filter/hard-delete-all",
            data={"confirm": "DELETE_ALL_FILTERED", "confirm_pks": f"{pk}:950"},
            follow_redirects=False,
        )
        assert resp.status_code == 303
        assert "msg=deleted_filtered" in resp.headers["location"]
        mock_svc.return_value.hard_delete_channels_by_pks.assert_awaited_once_with([pk])


@pytest.mark.anyio
async def test_hard_delete_all_rejects_empty_confirm(route_client, db):
    """Codex round 8 regression: confirm must be explicit, not pre-satisfied.

    The template no longer ships a hidden DELETE_ALL_FILTERED — admin must
    physically type the phrase. An empty (or whitespace-only) confirm field,
    which is the default when the text input is untouched, must be rejected
    so a stray submit does not perform the irreversible delete.
    """
    pk = await _add_filtered_channel(db, channel_id=980, title="Empty Confirm")
    await _enable_dev_mode(db)
    cases = ["", "   ", "\t", "\n"]
    for raw in cases:
        with patch("src.web.filter.handlers.deps.filter_deletion_service") as mock_svc:
            mock_svc.return_value.hard_delete_channels_by_pks = AsyncMock(
                return_value=PurgeResult(purged_count=1)
            )
            resp = await route_client.post(
                "/channels/filter/hard-delete-all",
                data={"confirm": raw, "confirm_pks": f"{pk}:980"},
                follow_redirects=False,
            )
            assert resp.status_code == 303, f"failed for confirm={raw!r}"
            assert "error=hard_delete_confirm_required" in resp.headers["location"]
            mock_svc.return_value.hard_delete_channels_by_pks.assert_not_called()
    remaining = await db.get_channel_by_pk(pk)
    assert remaining is not None


@pytest.mark.anyio
async def test_hard_delete_all_partial_failure_reports_error(route_client, db):
    """Codex round 8 regression: when the deletion service skips one or more
    channels (DB constraint, queue cancellation, transient error), the route
    must surface that as an error redirect — earlier rows are already gone
    irreversibly and the admin needs to know.
    """
    pk1 = await _add_filtered_channel(db, channel_id=981, title="ChDelOK")
    pk2 = await _add_filtered_channel(db, channel_id=982, title="ChDelFail")
    await _enable_dev_mode(db)
    with patch("src.web.filter.handlers.deps.filter_deletion_service") as mock_svc:
        # Service deleted pk1 but skipped pk2.
        mock_svc.return_value.hard_delete_channels_by_pks = AsyncMock(
            return_value=PurgeResult(purged_count=1, skipped_count=1)
        )
        resp = await route_client.post(
            "/channels/filter/hard-delete-all",
            data={
                "confirm": "DELETE_ALL_FILTERED",
                "confirm_pks": f"{pk1}:981,{pk2}:982",
            },
            follow_redirects=False,
        )
        assert resp.status_code == 303
        loc = resp.headers["location"]
        assert "error=hard_delete_partial" in loc
        assert "purged=1" in loc
        assert "skipped=1" in loc
        assert "expected=2" in loc


@pytest.mark.anyio
async def test_hard_delete_all_purge_count_mismatch_reports_error(route_client, db):
    """If the service returns fewer purges than the confirmed snapshot
    requested without an explicit skipped tally (e.g. an unusual error path),
    the route still surfaces it as an error rather than a silent partial."""
    pk = await _add_filtered_channel(db, channel_id=983, title="Mismatch")
    await _enable_dev_mode(db)
    with patch("src.web.filter.handlers.deps.filter_deletion_service") as mock_svc:
        # purged=0, skipped=0 — count mismatches expected (1).
        mock_svc.return_value.hard_delete_channels_by_pks = AsyncMock(
            return_value=PurgeResult(purged_count=0, skipped_count=0)
        )
        resp = await route_client.post(
            "/channels/filter/hard-delete-all",
            data={"confirm": "DELETE_ALL_FILTERED", "confirm_pks": f"{pk}:983"},
            follow_redirects=False,
        )
        assert resp.status_code == 303
        assert "error=hard_delete_partial" in resp.headers["location"]


@pytest.mark.anyio
async def test_hard_delete_all_no_filtered_channels(route_client, db):
    """No filtered channels → no_filtered_channels error even with a
    well-formed (empty) confirm_pks."""
    await _add_channel(db, channel_id=960, title="NotFiltered")
    await _enable_dev_mode(db)
    resp = await route_client.post(
        "/channels/filter/hard-delete-all",
        data={"confirm": "DELETE_ALL_FILTERED", "confirm_pks": ""},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "error=no_filtered_channels" in resp.headers["location"]


@pytest.mark.anyio
async def test_analyze_redirects(route_client):
    """Test analyze channels redirects (queues a background task, #793)."""
    resp = await route_client.post("/channels/filter/analyze", follow_redirects=False)
    assert resp.status_code == 303
    assert "/channels/filter/manage" in resp.headers["location"]
    assert "msg=filter_analyze_queued" in resp.headers["location"]


@pytest.mark.anyio
async def test_analyze_ignores_with_stats_query(route_client):
    """Test analyze route no longer runs stats collection inline."""
    with patch("src.web.filter.handlers.deps.collection_service") as mock_collection:
        resp = await route_client.post("/channels/filter/analyze?with_stats=1", follow_redirects=False)

    assert resp.status_code == 303
    mock_collection.assert_not_called()


@pytest.mark.anyio
async def test_analyze_returns_immediately_and_creates_task(route_client, db):
    """POST analyze enqueues a background task instead of running inline (#793)."""
    with patch("src.web.filter.handlers.ChannelAnalyzer") as mock_analyzer:
        mock_analyzer.return_value.analyze_all = AsyncMock(
            side_effect=AssertionError("analysis must not run inline in the HTTP handler (#793)")
        )
        resp = await route_client.post("/channels/filter/analyze", follow_redirects=False)

    assert resp.status_code == 303
    assert "msg=filter_analyze_queued" in resp.headers["location"]

    task = await db.repos.tasks.get_active_filter_analyze_task()
    assert task is not None


@pytest.mark.anyio
async def test_analyze_rejects_when_task_active(route_client, db):
    """A second POST while a filter-analyze task is pending/running is rejected (#793)."""
    from src.models import FilterAnalyzeTaskPayload

    await db.repos.tasks.create_filter_analyze_task(FilterAnalyzeTaskPayload())

    resp = await route_client.post("/channels/filter/analyze", follow_redirects=False)

    assert resp.status_code == 303
    assert "error=filter_analyze_running" in resp.headers["location"]


@pytest.mark.anyio
async def test_create_filter_analyze_task_is_atomic(db):
    """INSERT ... WHERE NOT EXISTS: the second create returns None while a task
    is active — no check-then-create race window (review on #823)."""
    from src.models import CollectionTaskStatus, FilterAnalyzeTaskPayload

    first = await db.repos.tasks.create_filter_analyze_task(FilterAnalyzeTaskPayload())
    assert first is not None

    second = await db.repos.tasks.create_filter_analyze_task(FilterAnalyzeTaskPayload())
    assert second is None

    await db.repos.tasks.update_collection_task(first, CollectionTaskStatus.COMPLETED)
    third = await db.repos.tasks.create_filter_analyze_task(FilterAnalyzeTaskPayload())
    assert third is not None


@pytest.mark.anyio
async def test_analyze_status_endpoint_reports_progress_and_result(route_client, db):
    """GET analyze/status reflects the latest filter-analyze task lifecycle (#793)."""
    from src.models import CollectionTaskStatus, FilterAnalyzeTaskPayload

    resp = await route_client.get("/channels/filter/analyze/status")
    assert resp.status_code == 200
    assert resp.json()["status"] is None

    task_id = await db.repos.tasks.create_filter_analyze_task(FilterAnalyzeTaskPayload())
    resp = await route_client.get("/channels/filter/analyze/status")
    assert resp.json()["status"] == "pending"

    await db.repos.tasks.update_collection_task(
        task_id,
        CollectionTaskStatus.COMPLETED,
        messages_collected=3,
        note="analyzed=10 filtered=3 purged=0",
    )
    body = (await route_client.get("/channels/filter/analyze/status")).json()
    assert body["status"] == "completed"
    assert "filtered=3" in body["note"]

    task_id_2 = await db.repos.tasks.create_filter_analyze_task(FilterAnalyzeTaskPayload())
    await db.repos.tasks.update_collection_task(
        task_id_2,
        CollectionTaskStatus.FAILED,
        error="Analysis timed out after 600s",
    )
    body = (await route_client.get("/channels/filter/analyze/status")).json()
    assert body["status"] == "failed"
    assert "timed out" in body["error"]


@pytest.mark.anyio
async def test_has_stats_true_when_no_active_channels(route_client, db):
    """Test has-stats returns true when there are no active channels to inspect."""
    channel = await db.get_channel_by_channel_id(100)
    assert channel is not None and channel.id is not None
    await db.set_channel_active(channel.id, False)

    resp = await route_client.get("/channels/filter/has-stats")

    assert resp.status_code == 200
    assert resp.json() == {"has_stats": True}


@pytest.mark.anyio
async def test_has_stats_false_when_active_channel_lacks_stats(route_client):
    """Test has-stats returns false when any active channel has no stats yet."""
    resp = await route_client.get("/channels/filter/has-stats")

    assert resp.status_code == 200
    assert resp.json() == {"has_stats": False}


@pytest.mark.anyio
async def test_has_stats_true_when_all_active_channels_have_stats(route_client, db):
    """Test has-stats returns true when every active channel already has stats."""
    await db.save_channel_stats(ChannelStats(channel_id=100, subscriber_count=1))
    extra_channel_id = 101
    await _add_channel(db, channel_id=extra_channel_id, title="Has Stats")
    await db.save_channel_stats(ChannelStats(channel_id=extra_channel_id, subscriber_count=2))

    resp = await route_client.get("/channels/filter/has-stats")

    assert resp.status_code == 200
    assert resp.json() == {"has_stats": True}


@pytest.mark.anyio
async def test_apply_missing_snapshot(route_client):
    """Test apply filters without snapshot."""
    resp = await route_client.post("/channels/filter/apply", follow_redirects=False)
    assert resp.status_code == 303
    assert "error=filter_snapshot_required" in resp.headers["location"]


@pytest.mark.anyio
async def test_apply_with_snapshot(route_client):
    """Test apply filters with snapshot."""
    with patch("src.web.filter.handlers.ChannelAnalyzer") as mock_analyzer:
        mock_instance = mock_analyzer.return_value
        mock_instance.apply_filters = AsyncMock(return_value=1)
        resp = await route_client.post(
            "/channels/filter/apply",
            data={"snapshot": "1", "selected": ["100|low_uniqueness"]},
            follow_redirects=False,
        )
        assert resp.status_code == 303
        assert "msg=filter_applied" in resp.headers["location"]


@pytest.mark.anyio
async def test_precheck_redirects(route_client):
    """Test precheck subscriber ratio redirects."""
    with patch("src.web.filter.handlers.ChannelAnalyzer") as mock_analyzer:
        mock_instance = mock_analyzer.return_value
        mock_instance.precheck_subscriber_ratio = AsyncMock(return_value=5)
        resp = await route_client.post("/channels/filter/precheck", follow_redirects=False)
        assert resp.status_code == 303
        assert "msg=precheck_done" in resp.headers["location"]


@pytest.mark.anyio
async def test_reset_redirects(route_client):
    """Test reset filters redirects."""
    with patch("src.web.filter.handlers.ChannelAnalyzer") as mock_analyzer:
        mock_instance = mock_analyzer.return_value
        mock_instance.reset_filters = AsyncMock()
        resp = await route_client.post("/channels/filter/reset", follow_redirects=False)
        assert resp.status_code == 303
        assert "msg=filter_reset" in resp.headers["location"]


@pytest.mark.anyio
async def test_purge_messages_not_filtered(route_client):
    """Test purge messages for non-filtered channel."""
    resp = await route_client.post("/channels/900/purge-messages", follow_redirects=False)
    assert resp.status_code == 303
    assert "error=not_filtered" in resp.headers["location"]


@pytest.mark.anyio
async def test_purge_messages_success(route_client, db):
    """Test purge messages for filtered channel."""
    pk = await _add_filtered_channel(db, channel_id=950, title="Purge Msgs")
    channel = await db.get_channel_by_pk(pk)

    with patch.object(db, "delete_messages_for_channel", AsyncMock(return_value=10)):
        resp = await route_client.post(
            f"/channels/{channel.channel_id}/purge-messages",
            follow_redirects=False,
        )
        assert resp.status_code == 303
        assert "msg=purged" in resp.headers["location"]


@pytest.mark.anyio
async def test_filter_toggle_not_found(route_client):
    """Test filter toggle with non-existent channel."""
    resp = await route_client.post("/channels/999999/filter-toggle", follow_redirects=False)
    assert resp.status_code == 303
    assert "msg=channel_not_found" in resp.headers["location"]


@pytest.mark.anyio
async def test_filter_toggle_success(route_client, db):
    """Test filter toggle success."""
    pk = await _add_channel(db, channel_id=960, title="Toggle Filter")
    resp = await route_client.post(f"/channels/{pk}/filter-toggle", follow_redirects=False)
    assert resp.status_code == 303
    assert "msg=filter_toggled" in resp.headers["location"]


# === Additional tests ===


@pytest.mark.anyio
async def test_parse_snapshot_valid(route_client, db):
    """Test apply filters with valid snapshot parsing."""
    with patch("src.web.filter.handlers.ChannelAnalyzer") as mock_analyzer:
        mock_instance = mock_analyzer.return_value
        mock_instance.apply_filters = AsyncMock(return_value=2)
        resp = await route_client.post(
            "/channels/filter/apply",
            data={
                "snapshot": "1",
                "selected": ["100|low_uniqueness", "200|low_subscriber_ratio,cross_channel_spam"],
            },
            follow_redirects=False,
        )
        assert resp.status_code == 303
        assert "msg=filter_applied" in resp.headers["location"]


@pytest.mark.anyio
async def test_parse_snapshot_dedupes_by_channel_id(route_client, db):
    """Test snapshot parsing dedupes by channel_id."""
    with patch("src.web.filter.handlers.ChannelAnalyzer") as mock_analyzer:
        mock_instance = mock_analyzer.return_value
        mock_instance.apply_filters = AsyncMock(return_value=1)
        resp = await route_client.post(
            "/channels/filter/apply",
            data={
                "snapshot": "1",
                "selected": ["100|low_uniqueness", "100|cross_channel_spam"],
            },
            follow_redirects=False,
        )
        assert resp.status_code == 303


@pytest.mark.anyio
async def test_parse_snapshot_invalid_channel_id(route_client, db):
    """Test snapshot parsing with invalid channel_id."""
    with patch("src.web.filter.handlers.ChannelAnalyzer") as mock_analyzer:
        mock_instance = mock_analyzer.return_value
        mock_instance.apply_filters = AsyncMock(return_value=0)
        resp = await route_client.post(
            "/channels/filter/apply",
            data={
                "snapshot": "1",
                "selected": ["abc|low_uniqueness"],
            },
            follow_redirects=False,
        )
        assert resp.status_code == 303


@pytest.mark.anyio
async def test_parse_snapshot_no_separator(route_client, db):
    """Test snapshot parsing without separator."""
    with patch("src.web.filter.handlers.ChannelAnalyzer") as mock_analyzer:
        mock_instance = mock_analyzer.return_value
        mock_instance.apply_filters = AsyncMock(return_value=0)
        resp = await route_client.post(
            "/channels/filter/apply",
            data={
                "snapshot": "1",
                "selected": ["100low_uniqueness"],  # No | separator
            },
            follow_redirects=False,
        )
        assert resp.status_code == 303


@pytest.mark.anyio
async def test_parse_snapshot_invalid_flag(route_client, db):
    """Test snapshot parsing with invalid flag."""
    with patch("src.web.filter.handlers.ChannelAnalyzer") as mock_analyzer:
        mock_instance = mock_analyzer.return_value
        mock_instance.apply_filters = AsyncMock(return_value=0)
        resp = await route_client.post(
            "/channels/filter/apply",
            data={
                "snapshot": "1",
                "selected": ["100|invalid_flag"],
            },
            follow_redirects=False,
        )
        assert resp.status_code == 303


@pytest.mark.anyio
async def test_analyze_with_auto_delete(route_client, db):
    """Auto-delete no longer runs inline in the HTTP handler — it moved into
    FilterAnalyzeTaskHandler together with the analysis itself (#793)."""
    await _add_filtered_channel(db, channel_id=3100, title="Auto Delete")
    await db.set_setting("auto_delete_filtered", "1")

    with patch("src.web.filter.handlers.deps.filter_deletion_service") as mock_svc:
        resp = await route_client.post("/channels/filter/analyze", follow_redirects=False)

    assert resp.status_code == 303
    assert "msg=filter_analyze_queued" in resp.headers["location"]
    mock_svc.assert_not_called()


@pytest.mark.anyio
async def test_purge_selected_with_multiple_pks(route_client, db):
    """Test purge selected with multiple PKs."""
    pk1 = await _add_filtered_channel(db, channel_id=3200, title="Purge 1")
    pk2 = await _add_filtered_channel(db, channel_id=3201, title="Purge 2")

    with patch("src.web.filter.handlers.deps.filter_deletion_service") as mock_svc:
        mock_svc.return_value.purge_channels_by_pks = AsyncMock(
            return_value=PurgeResult(purged_count=2)
        )
        resp = await route_client.post(
            "/channels/filter/purge-selected",
            data={"pks": [str(pk1), str(pk2), "invalid"]},  # Invalid PK is skipped
            follow_redirects=False,
        )
        assert resp.status_code == 303
        assert "msg=purged_selected" in resp.headers["location"]
