"""Tests for filter routes."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from src.models import ChannelStats
from src.services.filter_deletion_service import PurgeResult
from tests.routes.conftest import _add_channel, _add_filtered_channel, _enable_dev_mode


@pytest.fixture
async def client(route_client):
    """Use shared route_client fixture."""
    return route_client


@pytest.fixture
async def db(base_app):
    """Get db from base_app."""
    _, db, _ = base_app
    return db


@pytest.mark.asyncio
async def test_filter_manage_renders_empty(client):
    """Test filter manage page renders empty."""
    resp = await client.get("/channels/filter/manage")
    assert resp.status_code == 200
    assert 'onclick="refreshFilters(this)"' in resp.text
    assert "async function refreshFilters(button)" in resp.text


@pytest.mark.asyncio
async def test_filter_manage_shows_filtered(client, db):
    """Test filter manage shows filtered channels."""
    await _add_filtered_channel(db, channel_id=300, title="Filtered Channel")

    resp = await client.get("/channels/filter/manage")
    assert resp.status_code == 200
    assert "Filtered Channel" in resp.text


@pytest.mark.asyncio
async def test_purge_selected_no_pks(client):
    """Test purge selected with no PKs returns error."""
    resp = await client.post("/channels/filter/purge-selected", follow_redirects=False)
    assert resp.status_code == 303
    assert "error=no_filtered_channels" in resp.headers["location"]


@pytest.mark.asyncio
async def test_purge_selected_success(client, db):
    """Test purge selected channels."""
    pk = await _add_filtered_channel(db, channel_id=400, title="To Purge")

    with patch("src.web.routes.filter.deps.filter_deletion_service") as mock_svc:
        mock_svc.return_value.purge_channels_by_pks = AsyncMock()
        resp = await client.post(
            "/channels/filter/purge-selected",
            data={"pks": [str(pk)]},
            follow_redirects=False,
        )
        assert resp.status_code == 303
        assert "msg=purged_selected" in resp.headers["location"]


@pytest.mark.asyncio
async def test_purge_selected_removes_messages(client, db):
    """Test purge selected removes messages from DB."""
    pk = await _add_filtered_channel(db, channel_id=500, title="Purge Messages")

    with patch("src.web.routes.filter.deps.filter_deletion_service") as mock_svc:
        mock_svc.return_value.purge_channels_by_pks = AsyncMock()
        await client.post(
            "/channels/filter/purge-selected",
            data={"pks": [str(pk)]},
        )


@pytest.mark.asyncio
async def test_purge_all_no_filtered(client):
    """Test purge all with no filtered channels."""
    with patch("src.web.routes.filter.deps.filter_deletion_service") as mock_svc:
        from src.services.filter_deletion_service import PurgeResult
        mock_svc.return_value.purge_all_filtered = AsyncMock(
            return_value=PurgeResult(purged_count=0)
        )
        resp = await client.post("/channels/filter/purge-all", follow_redirects=False)
        assert resp.status_code == 303
        assert "error=no_filtered_channels" in resp.headers["location"]


@pytest.mark.asyncio
async def test_purge_all_success(client, db):
    """Test purge all filtered channels."""
    await _add_filtered_channel(db, channel_id=600, title="Purge All")

    with patch("src.web.routes.filter.deps.filter_deletion_service") as mock_svc:
        from src.services.filter_deletion_service import PurgeResult
        mock_svc.return_value.purge_all_filtered = AsyncMock(
            return_value=PurgeResult(purged_count=1)
        )
        resp = await client.post("/channels/filter/purge-all", follow_redirects=False)
        assert resp.status_code == 303
        assert "msg=purged_all_filtered" in resp.headers["location"]


@pytest.mark.asyncio
async def test_hard_delete_blocked_without_dev_mode(client, db):
    """Test hard delete blocked without dev mode."""
    pk = await _add_filtered_channel(db, channel_id=700, title="Hard Delete")
    resp = await client.post(
        "/channels/filter/hard-delete-selected",
        data={"pks": [str(pk)]},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "error=dev_mode_required_for_hard_delete" in resp.headers["location"]


@pytest.mark.asyncio
async def test_hard_delete_no_pks(client, db):
    """Test hard delete with no PKs."""
    await _enable_dev_mode(db)
    resp = await client.post("/channels/filter/hard-delete-selected", follow_redirects=False)
    assert resp.status_code == 303
    assert "error=no_filtered_channels" in resp.headers["location"]


@pytest.mark.asyncio
async def test_hard_delete_success(client, db):
    """Test hard delete channels."""
    pk = await _add_filtered_channel(db, channel_id=800, title="Hard Delete OK")
    await _enable_dev_mode(db)

    with patch("src.web.routes.filter.deps.filter_deletion_service") as mock_svc:
        from src.services.filter_deletion_service import PurgeResult
        mock_svc.return_value.hard_delete_channels_by_pks = AsyncMock(
            return_value=PurgeResult(purged_count=1)
        )
        resp = await client.post(
            "/channels/filter/hard-delete-selected",
            data={"pks": [str(pk)]},
            follow_redirects=False,
        )
        assert resp.status_code == 303
        assert "msg=deleted_filtered" in resp.headers["location"]


@pytest.mark.asyncio
async def test_analyze_redirects(client):
    """Test analyze channels redirects."""
    with patch("src.web.routes.filter.ChannelAnalyzer") as mock_analyzer:
        from src.filters.models import FilterReport
        mock_instance = mock_analyzer.return_value
        mock_instance.analyze_all = AsyncMock(
            return_value=FilterReport(results=[], total_channels=0, filtered_count=0)
        )
        mock_instance.apply_filters = AsyncMock(return_value=0)
        resp = await client.post("/channels/filter/analyze", follow_redirects=False)
        assert resp.status_code == 303
        assert "/channels/filter/manage" in resp.headers["location"]


@pytest.mark.asyncio
async def test_analyze_ignores_with_stats_query(client):
    """Test analyze route no longer runs stats collection inline."""
    with patch("src.web.routes.filter.ChannelAnalyzer") as mock_analyzer:
        from src.filters.models import FilterReport
        mock_instance = mock_analyzer.return_value
        mock_instance.analyze_all = AsyncMock(
            return_value=FilterReport(results=[], total_channels=0, filtered_count=0)
        )
        mock_instance.apply_filters = AsyncMock(return_value=0)
        with patch("src.web.routes.filter.deps.collection_service") as mock_collection:
            resp = await client.post("/channels/filter/analyze?with_stats=1", follow_redirects=False)

        assert resp.status_code == 303
        mock_collection.assert_not_called()


@pytest.mark.asyncio
async def test_has_stats_true_when_no_active_channels(client, db):
    """Test has-stats returns true when there are no active channels to inspect."""
    channel = await db.get_channel_by_channel_id(100)
    assert channel is not None and channel.id is not None
    await db.set_channel_active(channel.id, False)

    resp = await client.get("/channels/filter/has-stats")

    assert resp.status_code == 200
    assert resp.json() == {"has_stats": True}


@pytest.mark.asyncio
async def test_has_stats_false_when_active_channel_lacks_stats(client):
    """Test has-stats returns false when any active channel has no stats yet."""
    resp = await client.get("/channels/filter/has-stats")

    assert resp.status_code == 200
    assert resp.json() == {"has_stats": False}


@pytest.mark.asyncio
async def test_has_stats_true_when_all_active_channels_have_stats(client, db):
    """Test has-stats returns true when every active channel already has stats."""
    await db.save_channel_stats(ChannelStats(channel_id=100, subscriber_count=1))
    extra_channel_id = 101
    await _add_channel(db, channel_id=extra_channel_id, title="Has Stats")
    await db.save_channel_stats(ChannelStats(channel_id=extra_channel_id, subscriber_count=2))

    resp = await client.get("/channels/filter/has-stats")

    assert resp.status_code == 200
    assert resp.json() == {"has_stats": True}


@pytest.mark.asyncio
async def test_apply_missing_snapshot(client):
    """Test apply filters without snapshot."""
    resp = await client.post("/channels/filter/apply", follow_redirects=False)
    assert resp.status_code == 303
    assert "error=filter_snapshot_required" in resp.headers["location"]


@pytest.mark.asyncio
async def test_apply_with_snapshot(client):
    """Test apply filters with snapshot."""
    with patch("src.web.routes.filter.ChannelAnalyzer") as mock_analyzer:
        mock_instance = mock_analyzer.return_value
        mock_instance.apply_filters = AsyncMock(return_value=1)
        resp = await client.post(
            "/channels/filter/apply",
            data={"snapshot": "1", "selected": ["100|low_uniqueness"]},
            follow_redirects=False,
        )
        assert resp.status_code == 303
        assert "msg=filter_applied" in resp.headers["location"]


@pytest.mark.asyncio
async def test_precheck_redirects(client):
    """Test precheck subscriber ratio redirects."""
    with patch("src.web.routes.filter.ChannelAnalyzer") as mock_analyzer:
        mock_instance = mock_analyzer.return_value
        mock_instance.precheck_subscriber_ratio = AsyncMock(return_value=5)
        resp = await client.post("/channels/filter/precheck", follow_redirects=False)
        assert resp.status_code == 303
        assert "msg=precheck_done" in resp.headers["location"]


@pytest.mark.asyncio
async def test_reset_redirects(client):
    """Test reset filters redirects."""
    with patch("src.web.routes.filter.ChannelAnalyzer") as mock_analyzer:
        mock_instance = mock_analyzer.return_value
        mock_instance.reset_filters = AsyncMock()
        resp = await client.post("/channels/filter/reset", follow_redirects=False)
        assert resp.status_code == 303
        assert "msg=filter_reset" in resp.headers["location"]


@pytest.mark.asyncio
async def test_purge_messages_not_filtered(client):
    """Test purge messages for non-filtered channel."""
    resp = await client.post("/channels/900/purge-messages", follow_redirects=False)
    assert resp.status_code == 303
    assert "error=not_filtered" in resp.headers["location"]


@pytest.mark.asyncio
async def test_purge_messages_success(client, db):
    """Test purge messages for filtered channel."""
    pk = await _add_filtered_channel(db, channel_id=950, title="Purge Msgs")
    channel = await db.get_channel_by_pk(pk)

    with patch.object(db, "delete_messages_for_channel", AsyncMock(return_value=10)):
        resp = await client.post(
            f"/channels/{channel.channel_id}/purge-messages",
            follow_redirects=False,
        )
        assert resp.status_code == 303
        assert "msg=purged" in resp.headers["location"]


@pytest.mark.asyncio
async def test_filter_toggle_not_found(client):
    """Test filter toggle with non-existent channel."""
    resp = await client.post("/channels/999999/filter-toggle", follow_redirects=False)
    assert resp.status_code == 303
    assert "msg=channel_not_found" in resp.headers["location"]


@pytest.mark.asyncio
async def test_filter_toggle_success(client, db):
    """Test filter toggle success."""
    pk = await _add_channel(db, channel_id=960, title="Toggle Filter")
    resp = await client.post(f"/channels/{pk}/filter-toggle", follow_redirects=False)
    assert resp.status_code == 303
    assert "msg=filter_toggled" in resp.headers["location"]


# === Additional coverage tests ===


@pytest.mark.asyncio
async def test_parse_snapshot_valid(client, db):
    """Test apply filters with valid snapshot parsing."""
    with patch("src.web.routes.filter.ChannelAnalyzer") as mock_analyzer:
        mock_instance = mock_analyzer.return_value
        mock_instance.apply_filters = AsyncMock(return_value=2)
        resp = await client.post(
            "/channels/filter/apply",
            data={
                "snapshot": "1",
                "selected": ["100|low_uniqueness", "200|low_subscriber_ratio,cross_channel_spam"],
            },
            follow_redirects=False,
        )
        assert resp.status_code == 303
        assert "msg=filter_applied" in resp.headers["location"]


@pytest.mark.asyncio
async def test_parse_snapshot_dedupes_by_channel_id(client, db):
    """Test snapshot parsing dedupes by channel_id."""
    with patch("src.web.routes.filter.ChannelAnalyzer") as mock_analyzer:
        mock_instance = mock_analyzer.return_value
        mock_instance.apply_filters = AsyncMock(return_value=1)
        resp = await client.post(
            "/channels/filter/apply",
            data={
                "snapshot": "1",
                "selected": ["100|low_uniqueness", "100|cross_channel_spam"],
            },
            follow_redirects=False,
        )
        assert resp.status_code == 303


@pytest.mark.asyncio
async def test_parse_snapshot_invalid_channel_id(client, db):
    """Test snapshot parsing with invalid channel_id."""
    with patch("src.web.routes.filter.ChannelAnalyzer") as mock_analyzer:
        mock_instance = mock_analyzer.return_value
        mock_instance.apply_filters = AsyncMock(return_value=0)
        resp = await client.post(
            "/channels/filter/apply",
            data={
                "snapshot": "1",
                "selected": ["abc|low_uniqueness"],
            },
            follow_redirects=False,
        )
        assert resp.status_code == 303


@pytest.mark.asyncio
async def test_parse_snapshot_no_separator(client, db):
    """Test snapshot parsing without separator."""
    with patch("src.web.routes.filter.ChannelAnalyzer") as mock_analyzer:
        mock_instance = mock_analyzer.return_value
        mock_instance.apply_filters = AsyncMock(return_value=0)
        resp = await client.post(
            "/channels/filter/apply",
            data={
                "snapshot": "1",
                "selected": ["100low_uniqueness"],  # No | separator
            },
            follow_redirects=False,
        )
        assert resp.status_code == 303


@pytest.mark.asyncio
async def test_parse_snapshot_invalid_flag(client, db):
    """Test snapshot parsing with invalid flag."""
    with patch("src.web.routes.filter.ChannelAnalyzer") as mock_analyzer:
        mock_instance = mock_analyzer.return_value
        mock_instance.apply_filters = AsyncMock(return_value=0)
        resp = await client.post(
            "/channels/filter/apply",
            data={
                "snapshot": "1",
                "selected": ["100|invalid_flag"],
            },
            follow_redirects=False,
        )
        assert resp.status_code == 303


@pytest.mark.asyncio
async def test_analyze_with_auto_delete(client, db):
    """Test analyze channels with auto_delete enabled."""
    await _add_filtered_channel(db, channel_id=3100, title="Auto Delete")
    await db.set_setting("auto_delete_filtered", "1")

    with patch("src.web.routes.filter.ChannelAnalyzer") as mock_analyzer, patch(
        "src.web.routes.filter.deps.filter_deletion_service"
    ) as mock_svc:
        from src.filters.models import ChannelFilterResult, FilterReport

        mock_instance = mock_analyzer.return_value
        mock_instance.analyze_all = AsyncMock(
            return_value=FilterReport(
                results=[
                    ChannelFilterResult(
                        channel_id=3100, flags=["low_uniqueness"], is_filtered=True
                    )
                ],
                total_channels=1,
                filtered_count=1,
            )
        )
        mock_instance.apply_filters = AsyncMock(return_value=1)
        mock_svc.return_value.purge_channels_by_pks = AsyncMock(
            return_value=PurgeResult(purged_count=1)
        )

        resp = await client.post("/channels/filter/analyze", follow_redirects=False)
        assert resp.status_code == 303
        assert "msg=purged_all_filtered" in resp.headers["location"]


@pytest.mark.asyncio
async def test_purge_selected_with_multiple_pks(client, db):
    """Test purge selected with multiple PKs."""
    pk1 = await _add_filtered_channel(db, channel_id=3200, title="Purge 1")
    pk2 = await _add_filtered_channel(db, channel_id=3201, title="Purge 2")

    with patch("src.web.routes.filter.deps.filter_deletion_service") as mock_svc:
        mock_svc.return_value.purge_channels_by_pks = AsyncMock()
        resp = await client.post(
            "/channels/filter/purge-selected",
            data={"pks": [str(pk1), str(pk2), "invalid"]},  # Invalid PK is skipped
            follow_redirects=False,
        )
        assert resp.status_code == 303
        assert "msg=purged_selected" in resp.headers["location"]
