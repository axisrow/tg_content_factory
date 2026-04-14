from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

from src.models import Channel, CollectionTask
from src.services.channel_service import ChannelService

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_channel(
    pk: int = 1,
    channel_id: int = -1001,
    title: str = "Test",
    is_active: bool = True,
    channel_type: str | None = "channel",
) -> Channel:
    return Channel(
        id=pk,
        channel_id=channel_id,
        title=title,
        is_active=is_active,
        channel_type=channel_type,
    )


def _make_bundle() -> MagicMock:
    """Create a mock ChannelBundle with all async methods pre-configured."""
    bundle = MagicMock()
    bundle.list_channels = AsyncMock(return_value=[])
    bundle.list_channels_with_counts = AsyncMock(return_value=[])
    bundle.get_latest_and_previous_stats = AsyncMock(return_value=({}, {}))
    bundle.add_channel = AsyncMock(return_value=1)
    bundle.get_by_pk = AsyncMock(return_value=None)
    bundle.set_active = AsyncMock(return_value=None)
    bundle.delete_channel = AsyncMock(return_value=None)
    bundle.get_active_collection_tasks_for_channel = AsyncMock(return_value=[])
    bundle.update_channel_full_meta = AsyncMock(return_value=None)
    return bundle


def _make_pool() -> MagicMock:
    """Create a mock ClientPool with all async methods pre-configured."""
    pool = MagicMock()
    pool.resolve_channel = AsyncMock(return_value=None)
    pool.fetch_channel_meta = AsyncMock(return_value=None)
    pool.get_dialogs = AsyncMock(return_value=[])
    pool.get_dialogs_for_phone = AsyncMock(return_value=[])
    return pool


def _make_service(
    bundle: MagicMock | None = None,
    pool: MagicMock | None = None,
    queue: MagicMock | None = None,
) -> ChannelService:
    return ChannelService(
        bundle or _make_bundle(),
        pool or _make_pool(),
        queue,
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestListForPage:
    async def test_returns_tuple_of_channels_stats_dicts(self):
        channels = [_make_channel(pk=1), _make_channel(pk=2, channel_id=-1002)]
        latest_stats = {-1001: MagicMock(subscriber_count=500)}
        prev_subs = {-1001: 400, -1002: 100}

        bundle = _make_bundle()
        bundle.list_channels_with_counts = AsyncMock(return_value=channels)
        bundle.get_latest_and_previous_stats = AsyncMock(
            return_value=(latest_stats, prev_subs)
        )

        service = _make_service(bundle=bundle)
        result = await service.list_for_page(include_filtered=True)

        assert result == (channels, latest_stats, prev_subs)
        bundle.list_channels_with_counts.assert_awaited_once_with(include_filtered=True)
        bundle.get_latest_and_previous_stats.assert_awaited_once()


class TestAddByIdentifier:
    async def test_success(self):
        resolve_info = {
            "channel_id": -100123,
            "title": "My Channel",
            "username": "mychannel",
            "channel_type": "channel",
            "deactivate": False,
            "created_at": None,
        }
        meta = {
            "about": "Channel description",
            "linked_chat_id": -200456,
            "has_comments": True,
        }

        pool = _make_pool()
        pool.resolve_channel = AsyncMock(return_value=resolve_info)
        pool.fetch_channel_meta = AsyncMock(return_value=meta)

        bundle = _make_bundle()
        bundle.add_channel = AsyncMock(return_value=42)

        service = _make_service(bundle=bundle, pool=pool)
        ok = await service.add_by_identifier("@mychannel")

        assert ok is True
        pool.resolve_channel.assert_awaited_once_with("@mychannel")
        pool.fetch_channel_meta.assert_awaited_once_with(-100123, "channel")
        bundle.add_channel.assert_awaited_once()

        # Verify the Channel object passed to add_channel
        ch: Channel = bundle.add_channel.call_args[0][0]
        assert ch.channel_id == -100123
        assert ch.title == "My Channel"
        assert ch.username == "mychannel"
        assert ch.about == "Channel description"
        assert ch.linked_chat_id == -200456
        assert ch.has_comments is True
        assert ch.is_active is True

    async def test_returns_false_when_resolve_fails(self):
        pool = _make_pool()
        pool.resolve_channel = AsyncMock(return_value=None)

        service = _make_service(pool=pool)
        ok = await service.add_by_identifier("@nonexistent")

        assert ok is False
        pool.fetch_channel_meta.assert_not_awaited()

    async def test_strips_whitespace_from_identifier(self):
        pool = _make_pool()
        pool.resolve_channel = AsyncMock(
            return_value={
                "channel_id": -100,
                "title": "T",
                "username": None,
                "channel_type": None,
                "deactivate": False,
                "created_at": None,
            }
        )
        pool.fetch_channel_meta = AsyncMock(return_value=None)

        service = _make_service(pool=pool)
        await service.add_by_identifier("  @chan  ")

        pool.resolve_channel.assert_awaited_once_with("@chan")


class TestGetDialogsWithAddedFlags:
    async def test_marks_existing_channels(self):
        existing = [_make_channel(pk=1, channel_id=-100), _make_channel(pk=2, channel_id=-200)]
        dialogs = [
            {"channel_id": -100, "title": "A"},
            {"channel_id": -200, "title": "B"},
            {"channel_id": -300, "title": "C"},
        ]

        bundle = _make_bundle()
        bundle.list_channels = AsyncMock(return_value=existing)

        pool = _make_pool()
        pool.get_dialogs = AsyncMock(return_value=dialogs)

        service = _make_service(bundle=bundle, pool=pool)
        result = await service.get_dialogs_with_added_flags()

        assert len(result) == 3
        assert result[0]["already_added"] is True
        assert result[1]["already_added"] is True
        assert result[2]["already_added"] is False


class TestAddBulkByDialogIds:
    async def test_adds_matching_dialogs(self):
        dialogs = [
            {
                "channel_id": -100,
                "title": "ChA",
                "username": "cha",
                "channel_type": "channel",
                "deactivate": False,
                "created_at": None,
            },
            {
                "channel_id": -200,
                "title": "ChB",
                "username": "chb",
                "channel_type": "channel",
                "deactivate": False,
                "created_at": None,
            },
        ]

        pool = _make_pool()
        pool.get_dialogs = AsyncMock(return_value=dialogs)

        bundle = _make_bundle()
        bundle.add_channel = AsyncMock(return_value=1)

        service = _make_service(bundle=bundle, pool=pool)
        await service.add_bulk_by_dialog_ids(["-100", "-200"])

        assert bundle.add_channel.await_count == 2
        ch_a: Channel = bundle.add_channel.call_args_list[0][0][0]
        assert ch_a.channel_id == -100
        assert ch_a.title == "ChA"

    async def test_skips_unknown_ids(self):
        pool = _make_pool()
        pool.get_dialogs = AsyncMock(return_value=[
            {
                "channel_id": -100,
                "title": "Only",
                "username": None,
                "channel_type": None,
                "deactivate": False,
                "created_at": None,
            },
        ])

        bundle = _make_bundle()

        service = _make_service(bundle=bundle, pool=pool)
        await service.add_bulk_by_dialog_ids(["-999"])

        bundle.add_channel.assert_not_awaited()


class TestToggle:
    async def test_activates_inactive_channel(self):
        ch = _make_channel(pk=5, is_active=False)
        bundle = _make_bundle()
        bundle.get_by_pk = AsyncMock(return_value=ch)

        service = _make_service(bundle=bundle)
        await service.toggle(5)

        bundle.set_active.assert_awaited_once_with(5, True)

    async def test_deactivates_active_channel(self):
        ch = _make_channel(pk=7, is_active=True)
        bundle = _make_bundle()
        bundle.get_by_pk = AsyncMock(return_value=ch)

        service = _make_service(bundle=bundle)
        await service.toggle(7)

        bundle.set_active.assert_awaited_once_with(7, False)

    async def test_does_nothing_if_channel_not_found(self):
        bundle = _make_bundle()
        bundle.get_by_pk = AsyncMock(return_value=None)

        service = _make_service(bundle=bundle)
        await service.toggle(99)

        bundle.set_active.assert_not_awaited()


class TestDelete:
    async def test_cancels_tasks_and_removes_channel(self):
        ch = _make_channel(pk=3, channel_id=-555)
        task = CollectionTask(id=10, channel_id=-555)
        queue = MagicMock()
        queue.cancel_task = AsyncMock()

        bundle = _make_bundle()
        bundle.get_by_pk = AsyncMock(return_value=ch)
        bundle.get_active_collection_tasks_for_channel = AsyncMock(return_value=[task])

        service = _make_service(bundle=bundle, queue=queue)
        await service.delete(3)

        queue.cancel_task.assert_awaited_once_with(10, note="Канал удалён пользователем.")
        bundle.delete_channel.assert_awaited_once_with(3)

    async def test_no_queue_does_not_raise(self):
        ch = _make_channel(pk=3, channel_id=-555)
        task = CollectionTask(id=10, channel_id=-555)

        bundle = _make_bundle()
        bundle.get_by_pk = AsyncMock(return_value=ch)
        bundle.get_active_collection_tasks_for_channel = AsyncMock(return_value=[task])

        service = _make_service(bundle=bundle, queue=None)
        # Should not raise — queue is None so task cancellation is skipped
        await service.delete(3)

        bundle.delete_channel.assert_awaited_once_with(3)

    async def test_deletes_even_if_channel_not_found(self):
        bundle = _make_bundle()
        bundle.get_by_pk = AsyncMock(return_value=None)

        service = _make_service(bundle=bundle)
        await service.delete(42)

        bundle.delete_channel.assert_awaited_once_with(42)


class TestGetByPk:
    async def test_delegates_to_bundle(self):
        ch = _make_channel(pk=1)
        bundle = _make_bundle()
        bundle.get_by_pk = AsyncMock(return_value=ch)

        service = _make_service(bundle=bundle)
        result = await service.get_by_pk(1)

        assert result is ch
        bundle.get_by_pk.assert_awaited_once_with(1)

    async def test_returns_none_when_not_found(self):
        bundle = _make_bundle()
        bundle.get_by_pk = AsyncMock(return_value=None)

        service = _make_service(bundle=bundle)
        result = await service.get_by_pk(999)

        assert result is None


class TestRefreshChannelMeta:
    async def test_success(self):
        ch = _make_channel(pk=1, channel_id=-100, channel_type="channel")
        meta = {"about": "New about", "linked_chat_id": -999, "has_comments": False}

        bundle = _make_bundle()
        bundle.get_by_pk = AsyncMock(return_value=ch)

        pool = _make_pool()
        pool.fetch_channel_meta = AsyncMock(return_value=meta)

        service = _make_service(bundle=bundle, pool=pool)
        ok = await service.refresh_channel_meta(1)

        assert ok is True
        pool.fetch_channel_meta.assert_awaited_once_with(-100, "channel")
        bundle.update_channel_full_meta.assert_awaited_once_with(
            -100, about="New about", linked_chat_id=-999, has_comments=False
        )

    async def test_returns_false_when_channel_not_found(self):
        bundle = _make_bundle()
        bundle.get_by_pk = AsyncMock(return_value=None)

        service = _make_service(bundle=bundle)
        ok = await service.refresh_channel_meta(99)

        assert ok is False

    async def test_returns_false_when_meta_is_none(self):
        ch = _make_channel(pk=1, channel_id=-100)

        bundle = _make_bundle()
        bundle.get_by_pk = AsyncMock(return_value=ch)

        pool = _make_pool()
        pool.fetch_channel_meta = AsyncMock(return_value=None)

        service = _make_service(bundle=bundle, pool=pool)
        ok = await service.refresh_channel_meta(1)

        assert ok is False
        bundle.update_channel_full_meta.assert_not_awaited()


class TestRefreshAllChannelMeta:
    async def test_mixed_results(self):
        ch_ok = _make_channel(pk=1, channel_id=-100)
        ch_fail = _make_channel(pk=2, channel_id=-200)

        bundle = _make_bundle()
        bundle.list_channels = AsyncMock(return_value=[ch_ok, ch_fail])

        pool = _make_pool()

        service = _make_service(bundle=bundle, pool=pool)

        # Make refresh_channel_meta succeed for pk=1, fail for pk=2
        async def fake_refresh(pk: int) -> bool:
            return pk == 1

        with patch.object(service, "refresh_channel_meta", side_effect=fake_refresh):
            ok, failed = await service.refresh_all_channel_meta()

        assert ok == 1
        assert failed == 1
        bundle.list_channels.assert_awaited_once_with(active_only=True)

    async def test_all_succeed(self):
        ch1 = _make_channel(pk=1)
        ch2 = _make_channel(pk=2)

        bundle = _make_bundle()
        bundle.list_channels = AsyncMock(return_value=[ch1, ch2])

        service = _make_service(bundle=bundle)

        with patch.object(service, "refresh_channel_meta", new=AsyncMock(return_value=True)):
            ok, failed = await service.refresh_all_channel_meta()

        assert ok == 2
        assert failed == 0


class TestGetMyDialogs:
    async def test_marks_already_added(self):
        existing = [_make_channel(pk=1, channel_id=-100)]
        dialogs = [
            {"channel_id": -100, "title": "Existing"},
            {"channel_id": -200, "title": "New"},
        ]

        bundle = _make_bundle()
        bundle.list_channels = AsyncMock(return_value=existing)

        pool = _make_pool()
        pool.get_dialogs_for_phone = AsyncMock(return_value=dialogs)

        service = _make_service(bundle=bundle, pool=pool)
        result = await service.get_my_dialogs("+1234567890")

        assert len(result) == 2
        assert result[0]["already_added"] is True
        assert result[1]["already_added"] is False
        pool.get_dialogs_for_phone.assert_awaited_once_with(
            "+1234567890", include_dm=True, mode="full", refresh=False
        )

    async def test_refresh_flag_passed_through(self):
        bundle = _make_bundle()
        bundle.list_channels = AsyncMock(return_value=[])

        pool = _make_pool()
        pool.get_dialogs_for_phone = AsyncMock(return_value=[])

        service = _make_service(bundle=bundle, pool=pool)
        await service.get_my_dialogs("+111", refresh=True)

        pool.get_dialogs_for_phone.assert_awaited_once_with(
            "+111", include_dm=True, mode="full", refresh=True
        )
