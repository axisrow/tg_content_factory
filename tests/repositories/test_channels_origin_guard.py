"""Origin guards for channel decision writers (#1310)."""

from __future__ import annotations

from src.models import Channel


async def test_channel_active_origin_guard(channels_repo):
    pk = await channels_repo.add_channel(Channel(channel_id=101, title="Active"))

    assert await channels_repo.set_channel_active(pk, False, origin="human") == 1
    assert await channels_repo.set_channel_active(pk, True, origin="auto") == 0
    channel = await channels_repo.get_channel_by_pk(pk)
    assert channel is not None
    assert channel.is_active is False
    assert channel.active_origin == "human"
    history = await channels_repo._database.repos.decisions.history(
        "channel", 101, field="is_active"
    )
    assert [decision.origin for decision in history] == ["auto", "human"]


async def test_channel_upsert_preserves_human_active_decision(channels_repo):
    pk = await channels_repo.add_channel(Channel(channel_id=109, title="Protected"))

    assert await channels_repo.set_channel_active(pk, False, origin="human") == 1
    await channels_repo.add_channel(Channel(channel_id=109, title="Refreshed", is_active=True))

    channel = await channels_repo.get_channel_by_pk(pk)
    assert channel is not None
    assert channel.title == "Refreshed"
    assert channel.is_active is False
    assert channel.active_origin == "human"


async def test_channel_upsert_persists_and_preserves_human_filter_provenance(channels_repo):
    pk = await channels_repo.add_channel(
        Channel(channel_id=110, title="Private", filtered_origin="human")
    )

    channel = await channels_repo.get_channel_by_pk(pk)
    assert channel is not None
    assert channel.filtered_origin == "human"

    await channels_repo.add_channel(
        Channel(channel_id=110, title="Refreshed", filtered_origin="auto")
    )

    channel = await channels_repo.get_channel_by_pk(pk)
    assert channel is not None
    assert channel.filtered_origin == "human"


async def test_channel_filtered_origin_guard(channels_repo):
    pk = await channels_repo.add_channel(Channel(channel_id=102, title="Filtered"))

    assert await channels_repo.set_channel_filtered(pk, True, origin="human") == 1
    assert await channels_repo.set_channel_filtered(pk, False, origin="auto") == 0
    channel = await channels_repo.get_channel_by_pk(pk)
    assert channel is not None
    assert channel.is_filtered is True
    assert channel.filtered_origin == "human"


async def test_human_can_change_human_decision_and_auto_can_change_auto(channels_repo):
    pk = await channels_repo.add_channel(Channel(channel_id=103, title="Revisable"))

    assert await channels_repo.set_channel_active(pk, False) == 1
    assert await channels_repo.set_channel_active(pk, True) == 1
    assert await channels_repo.set_channel_active(pk, False, origin="human") == 1
    assert await channels_repo.set_channel_active(pk, True, origin="human") == 1


async def test_set_filtered_bulk_returns_applied_and_suppressed_counts(channels_repo):
    human_pk = await channels_repo.add_channel(Channel(channel_id=104, title="Human"))
    auto_pk = await channels_repo.add_channel(Channel(channel_id=105, title="Auto"))
    assert await channels_repo.set_channel_filtered(human_pk, True, origin="human") == 1

    result = await channels_repo.set_filtered_bulk(
        [(104, "heuristic"), (105, "heuristic")]
    )
    assert result == (1, 1)
    assert result.applied == 1
    assert result.suppressed == 1
    human = await channels_repo.get_channel_by_pk(human_pk)
    auto = await channels_repo.get_channel_by_pk(auto_pk)
    assert human is not None and human.filter_flags == "manual"
    assert auto is not None and auto.filter_flags == "heuristic"


async def test_reset_filter_writers_respect_origin(channels_repo):
    human_pk = await channels_repo.add_channel(Channel(channel_id=106, title="Human"))
    auto_pk = await channels_repo.add_channel(Channel(channel_id=107, title="Auto"))
    await channels_repo.set_channel_filtered(human_pk, True, origin="human")
    await channels_repo.set_channel_filtered(auto_pk, True)

    reset_result = await channels_repo.reset_all_filters()
    assert reset_result == (1, 1)
    human = await channels_repo.get_channel_by_pk(human_pk)
    auto = await channels_repo.get_channel_by_pk(auto_pk)
    assert human is not None and human.is_filtered is True
    assert auto is not None and auto.is_filtered is False

    assert await channels_repo.reset_filters_for_pks([human_pk], origin="human") == (1, 0)
    human = await channels_repo.get_channel_by_pk(human_pk)
    assert human is not None and human.is_filtered is False


async def test_reset_all_filters_does_not_badge_human_unfiltered_rows(channels_repo):
    pk = await channels_repo.add_channel(Channel(channel_id=108, title="Already clear"))
    await channels_repo.set_channel_filtered(pk, False, origin="human")
    before = await channels_repo._database.repos.decisions.history(
        "channel", 108, field="is_filtered"
    )

    assert await channels_repo.reset_all_filters() == (0, 0)
    after = await channels_repo._database.repos.decisions.history(
        "channel", 108, field="is_filtered"
    )
    assert len(after) == len(before) == 1
