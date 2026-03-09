"""Integration tests for Bundle layer — every public method exercised against real SQLite."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from src.database.bundles import (
    AccountBundle,
    ChannelBundle,
    CollectionBundle,
    NotificationBundle,
    SchedulerBundle,
    SearchBundle,
)
from src.models import (
    Account,
    Channel,
    ChannelStats,
    CollectionTaskStatus,
    Message,
    NotificationBot,
    SearchQuery,
    StatsAllTaskPayload,
)

NOW = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


def _account(phone: str = "+70001112233", session: str = "sess") -> Account:
    return Account(phone=phone, session_string=session)


def _channel(channel_id: int = 100, title: str = "Test") -> Channel:
    return Channel(channel_id=channel_id, title=title)


def _message(channel_id: int = 100, message_id: int = 1, text: str = "hello") -> Message:
    return Message(
        channel_id=channel_id,
        message_id=message_id,
        text=text,
        date=NOW,
    )


# ---------------------------------------------------------------------------
# AccountBundle
# ---------------------------------------------------------------------------


class TestAccountBundle:
    async def test_from_database(self, db):
        bundle = AccountBundle.from_database(db)
        assert bundle.accounts is not None

    async def test_add_account(self, db):
        b = AccountBundle.from_database(db)
        pk = await b.add_account(_account())
        assert isinstance(pk, int) and pk > 0

    async def test_list_accounts(self, db):
        b = AccountBundle.from_database(db)
        await b.add_account(_account())
        accs = await b.list_accounts()
        assert len(accs) >= 1
        assert accs[0].phone == "+70001112233"

    async def test_set_active(self, db):
        b = AccountBundle.from_database(db)
        pk = await b.add_account(_account())
        await b.set_active(pk, False)
        accs = await b.list_accounts()
        found = next(a for a in accs if a.id == pk)
        assert found.is_active is False

    async def test_delete_account(self, db):
        b = AccountBundle.from_database(db)
        pk = await b.add_account(_account())
        await b.delete_account(pk)
        accs = await b.list_accounts()
        assert all(a.id != pk for a in accs)

    async def test_update_flood(self, db):
        b = AccountBundle.from_database(db)
        await b.add_account(_account())
        until = NOW + timedelta(hours=1)
        await b.update_flood("+70001112233", until)
        accs = await b.list_accounts()
        assert accs[0].flood_wait_until is not None

    async def test_update_premium(self, db):
        b = AccountBundle.from_database(db)
        await b.add_account(_account())
        await b.update_premium("+70001112233", True)
        accs = await b.list_accounts()
        assert accs[0].is_premium is True


# ---------------------------------------------------------------------------
# ChannelBundle
# ---------------------------------------------------------------------------


class TestChannelBundle:
    async def test_from_database(self, db):
        b = ChannelBundle.from_database(db)
        assert b.channels is not None

    async def test_add_channel(self, db):
        b = ChannelBundle.from_database(db)
        pk = await b.add_channel(_channel())
        assert isinstance(pk, int) and pk > 0

    async def test_list_channels(self, db):
        b = ChannelBundle.from_database(db)
        await b.add_channel(_channel())
        chs = await b.list_channels()
        assert len(chs) >= 1

    async def test_list_channels_with_counts(self, db):
        b = ChannelBundle.from_database(db)
        await b.add_channel(_channel())
        chs = await b.list_channels_with_counts()
        assert len(chs) >= 1

    async def test_get_by_pk(self, db):
        b = ChannelBundle.from_database(db)
        pk = await b.add_channel(_channel())
        ch = await b.get_by_pk(pk)
        assert ch is not None and ch.channel_id == 100

    async def test_get_by_channel_id(self, db):
        b = ChannelBundle.from_database(db)
        await b.add_channel(_channel(channel_id=200))
        ch = await b.get_by_channel_id(200)
        assert ch is not None and ch.channel_id == 200

    async def test_set_active(self, db):
        b = ChannelBundle.from_database(db)
        pk = await b.add_channel(_channel())
        await b.set_active(pk, False)
        ch = await b.get_by_pk(pk)
        assert ch is not None and ch.is_active is False

    async def test_set_type(self, db):
        b = ChannelBundle.from_database(db)
        await b.add_channel(_channel(channel_id=300))
        await b.set_type(300, "supergroup")
        ch = await b.get_by_channel_id(300)
        assert ch is not None and ch.channel_type == "supergroup"

    async def test_update_last_id(self, db):
        b = ChannelBundle.from_database(db)
        await b.add_channel(_channel(channel_id=400))
        await b.update_last_id(400, 999)
        ch = await b.get_by_channel_id(400)
        assert ch is not None and ch.last_collected_id == 999

    async def test_update_meta(self, db):
        b = ChannelBundle.from_database(db)
        await b.add_channel(_channel(channel_id=500))
        await b.update_meta(500, username="newuser", title="NewTitle")
        ch = await b.get_by_channel_id(500)
        assert ch is not None
        assert ch.username == "newuser"
        assert ch.title == "NewTitle"

    async def test_set_filtered_bulk(self, db):
        b = ChannelBundle.from_database(db)
        pk = await b.add_channel(_channel(channel_id=600))
        count = await b.set_filtered_bulk([(600, "low_uniqueness")])
        assert count == 1
        ch = await b.get_by_pk(pk)
        assert ch is not None and ch.is_filtered is True

    async def test_reset_all_filters(self, db):
        b = ChannelBundle.from_database(db)
        pk = await b.add_channel(_channel(channel_id=601))
        await b.set_filtered_bulk([(pk, "spam")])
        count = await b.reset_all_filters()
        assert count >= 1
        ch = await b.get_by_pk(pk)
        assert ch is not None and ch.is_filtered is False

    async def test_delete_channel(self, db):
        b = ChannelBundle.from_database(db)
        pk = await b.add_channel(_channel(channel_id=700))
        await b.delete_channel(pk)
        ch = await b.get_by_pk(pk)
        assert ch is None

    async def test_save_and_get_stats(self, db):
        b = ChannelBundle.from_database(db)
        await b.add_channel(_channel(channel_id=800))
        stats = ChannelStats(channel_id=800, subscriber_count=1000, avg_views=50.0)
        sid = await b.save_stats(stats)
        assert sid > 0
        rows = await b.get_stats(800)
        assert len(rows) == 1 and rows[0].subscriber_count == 1000

    async def test_get_latest_stats_for_all(self, db):
        b = ChannelBundle.from_database(db)
        await b.add_channel(_channel(channel_id=801))
        await b.save_stats(ChannelStats(channel_id=801, subscriber_count=500))
        result = await b.get_latest_stats_for_all()
        assert 801 in result

    async def test_create_and_get_collection_task(self, db):
        b = ChannelBundle.from_database(db)
        tid = await b.create_collection_task(900, "ch900")
        task = await b.get_collection_task(tid)
        assert task is not None and task.channel_id == 900

    async def test_update_collection_task(self, db):
        b = ChannelBundle.from_database(db)
        tid = await b.create_collection_task(901, "ch901")
        await b.update_collection_task(tid, CollectionTaskStatus.RUNNING)
        task = await b.get_collection_task(tid)
        assert task is not None and task.status == CollectionTaskStatus.RUNNING

    async def test_update_collection_task_progress(self, db):
        b = ChannelBundle.from_database(db)
        tid = await b.create_collection_task(902, "ch902")
        await b.update_collection_task(tid, CollectionTaskStatus.RUNNING)
        await b.update_collection_task_progress(tid, 42)
        task = await b.get_collection_task(tid)
        assert task is not None and task.messages_collected == 42

    async def test_get_collection_tasks(self, db):
        b = ChannelBundle.from_database(db)
        await b.create_collection_task(903, "ch903")
        tasks = await b.get_collection_tasks()
        assert len(tasks) >= 1

    async def test_get_active_collection_tasks_for_channel(self, db):
        b = ChannelBundle.from_database(db)
        await b.create_collection_task(904, "ch904")
        tasks = await b.get_active_collection_tasks_for_channel(904)
        assert len(tasks) >= 1

    async def test_get_channel_ids_with_active_tasks(self, db):
        b = ChannelBundle.from_database(db)
        await b.create_collection_task(905, "ch905")
        ids = await b.get_channel_ids_with_active_tasks()
        assert 905 in ids

    async def test_cancel_collection_task(self, db):
        b = ChannelBundle.from_database(db)
        tid = await b.create_collection_task(906, "ch906")
        ok = await b.cancel_collection_task(tid, note="test cancel")
        assert ok is True
        task = await b.get_collection_task(tid)
        assert task is not None and task.status == CollectionTaskStatus.CANCELLED

    async def test_get_pending_channel_tasks(self, db):
        b = ChannelBundle.from_database(db)
        await b.create_collection_task(907, "ch907")
        pending = await b.get_pending_channel_tasks()
        assert any(t.channel_id == 907 for t in pending)

    async def test_fail_running_on_startup(self, db):
        b = ChannelBundle.from_database(db)
        tid = await b.create_collection_task(908, "ch908")
        await b.update_collection_task(tid, CollectionTaskStatus.RUNNING)
        count = await b.fail_running_collection_tasks_on_startup()
        assert count >= 1
        task = await b.get_collection_task(tid)
        assert task is not None and task.status == CollectionTaskStatus.FAILED

    async def test_stats_task_lifecycle(self, db):
        b = ChannelBundle.from_database(db)
        payload = StatsAllTaskPayload(channel_ids=[1, 2, 3])
        tid = await b.create_stats_task(payload)
        assert tid > 0

        active = await b.get_active_stats_task()
        assert active is not None

        claimed = await b.claim_next_due_stats_task(NOW)
        assert claimed is not None and claimed.id == tid

        cont_id = await b.create_stats_continuation_task(
            payload=payload,
            run_after=NOW + timedelta(minutes=5),
            parent_task_id=tid,
        )
        assert cont_id > tid

        count = await b.requeue_running_stats_tasks_on_startup(NOW)
        assert count >= 0


# ---------------------------------------------------------------------------
# CollectionBundle
# ---------------------------------------------------------------------------


class TestCollectionBundle:
    async def test_from_database(self, db):
        b = CollectionBundle.from_database(db)
        assert b.channels is not None

    async def test_list_channels(self, db):
        b = CollectionBundle.from_database(db)
        await b.channels.add_channel(_channel(channel_id=1001))
        chs = await b.list_channels()
        assert len(chs) >= 1

    async def test_get_by_pk(self, db):
        b = CollectionBundle.from_database(db)
        pk = await b.channels.add_channel(_channel(channel_id=1002))
        ch = await b.get_by_pk(pk)
        assert ch is not None
        assert ch.channel_id == 1002

    async def test_get_by_channel_id(self, db):
        b = CollectionBundle.from_database(db)
        await b.channels.add_channel(_channel(channel_id=1003))
        ch = await b.get_by_channel_id(1003)
        assert ch is not None

    async def test_update_last_id(self, db):
        b = CollectionBundle.from_database(db)
        await b.channels.add_channel(_channel(channel_id=1004))
        await b.update_last_id(1004, 556)
        ch = await b.get_by_channel_id(1004)
        assert ch is not None and ch.last_collected_id == 556

    async def test_update_meta(self, db):
        b = CollectionBundle.from_database(db)
        await b.channels.add_channel(_channel(channel_id=1005))
        await b.update_meta(1005, username="u2", title="t2")
        ch = await b.get_by_channel_id(1005)
        assert ch is not None and ch.username == "u2"

    async def test_set_active(self, db):
        b = CollectionBundle.from_database(db)
        pk = await b.channels.add_channel(_channel(channel_id=1006))
        await b.set_active(pk, True)
        ch = await b.get_by_pk(pk)
        assert ch is not None and ch.is_active is True

    async def test_set_type(self, db):
        b = CollectionBundle.from_database(db)
        await b.channels.add_channel(_channel(channel_id=1007))
        await b.set_type(1007, "supergroup")
        ch = await b.get_by_channel_id(1007)
        assert ch is not None and ch.channel_type == "supergroup"

    async def test_set_filtered_bulk(self, db):
        b = CollectionBundle.from_database(db)
        await b.channels.add_channel(_channel(channel_id=1008))
        count = await b.set_filtered_bulk([(1008, "spam")])
        assert count == 1

    async def test_reset_all_filters(self, db):
        b = CollectionBundle.from_database(db)
        pk = await b.channels.add_channel(_channel(channel_id=1009))
        await b.set_filtered_bulk([(pk, "spam")])
        count = await b.reset_all_filters()
        assert count >= 1

    async def test_insert_message(self, db):
        b = CollectionBundle.from_database(db)
        await b.channels.add_channel(_channel(channel_id=1010))
        ok = await b.insert_message(_message(channel_id=1010, message_id=1))
        assert ok is True

    async def test_insert_messages_batch(self, db):
        b = CollectionBundle.from_database(db)
        await b.channels.add_channel(_channel(channel_id=1011))
        msgs = [_message(channel_id=1011, message_id=i) for i in range(1, 4)]
        count = await b.insert_messages_batch(msgs)
        assert count == 3

    async def test_search_messages(self, db):
        b = CollectionBundle.from_database(db)
        await b.channels.add_channel(_channel(channel_id=1012))
        await b.insert_message(_message(channel_id=1012, message_id=1, text="findme"))
        results, total = await b.search_messages(query="findme")
        assert total >= 1

    async def test_delete_messages_for_channel(self, db):
        b = CollectionBundle.from_database(db)
        await b.channels.add_channel(_channel(channel_id=1013))
        await b.insert_message(_message(channel_id=1013, message_id=1))
        count = await b.delete_messages_for_channel(1013)
        assert count == 1

    async def test_get_message_stats(self, db):
        b = CollectionBundle.from_database(db)
        stats = await b.get_message_stats()
        assert isinstance(stats, dict)

    async def test_count_matching_prefixes_in_other_channels(self, db):
        b = CollectionBundle.from_database(db)
        count = await b.count_matching_prefixes_in_other_channels(999, ["test"])
        assert isinstance(count, int)

    async def test_settings_get_set(self, db):
        b = CollectionBundle.from_database(db)
        await b.set_setting("mykey", "myval")
        val = await b.get_setting("mykey")
        assert val == "myval"

    async def test_notification_queries(self, db):
        b = CollectionBundle.from_database(db)
        sq = SearchQuery(query="test", notify_on_collect=True, track_stats=False)
        sq_id = await b.search_queries.add(sq)
        assert sq_id > 0
        nqs = await b.list_notification_queries()
        assert any(q.id == sq_id for q in nqs)
        await b.search_queries.delete(sq_id)
        nqs2 = await b.list_notification_queries()
        assert all(q.id != sq_id for q in nqs2)

    async def test_channel_stats(self, db):
        b = CollectionBundle.from_database(db)
        await b.channels.add_channel(_channel(channel_id=1014))
        sid = await b.channel_stats.save_channel_stats(
            ChannelStats(channel_id=1014, subscriber_count=99)
        )
        assert sid > 0
        rows = await b.get_channel_stats(1014)
        assert len(rows) == 1

    async def test_create_collection_task(self, db):
        b = CollectionBundle.from_database(db)
        tid = await b.create_collection_task(1015, "ch1015")
        assert tid > 0

# ---------------------------------------------------------------------------
# SearchBundle
# ---------------------------------------------------------------------------


class TestSearchBundle:
    async def test_from_database(self, db):
        b = SearchBundle.from_database(db)
        assert b.messages is not None

    async def test_search_messages(self, db):
        b = SearchBundle.from_database(db)
        results, total = await b.search_messages(query="nope")
        assert total == 0

    async def test_add_channel_and_insert(self, db):
        b = SearchBundle.from_database(db)
        pk = await b.add_channel(_channel(channel_id=2001))
        assert pk > 0
        count = await b.insert_messages_batch([_message(channel_id=2001, message_id=1)])
        assert count == 1

    async def test_search_after_insert(self, db):
        b = SearchBundle.from_database(db)
        await b.add_channel(_channel(channel_id=2002))
        await b.insert_messages_batch([_message(channel_id=2002, message_id=1, text="searchterm")])
        results, total = await b.search_messages(query="searchterm")
        assert total >= 1

    async def test_log_and_get_searches(self, db):
        b = SearchBundle.from_database(db)
        await b.log_search("+70001112233", "test query", 5)
        recent = await b.get_recent_searches()
        assert len(recent) >= 1


# ---------------------------------------------------------------------------
# SchedulerBundle
# ---------------------------------------------------------------------------


class TestSchedulerBundle:
    async def test_from_database(self, db):
        b = SchedulerBundle.from_database(db)
        assert b.settings is not None

    async def test_settings(self, db):
        b = SchedulerBundle.from_database(db)
        await b.set_setting("skey", "sval")
        val = await b.get_setting("skey")
        assert val == "sval"

    async def test_list_notification_queries(self, db):
        b = SchedulerBundle.from_database(db)
        nqs = await b.list_notification_queries()
        assert isinstance(nqs, list)

    async def test_get_collection_tasks(self, db):
        b = SchedulerBundle.from_database(db)
        tasks = await b.get_collection_tasks()
        assert isinstance(tasks, list)

    async def test_get_recent_searches(self, db):
        b = SchedulerBundle.from_database(db)
        recent = await b.get_recent_searches()
        assert isinstance(recent, list)


# ---------------------------------------------------------------------------
# NotificationBundle
# ---------------------------------------------------------------------------


class TestNotificationBundle:
    async def test_from_database(self, db):
        b = NotificationBundle.from_database(db)
        assert b.accounts is not None

    async def test_list_accounts(self, db):
        b = NotificationBundle.from_database(db)
        accs = await b.list_accounts()
        assert isinstance(accs, list)

    async def test_settings(self, db):
        b = NotificationBundle.from_database(db)
        await b.set_setting("nkey", "nval")
        val = await b.get_setting("nkey")
        assert val == "nval"

    async def test_bot_lifecycle(self, db):
        b = NotificationBundle.from_database(db)
        bot = NotificationBot(
            tg_user_id=123,
            bot_username="testbot",
            bot_token="123:ABC",
        )
        bid = await b.save_bot(bot)
        assert bid > 0
        got = await b.get_bot(123)
        assert got is not None and got.bot_username == "testbot"
        await b.delete_bot(123)
        gone = await b.get_bot(123)
        assert gone is None
