"""Tests for the shared TelegramActionService contract."""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from tg_agent.services.telegram_actions import (
    TelegramActionClientUnavailableError,
    TelegramActionMessageNotFoundError,
    TelegramActionService,
)


def _pool_with_client(client):
    pool = MagicMock()
    pool.get_native_client_by_phone = AsyncMock(return_value=(client, "+1"))
    pool.get_available_client = AsyncMock(return_value=(client, "+1"))
    pool.get_client_by_phone = AsyncMock(return_value=(client, "+1"))
    pool.release_client = AsyncMock()
    return pool


@pytest.mark.anyio
async def test_send_reaction_resolves_entity_and_releases_client():
    client = AsyncMock()
    client.get_entity = AsyncMock(return_value="entity")
    client.get_input_entity = AsyncMock(return_value="entity")
    client.send_reaction = AsyncMock()
    pool = _pool_with_client(client)

    result = await TelegramActionService(pool).send_reaction(
        phone="+1",
        chat_id="@chat",
        message_id=42,
        emoji="🔥",
    )

    assert result.phone == "+1"
    client.get_input_entity.assert_awaited_once_with("@chat")
    client.send_reaction.assert_awaited_once_with("entity", 42, "🔥")
    pool.release_client.assert_awaited_once_with("+1")


@pytest.mark.anyio
async def test_send_reaction_clear_passes_none_and_releases_client():
    client = AsyncMock()
    client.get_entity = AsyncMock(return_value="entity")
    client.get_input_entity = AsyncMock(return_value="entity")
    client.send_reaction = AsyncMock()
    pool = _pool_with_client(client)

    result = await TelegramActionService(pool).send_reaction(
        phone="+1",
        chat_id="@chat",
        message_id=42,
        emoji=None,
    )

    assert result.phone == "+1"
    client.get_input_entity.assert_awaited_once_with("@chat")
    client.send_reaction.assert_awaited_once_with("entity", 42, None)
    pool.release_client.assert_awaited_once_with("+1")


@pytest.mark.anyio
async def test_create_channel_sets_username_and_returns_link():
    client = AsyncMock()
    channel = SimpleNamespace(id=123, username="")
    client.create_channel = AsyncMock(return_value=SimpleNamespace(chats=[channel]))
    client.update_channel_username = AsyncMock()
    pool = _pool_with_client(client)

    result = await TelegramActionService(pool).create_channel(
        phone="+1",
        title="Title",
        about="About",
        username="public_name",
    )

    assert result.channel_id == 123
    assert result.channel_username == "public_name"
    assert result.invite_link == "https://t.me/public_name"
    client.create_channel.assert_awaited_once_with(
        title="Title",
        about="About",
        broadcast=True,
        megagroup=False,
    )
    client.update_channel_username.assert_awaited_once_with(channel, "public_name")


@pytest.mark.anyio
async def test_create_public_channel_reserves_both_lifecycle_writes_first():
    events: list[str] = []
    channel = SimpleNamespace(id=123, username="")

    class Client:
        def reserve_rate_limit_slots(self, operation: str, *, slots: int = 1) -> None:
            events.append(f"reserve:{operation}:{slots}")

        async def create_channel(self, **kwargs):
            events.append("create")
            assert kwargs.pop("_rate_limit_reserved") is True
            return SimpleNamespace(chats=[channel])

        async def update_channel_username(self, target, username, **kwargs):
            events.append("username")
            assert target is channel
            assert username == "public_name"
            assert kwargs == {"_rate_limit_reserved": True}

    result = await TelegramActionService(_pool_with_client(Client())).create_channel(
        phone="+1",
        title="Title",
        username="public_name",
    )

    assert result.channel_username == "public_name"
    assert events == ["reserve:telegram_create_channel:2", "create", "username"]


@pytest.mark.anyio
async def test_create_group_passes_megagroup_flags_without_username_update():
    client = AsyncMock()
    group = SimpleNamespace(id=456, username="")
    client.create_channel = AsyncMock(return_value=SimpleNamespace(chats=[group]))
    client.update_channel_username = AsyncMock()
    pool = _pool_with_client(client)

    result = await TelegramActionService(pool).create_channel(
        phone="+1",
        title="Group",
        about="About",
        broadcast=False,
        megagroup=True,
    )

    assert result.channel_id == 456
    assert result.channel_username == ""
    client.create_channel.assert_awaited_once_with(
        title="Group",
        about="About",
        broadcast=False,
        megagroup=True,
    )
    client.update_channel_username.assert_not_awaited()


@pytest.mark.anyio
async def test_join_dialog_public_resolves_entity_joins_and_clears_cache():
    client = AsyncMock()
    client.get_entity = AsyncMock(return_value="entity")
    client.get_input_entity = AsyncMock(return_value="entity")
    client.join_channel = AsyncMock()
    pool = _pool_with_client(client)
    pool.invalidate_dialogs_cache = MagicMock()

    result = await TelegramActionService(pool).join_dialog(
        phone="+1",
        target="https://t.me/prog_ai/1290",
    )

    assert result.phone == "+1"
    assert result.target == "@prog_ai"
    assert result.via_invite is False
    client.get_input_entity.assert_awaited_once_with("@prog_ai")
    client.join_channel.assert_awaited_once_with("entity")
    pool.invalidate_dialogs_cache.assert_called_once_with("+1")
    pool.release_client.assert_awaited_once_with("+1")


@pytest.mark.anyio
async def test_join_dialog_private_invite_uses_import_hash():
    client = AsyncMock()
    client.get_entity = AsyncMock()
    client.import_chat_invite = AsyncMock()
    pool = _pool_with_client(client)

    result = await TelegramActionService(pool).join_dialog(
        phone="+1",
        target="https://t.me/+AAAAExampleHash",
    )

    assert result.via_invite is True
    client.import_chat_invite.assert_awaited_once_with("AAAAExampleHash")
    client.get_entity.assert_not_awaited()
    pool.release_client.assert_awaited_once_with("+1")


@pytest.mark.anyio
async def test_download_media_looks_up_message_checks_path_and_releases_client(tmp_path):
    client = AsyncMock()
    client.get_entity = AsyncMock(return_value="entity")
    client.get_input_entity = AsyncMock(return_value="entity")
    message = SimpleNamespace(id=7)

    async def _iter_messages(entity, ids):
        assert entity == "entity"
        assert ids == 7
        yield message

    media_path = tmp_path / "media.jpg"
    media_path.write_text("x")
    client.iter_messages = MagicMock(return_value=_iter_messages("entity", 7))
    client.download_media = AsyncMock(return_value=str(media_path))
    pool = _pool_with_client(client)

    result = await TelegramActionService(pool).download_media(
        phone="+1",
        chat_id="@chat",
        message_id=7,
        output_dir=tmp_path,
    )

    assert result.path == str(media_path)
    client.download_media.assert_awaited_once_with(message, file=str(tmp_path.resolve()))
    pool.release_client.assert_awaited_once_with("+1")


@pytest.mark.anyio
async def test_leave_dialogs_delegates_to_pool_leave_channels():
    pool = MagicMock()
    pool.leave_channels = AsyncMock(return_value={100: True, 200: False})

    result = await TelegramActionService(pool).leave_dialogs(
        phone="+1",
        dialogs=[(100, "channel"), (200, "supergroup")],
    )

    assert result.success_count == 1
    assert result.failed_count == 1
    pool.leave_channels.assert_awaited_once_with("+1", [(100, "channel"), (200, "supergroup")])


@pytest.mark.anyio
async def test_delete_dialogs_delegates_to_pool_delete_dialogs():
    pool = MagicMock()
    pool.delete_dialogs = AsyncMock(return_value={100: True, 200: False})

    result = await TelegramActionService(pool).delete_dialogs(
        phone="+1",
        dialogs=[(100, "channel"), (200, "supergroup")],
    )

    assert result.success_count == 1
    assert result.failed_count == 1
    pool.delete_dialogs.assert_awaited_once_with("+1", [(100, "channel"), (200, "supergroup")])


@pytest.mark.anyio
async def test_action_service_raises_when_client_unavailable():
    pool = MagicMock()
    pool.get_native_client_by_phone = AsyncMock(return_value=None)

    with pytest.raises(TelegramActionClientUnavailableError):
        await TelegramActionService(pool).send_message(phone="+1", recipient="@u", text="hi")


@pytest.mark.anyio
async def test_send_message_disables_link_preview_by_default():
    client = AsyncMock()
    client.get_entity = AsyncMock(return_value="entity")
    client.get_input_entity = AsyncMock(return_value="entity")
    client.send_message = AsyncMock(return_value=SimpleNamespace(id=7))
    pool = _pool_with_client(client)

    result = await TelegramActionService(pool).send_message(phone="+1", recipient="@u", text="hi")

    assert result.message_id == 7
    client.send_message.assert_awaited_once_with("entity", "hi", link_preview=False)
    pool.release_client.assert_awaited_once_with("+1")


@pytest.mark.anyio
async def test_send_message_silent_passes_through_to_the_client():
    client = AsyncMock()
    client.get_entity = AsyncMock(return_value="entity")
    client.get_input_entity = AsyncMock(return_value="entity")
    client.send_message = AsyncMock(return_value=SimpleNamespace(id=8))
    pool = _pool_with_client(client)

    await TelegramActionService(pool).send_message(phone="+1", recipient="@u", text="hi", silent=True)

    client.send_message.assert_awaited_once_with("entity", "hi", link_preview=False, silent=True)


@pytest.mark.anyio
async def test_send_message_reply_to_passes_through_to_the_client():
    """#1902: an in-chat reply send carries reply_to (reply_to_message_id)."""
    client = AsyncMock()
    client.get_entity = AsyncMock(return_value="entity")
    client.get_input_entity = AsyncMock(return_value="entity")
    client.send_message = AsyncMock(return_value=SimpleNamespace(id=10))
    pool = _pool_with_client(client)

    await TelegramActionService(pool).send_message(
        phone="+1", recipient="@chat", text="answering", reply_to=55
    )

    client.send_message.assert_awaited_once_with(
        "entity", "answering", link_preview=False, reply_to=55
    )


@pytest.mark.anyio
async def test_send_message_comment_to_passes_through_to_the_client():
    """#1903: a comment send carries comment_to so Telethon routes it to the
    linked discussion group of the recipient channel's post."""
    client = AsyncMock()
    client.get_entity = AsyncMock(return_value="entity")
    client.get_input_entity = AsyncMock(return_value="entity")
    client.send_message = AsyncMock(return_value=SimpleNamespace(id=9))
    pool = _pool_with_client(client)

    await TelegramActionService(pool).send_message(
        phone="+1", recipient="@channel", text="nice", comment_to=42
    )

    client.send_message.assert_awaited_once_with(
        "entity", "nice", link_preview=False, comment_to=42
    )


@pytest.mark.anyio
async def test_send_message_comment_to_records_the_discussion_group_chat(db):
    """#1903 review: the registry row joins the deletion store by chat, and a
    comment's id lives in the linked discussion group's id space — the row
    must carry the group's chat (message.peer_id), not the recipient channel,
    while recipient keeps the channel context.
    #1946: the row says it is a comment and names the post it answers; the
    result carries the landing chat for the caller's echo."""
    from telethon import types

    channel_peer = types.PeerChannel(channel_id=555)
    group_peer = types.PeerChannel(channel_id=2000)
    client = AsyncMock()
    client.get_entity = AsyncMock(return_value=channel_peer)
    client.get_input_entity = AsyncMock(return_value=channel_peer)
    client.send_message = AsyncMock(
        return_value=SimpleNamespace(id=777, peer_id=group_peer)
    )
    pool = _pool_with_client(client)

    result = await TelegramActionService(pool, db=db).send_message(
        phone="+1", recipient="@chat", text="nice", comment_to=42, source="dialogs_send"
    )

    rows = await db.execute_fetchall(
        "SELECT chat_id, chat_type, recipient, ref FROM sent_messages"
    )
    assert [tuple(row) for row in rows] == [(-1000000002000, "comment", "@chat", "42")]
    assert result.chat_id == -1000000002000


@pytest.mark.anyio
async def test_edit_message_disables_link_preview_by_default():
    client = AsyncMock()
    client.get_entity = AsyncMock(return_value="entity")
    client.get_input_entity = AsyncMock(return_value="entity")
    client.edit_message = AsyncMock()
    pool = _pool_with_client(client)

    await TelegramActionService(pool).edit_message(
        phone="+1", chat_id="@chat", message_id=9, text="new"
    )

    client.edit_message.assert_awaited_once_with("entity", 9, "new", link_preview=False)
    pool.release_client.assert_awaited_once_with("+1")


def _edit_media_client(caption="old caption", entities=("entity-a",)):
    client = AsyncMock()
    client.get_entity = AsyncMock(return_value="entity")
    client.get_input_entity = AsyncMock(return_value="entity")
    message = SimpleNamespace(id=7, message=caption, entities=entities)

    async def _iter_messages(entity, ids):
        assert ids == 7
        yield message

    client.iter_messages = MagicMock(return_value=_iter_messages("entity", 7))
    client.edit_message = AsyncMock()
    return client


@pytest.mark.anyio
async def test_edit_message_media_preserves_caption_and_entities(tmp_path):
    client = _edit_media_client()
    pool = _pool_with_client(client)
    media = tmp_path / "replacement.png"
    media.write_text("x")

    await TelegramActionService(pool).edit_message_media(
        phone="+1", chat_id="@chat", message_id=7, file=str(media)
    )

    client.edit_message.assert_awaited_once_with(
        "entity", 7, "old caption", file=str(media), parse_mode=None, formatting_entities=["entity-a"]
    )
    pool.release_client.assert_awaited_once_with("+1")


@pytest.mark.anyio
async def test_edit_message_media_empty_text_clears_caption(tmp_path):
    client = _edit_media_client(caption="", entities=[])
    pool = _pool_with_client(client)
    media = tmp_path / "replacement.png"
    media.write_text("x")

    await TelegramActionService(pool).edit_message_media(
        phone="+1", chat_id="@chat", message_id=7, file=str(media)
    )

    client.edit_message.assert_awaited_once_with(
        "entity", 7, "", file=str(media), parse_mode=None, formatting_entities=[]
    )


@pytest.mark.anyio
async def test_edit_message_media_replaces_caption_with_parse_mode(tmp_path):
    client = _edit_media_client()
    pool = _pool_with_client(client)
    media = tmp_path / "replacement.png"
    media.write_text("x")

    await TelegramActionService(pool).edit_message_media(
        phone="+1", chat_id="@chat", message_id=7, file=str(media), text="**new**", parse_mode="md"
    )

    client.edit_message.assert_awaited_once_with("entity", 7, "**new**", file=str(media), parse_mode="md")


@pytest.mark.anyio
async def test_edit_message_media_plain_text_disables_parsing(tmp_path):
    client = _edit_media_client()
    pool = _pool_with_client(client)
    media = tmp_path / "replacement.png"
    media.write_text("x")

    await TelegramActionService(pool).edit_message_media(
        phone="+1", chat_id="@chat", message_id=7, file=str(media), text="literal **x**", parse_mode="plain"
    )

    client.edit_message.assert_awaited_once_with(
        "entity", 7, "literal **x**", file=str(media), parse_mode=None
    )


@pytest.mark.anyio
async def test_edit_message_media_default_parse_mode_keeps_telethon_default(tmp_path):
    client = _edit_media_client()
    pool = _pool_with_client(client)
    media = tmp_path / "replacement.png"
    media.write_text("x")

    await TelegramActionService(pool).edit_message_media(
        phone="+1", chat_id="@chat", message_id=7, file=str(media), text="plain words"
    )

    client.edit_message.assert_awaited_once_with("entity", 7, "plain words", file=str(media))


@pytest.mark.anyio
async def test_edit_message_media_rejects_unknown_parse_mode(tmp_path):
    client = _edit_media_client()
    pool = _pool_with_client(client)
    media = tmp_path / "replacement.png"
    media.write_text("x")

    with pytest.raises(ValueError, match="parse_mode"):
        await TelegramActionService(pool).edit_message_media(
            phone="+1", chat_id="@chat", message_id=7, file=str(media), text="new", parse_mode="rich"
        )

    client.edit_message.assert_not_awaited()


@pytest.mark.anyio
async def test_edit_message_media_requires_an_existing_local_file(tmp_path):
    client = _edit_media_client()
    pool = _pool_with_client(client)

    with pytest.raises(FileNotFoundError, match="replacement.png"):
        await TelegramActionService(pool).edit_message_media(
            phone="+1", chat_id="@chat", message_id=7, file=str(tmp_path / "replacement.png")
        )

    client.edit_message.assert_not_awaited()


@pytest.mark.anyio
async def test_edit_message_media_missing_message_raises(tmp_path):
    client = AsyncMock()
    client.get_entity = AsyncMock(return_value="entity")
    client.get_input_entity = AsyncMock(return_value="entity")

    async def _iter_messages(entity, ids):
        return
        yield

    client.iter_messages = MagicMock(return_value=_iter_messages("entity", 7))
    client.edit_message = AsyncMock()
    pool = _pool_with_client(client)
    media = tmp_path / "replacement.png"
    media.write_text("x")

    with pytest.raises(TelegramActionMessageNotFoundError):
        await TelegramActionService(pool).edit_message_media(
            phone="+1", chat_id="@chat", message_id=7, file=str(media)
        )

    client.edit_message.assert_not_awaited()


# --------------------------------------------------------------------------- #
# sent-messages registry (#1893)
# --------------------------------------------------------------------------- #


@pytest.mark.anyio
async def test_send_message_records_a_registry_row(db):
    from telethon import types

    peer = types.PeerChannel(channel_id=555)
    client = AsyncMock()
    client.get_entity = AsyncMock(return_value=peer)
    client.get_input_entity = AsyncMock(return_value=peer)
    client.send_message = AsyncMock(return_value=SimpleNamespace(id=777))
    pool = _pool_with_client(client)

    result = await TelegramActionService(pool, db=db).send_message(
        phone="+1", recipient="@chat", text="привет", source="dialogs_send", ref="run:42"
    )

    assert result.phone == "+1"
    rows = await db.execute_fetchall(
        "SELECT phone, chat_id, message_id, chat_type, recipient, text, source, ref"
        " FROM sent_messages"
    )
    assert [tuple(row) for row in rows] == [
        ("+1", -1000000000555, 777, "channel", "@chat", "привет", "dialogs_send", "run:42"),
    ]


@pytest.mark.anyio
async def test_send_message_dm_shape_records_null_chat(db):
    from telethon import types

    peer = types.PeerUser(user_id=888)
    client = AsyncMock()
    client.get_entity = AsyncMock(return_value=peer)
    client.get_input_entity = AsyncMock(return_value=peer)
    client.send_message = AsyncMock(return_value=SimpleNamespace(id=9))
    pool = _pool_with_client(client)

    await TelegramActionService(pool, db=db).send_message(
        phone="+1", recipient="+888", text="hi", is_user=True, source="dm_draft"
    )

    rows = await db.execute_fetchall(
        "SELECT chat_id, chat_type, source FROM sent_messages"
    )
    assert [tuple(row) for row in rows] == [(None, "dm", "dm_draft")]


@pytest.mark.anyio
async def test_send_message_without_db_skips_registry_and_does_not_crash():
    client = AsyncMock()
    client.get_entity = AsyncMock(return_value="entity")
    client.get_input_entity = AsyncMock(return_value="entity")
    client.send_message = AsyncMock(return_value=SimpleNamespace(id=7))
    pool = _pool_with_client(client)

    result = await TelegramActionService(pool).send_message(phone="+1", recipient="@u", text="hi")

    assert result.message_id == 7
    assert result.chat_id is None  # a plain send lands where it was addressed (#1946)


@pytest.mark.anyio
async def test_send_message_registry_failure_never_breaks_the_send():
    from telethon import types

    peer = types.PeerChannel(channel_id=555)
    client = AsyncMock()
    client.get_entity = AsyncMock(return_value=peer)
    client.get_input_entity = AsyncMock(return_value=peer)
    client.send_message = AsyncMock(return_value=SimpleNamespace(id=777))

    class _BrokenDb:
        repos = SimpleNamespace(
            sent_messages=SimpleNamespace(record_many=AsyncMock(side_effect=RuntimeError("db down")))
        )

    pool = _pool_with_client(client)

    result = await TelegramActionService(pool, db=_BrokenDb()).send_message(
        phone="+1", recipient="@chat", text="hi", source="dialogs_send"
    )

    assert result.message_id == 777


@pytest.mark.anyio
async def test_forward_messages_records_one_row_per_forwarded_id(db):
    from telethon import types

    peer = types.PeerChannel(channel_id=556)
    client = AsyncMock()
    client.get_entity = AsyncMock(return_value=peer)
    client.get_input_entity = AsyncMock(return_value=peer)
    client.forward_messages = AsyncMock(
        return_value=[SimpleNamespace(id=1), SimpleNamespace(id=2)]
    )
    pool = _pool_with_client(client)

    result = await TelegramActionService(pool, db=db).forward_messages(
        phone="+1", from_chat="@src", to_chat="@dst", message_ids=[10, 20], source="dialogs_forward"
    )

    assert result.message_ids == (1, 2)
    rows = await db.execute_fetchall(
        "SELECT phone, chat_id, message_id, chat_type, text, source FROM sent_messages ORDER BY message_id"
    )
    assert [tuple(row) for row in rows] == [
        ("+1", -1000000000556, 1, "channel", None, "dialogs_forward"),
        ("+1", -1000000000556, 2, "channel", None, "dialogs_forward"),
    ]


@pytest.mark.anyio
async def test_forward_messages_records_the_landing_peer_of_the_delivered_message(db):
    """#1946: the pipeline forward node hands a bare dialog id and skips the
    resolve — the store would class that id as a DM. The row follows the peer
    of the delivered message."""
    from telethon import types

    client = AsyncMock()
    client.forward_messages = AsyncMock(
        return_value=[SimpleNamespace(id=5, peer_id=types.PeerChannel(channel_id=557))]
    )
    pool = _pool_with_client(client)

    result = await TelegramActionService(pool, db=db).forward_messages(
        phone="+1", from_chat=111, to_chat=557, message_ids=[9],
        resolve_entities=False, source="pipeline_forward",
    )

    assert result.message_ids == (5,)
    rows = await db.execute_fetchall(
        "SELECT chat_id, message_id, chat_type, recipient FROM sent_messages"
    )
    assert [tuple(row) for row in rows] == [(-1000000000557, 5, "channel", "557")]
