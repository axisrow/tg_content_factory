"""Canonical inventory of Telegram business actions and their entrypoints."""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class TelegramActionInventoryItem:
    """Known Telegram business action surface in the application."""

    action: str
    cli: str | None = None
    web_command: str | None = None
    agent_tool: str | None = None
    pipeline_node: str | None = None
    backend_method: str | None = None


TELEGRAM_ACTION_INVENTORY: tuple[TelegramActionInventoryItem, ...] = (
    TelegramActionInventoryItem(
        action="send_reaction",
        cli="dialogs react",
        web_command="dialogs.react",
        agent_tool="send_reaction",
        pipeline_node="react",
        backend_method="send_reaction",
    ),
    TelegramActionInventoryItem(
        action="create_channel",
        cli="dialogs create-channel",
        web_command="dialogs.create_channel",
        agent_tool="create_telegram_channel",
        backend_method="create_channel",
    ),
    TelegramActionInventoryItem(
        action="leave_dialogs",
        cli="dialogs leave",
        web_command="dialogs.leave",
        agent_tool="leave_dialogs",
        backend_method="leave_channels",
    ),
    TelegramActionInventoryItem(
        action="join_channel",
        cli="dialogs join",
        agent_tool="join_channel",
        backend_method="join_channel",
    ),
    TelegramActionInventoryItem(
        action="send_message",
        cli="dialogs send",
        web_command="dialogs.send",
        agent_tool="send_message",
        backend_method="send_message",
    ),
    TelegramActionInventoryItem(
        action="edit_message",
        cli="dialogs edit-message",
        web_command="dialogs.edit_message",
        agent_tool="edit_message",
        backend_method="edit_message",
    ),
    TelegramActionInventoryItem(
        action="delete_messages",
        cli="dialogs delete-message",
        web_command="dialogs.delete_message",
        agent_tool="delete_message",
        pipeline_node="delete_message",
        backend_method="delete_messages",
    ),
    TelegramActionInventoryItem(
        action="forward_messages",
        cli="dialogs forward",
        web_command="dialogs.forward_messages",
        agent_tool="forward_messages",
        pipeline_node="forward",
        backend_method="forward_messages",
    ),
    TelegramActionInventoryItem(
        action="pin_message",
        cli="dialogs pin-message",
        web_command="dialogs.pin_message",
        agent_tool="pin_message",
        backend_method="pin_message",
    ),
    TelegramActionInventoryItem(
        action="unpin_message",
        cli="dialogs unpin-message",
        web_command="dialogs.unpin_message",
        agent_tool="unpin_message",
        backend_method="unpin_message",
    ),
    TelegramActionInventoryItem(
        action="download_media",
        cli="dialogs download-media",
        web_command="dialogs.download_media",
        agent_tool="download_media",
        backend_method="download_media",
    ),
    TelegramActionInventoryItem(
        action="edit_admin",
        cli="dialogs edit-admin",
        web_command="dialogs.edit_admin",
        agent_tool="edit_admin",
        backend_method="edit_admin",
    ),
    TelegramActionInventoryItem(
        action="edit_permissions",
        cli="dialogs edit-permissions",
        web_command="dialogs.edit_permissions",
        agent_tool="edit_permissions",
        backend_method="edit_permissions",
    ),
    TelegramActionInventoryItem(
        action="kick_participant",
        cli="dialogs kick",
        web_command="dialogs.kick",
        agent_tool="kick_participant",
        backend_method="kick_participant",
    ),
    TelegramActionInventoryItem(
        action="mark_read",
        cli="dialogs mark-read",
        web_command="dialogs.mark_read",
        agent_tool="mark_read",
        backend_method="send_read_acknowledge",
    ),
    TelegramActionInventoryItem(
        action="archive_chat",
        cli="dialogs archive",
        web_command="dialogs.archive",
        agent_tool="archive_chat",
        backend_method="edit_folder",
    ),
    TelegramActionInventoryItem(
        action="unarchive_chat",
        cli="dialogs unarchive",
        web_command="dialogs.unarchive",
        agent_tool="unarchive_chat",
        backend_method="edit_folder",
    ),
    TelegramActionInventoryItem(
        action="get_participants",
        cli="dialogs participants",
        web_command="dialogs.participants",
        agent_tool="get_participants",
        backend_method="get_participants",
    ),
    TelegramActionInventoryItem(
        action="get_broadcast_stats",
        cli="dialogs broadcast-stats",
        web_command="dialogs.broadcast_stats",
        agent_tool="get_broadcast_stats",
        backend_method="get_broadcast_stats",
    ),
)
