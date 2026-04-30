from __future__ import annotations

import argparse


def register(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser | None:
    dialogs_parser = subparsers.add_parser(
        "dialogs", aliases=["my-telegram"], help="Telegram dialogs management",
    )
    dialogs_sub = dialogs_parser.add_subparsers(dest="dialogs_action")
    dialogs_list = dialogs_sub.add_parser("list", help="List all dialogs for an account")
    dialogs_list.add_argument(
        "--phone", default=None, help="Account phone (default: first connected)"
    )  # noqa: E501
    dialogs_refresh = dialogs_sub.add_parser("refresh", help="Refresh dialog cache from Telegram")
    dialogs_refresh.add_argument(
        "--phone", default=None, help="Account phone (default: first connected)"
    )

    dialogs_leave = dialogs_sub.add_parser("leave", help="Leave dialogs by ID")
    dialogs_leave.add_argument(
        "dialog_ids",
        nargs="+",
        help="Dialog IDs to leave (space- or comma-separated)",
    )
    dialogs_leave.add_argument(
        "--phone", default=None, help="Account phone (default: first connected)"
    )
    dialogs_leave.add_argument("--yes", "-y", action="store_true", help="Skip confirmation prompt")

    my_tg_topics = dialogs_sub.add_parser("topics", help="List forum topics for a channel")
    my_tg_topics.add_argument(
        "--channel-id",
        type=int,
        required=True,
        dest="channel_id",
        help="Channel ID to fetch forum topics for",
    )
    my_tg_topics.add_argument(
        "--phone",
        default=None,
        help="Account phone (default: any available)",
    )

    my_tg_clear = dialogs_sub.add_parser("cache-clear", help="Clear in-memory and DB dialog cache")
    my_tg_clear.add_argument("--phone", default=None, help="Account phone (default: all accounts)")
    dialogs_sub.add_parser("cache-status", help="Show dialog cache status (entries, age)")

    my_tg_send = dialogs_sub.add_parser("send", help="Send a direct message to a user or chat")
    my_tg_send.add_argument("recipient", help="Recipient: @username, phone number, or numeric ID")
    my_tg_send.add_argument("text", help="Message text to send")
    my_tg_send.add_argument("--phone", default=None, help="Account phone (default: first connected)")
    my_tg_send.add_argument("--yes", "-y", action="store_true", help="Skip confirmation prompt")

    my_tg_fwd = dialogs_sub.add_parser("forward", help="Forward messages between chats")
    my_tg_fwd.add_argument("from_chat", help="Source chat ID or @username")
    my_tg_fwd.add_argument("to_chat", help="Destination chat ID or @username")
    my_tg_fwd.add_argument("message_ids", nargs="+", help="Message IDs to forward (space or comma-separated)")
    my_tg_fwd.add_argument("--phone", default=None, help="Account phone (default: first connected)")
    my_tg_fwd.add_argument("--yes", "-y", action="store_true", help="Skip confirmation prompt")

    my_tg_edit = dialogs_sub.add_parser("edit-message", help="Edit a sent message")
    my_tg_edit.add_argument("chat_id", help="Chat ID or @username")
    my_tg_edit.add_argument("message_id", type=int, help="Message ID to edit")
    my_tg_edit.add_argument("text", help="New message text")
    my_tg_edit.add_argument("--phone", default=None, help="Account phone (default: first connected)")
    my_tg_edit.add_argument("--yes", "-y", action="store_true", help="Skip confirmation prompt")

    my_tg_del_msg = dialogs_sub.add_parser("delete-message", help="Delete messages from a chat")
    my_tg_del_msg.add_argument("chat_id", help="Chat ID or @username")
    my_tg_del_msg.add_argument("message_ids", nargs="+", help="Message IDs to delete (space or comma-separated)")
    my_tg_del_msg.add_argument("--phone", default=None, help="Account phone (default: first connected)")
    my_tg_del_msg.add_argument("--yes", "-y", action="store_true", help="Skip confirmation prompt")

    my_tg_create = dialogs_sub.add_parser("create-channel", help="Create a new Telegram broadcast channel")
    my_tg_create.add_argument("--phone", default=None, help="Account phone (default: first connected)")
    my_tg_create.add_argument("--title", required=True, help="Channel title")
    my_tg_create.add_argument("--about", default="", help="Channel description")
    my_tg_create.add_argument("--username", default="", help="Public username (leave empty for private)")

    my_tg_pin = dialogs_sub.add_parser("pin-message", help="Pin a message in a chat")
    my_tg_pin.add_argument("chat_id", help="Chat ID or @username")
    my_tg_pin.add_argument("message_id", type=int, help="Message ID to pin")
    my_tg_pin.add_argument("--phone", default=None, help="Account phone (default: first connected)")
    my_tg_pin.add_argument("--notify", action="store_true", help="Notify members about pinned message")
    my_tg_pin.add_argument("--yes", "-y", action="store_true", help="Skip confirmation prompt")

    my_tg_react = dialogs_sub.add_parser("react", help="Set a reaction on a message")
    my_tg_react.add_argument("chat_id", help="Chat ID or @username")
    my_tg_react.add_argument("message_id", type=int, help="Message ID to react on")
    my_tg_react.add_argument("emoji", help="Reaction emoji (e.g. 👍)")
    my_tg_react.add_argument("--phone", default=None, help="Account phone (default: first connected)")

    my_tg_unpin = dialogs_sub.add_parser("unpin-message", help="Unpin a message in a chat")
    my_tg_unpin.add_argument("chat_id", help="Chat ID or @username")
    my_tg_unpin.add_argument("--message-id", type=int, default=None, dest="message_id",
                             help="Message ID to unpin (omit to unpin all)")
    my_tg_unpin.add_argument("--phone", default=None, help="Account phone (default: first connected)")
    my_tg_unpin.add_argument("--yes", "-y", action="store_true", help="Skip confirmation prompt")

    my_tg_dl = dialogs_sub.add_parser("download-media", help="Download media from a message")
    my_tg_dl.add_argument("chat_id", help="Chat ID or @username")
    my_tg_dl.add_argument("message_id", type=int, help="Message ID containing media")
    my_tg_dl.add_argument("--phone", default=None, help="Account phone (default: first connected)")
    my_tg_dl.add_argument("--output-dir", default=".", dest="output_dir",
                          help="Directory to save file (default: current dir)")

    my_tg_participants = dialogs_sub.add_parser("participants", help="List participants of a channel/group")
    my_tg_participants.add_argument("chat_id", help="Chat ID or @username")
    my_tg_participants.add_argument("--phone", default=None, help="Account phone (default: first connected)")
    my_tg_participants.add_argument("--limit", type=int, default=200, help="Max participants to fetch (default: 200)")
    my_tg_participants.add_argument("--search", default="", help="Search query to filter participants")

    my_tg_edit_admin = dialogs_sub.add_parser("edit-admin", help="Promote or demote a user as admin")
    my_tg_edit_admin.add_argument("chat_id", help="Chat ID or @username")
    my_tg_edit_admin.add_argument("user_id", help="User ID or @username to change admin rights for")
    my_tg_edit_admin.add_argument("--phone", default=None, help="Account phone (default: first connected)")
    my_tg_edit_admin.add_argument("--title", default=None, help="Custom admin title")
    my_tg_edit_admin.add_argument("--is-admin", dest="is_admin", action="store_true",
                                  default=True, help="Promote to admin (default)")
    my_tg_edit_admin.add_argument("--no-admin", dest="is_admin", action="store_false", help="Demote from admin")
    my_tg_edit_admin.add_argument("--yes", "-y", action="store_true", help="Skip confirmation prompt")

    my_tg_edit_perms = dialogs_sub.add_parser("edit-permissions", help="Restrict or unrestrict a user in a group")
    my_tg_edit_perms.add_argument("chat_id", help="Chat ID or @username")
    my_tg_edit_perms.add_argument("user_id", help="User ID or @username")
    my_tg_edit_perms.add_argument("--phone", default=None, help="Account phone (default: first connected)")
    my_tg_edit_perms.add_argument("--until-date", default=None, dest="until_date",
                                  help="Restriction end date (ISO format, e.g. 2025-12-31)")
    my_tg_edit_perms.add_argument("--send-messages", dest="send_messages", default=None,
                                  help="Allow sending messages (true/false)")
    my_tg_edit_perms.add_argument("--send-media", dest="send_media", default=None,
                                  help="Allow sending media (true/false)")
    my_tg_edit_perms.add_argument("--yes", "-y", action="store_true", help="Skip confirmation prompt")

    my_tg_kick = dialogs_sub.add_parser("kick", help="Kick a participant from a chat")
    my_tg_kick.add_argument("chat_id", help="Chat ID or @username")
    my_tg_kick.add_argument("user_id", help="User ID or @username to kick")
    my_tg_kick.add_argument("--phone", default=None, help="Account phone (default: first connected)")
    my_tg_kick.add_argument("--yes", "-y", action="store_true", help="Skip confirmation prompt")

    my_tg_bstats = dialogs_sub.add_parser("broadcast-stats", help="Get broadcast statistics for a channel")
    my_tg_bstats.add_argument("chat_id", help="Channel ID or @username")
    my_tg_bstats.add_argument("--phone", default=None, help="Account phone (default: first connected)")

    my_tg_archive = dialogs_sub.add_parser("archive", help="Archive a dialog (move to archive folder)")
    my_tg_archive.add_argument("chat_id", help="Chat ID or @username")
    my_tg_archive.add_argument("--phone", default=None, help="Account phone (default: first connected)")

    my_tg_unarchive = dialogs_sub.add_parser("unarchive", help="Unarchive a dialog (move to main folder)")
    my_tg_unarchive.add_argument("chat_id", help="Chat ID or @username")
    my_tg_unarchive.add_argument("--phone", default=None, help="Account phone (default: first connected)")

    my_tg_mark_read = dialogs_sub.add_parser("mark-read", help="Mark messages as read in a chat")
    my_tg_mark_read.add_argument("chat_id", help="Chat ID or @username")
    my_tg_mark_read.add_argument("--phone", default=None, help="Account phone (default: first connected)")
    my_tg_mark_read.add_argument("--max-id", type=int, default=None, dest="max_id",
                                 help="Mark messages up to this ID as read (default: all)")
