from __future__ import annotations

import argparse


def register(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser | None:
    acc_parser = subparsers.add_parser("account", help="Account management")
    acc_sub = acc_parser.add_subparsers(dest="account_action")
    acc_sub.add_parser("list", help="List accounts")

    acc_info = acc_sub.add_parser("info", help="Show profile info for connected accounts")
    acc_info.add_argument("--phone", default=None, help="Filter by phone number")

    acc_toggle = acc_sub.add_parser("toggle", help="Toggle account active state")
    acc_toggle.add_argument("id", type=int, help="Account id")

    acc_set_primary = acc_sub.add_parser("set-primary", help="Make account the primary one")
    acc_set_primary.add_argument("id", type=int, help="Account id")

    acc_del = acc_sub.add_parser("delete", help="Delete account")
    acc_del.add_argument("id", type=int, help="Account id")
    acc_del.add_argument("--notify-to", default=None, dest="notify_to",
                         help="Phone to reassign notifications to if deleting the notification account")

    acc_send = acc_sub.add_parser("send-code", help="Send Telegram auth code to phone")
    acc_send.add_argument("--phone", required=True, help="Phone number with country code")
    acc_send.add_argument("--api-id", type=int, default=None, dest="api_id",
                          help="Telegram API ID (uses stored if omitted)")
    acc_send.add_argument("--api-hash", default=None, dest="api_hash",
                          help="Telegram API hash (uses stored if omitted)")

    acc_verify = acc_sub.add_parser("verify-code", help="Verify Telegram auth code and add account")
    acc_verify.add_argument("--phone", required=True, help="Phone number with country code")
    acc_verify.add_argument("--code", required=True, help="Auth code received in Telegram")
    acc_verify.add_argument("--password", default=None, help="2FA password (if required)")
    acc_verify.add_argument("--api-id", type=int, default=None, dest="api_id",
                            help="Telegram API ID (uses stored if omitted)")
    acc_verify.add_argument("--api-hash", default=None, dest="api_hash",
                            help="Telegram API hash (uses stored if omitted)")

    acc_add = acc_sub.add_parser(
        "add",
        help="Compatibility alias for send-code / verify-code account onboarding",
    )
    acc_add.add_argument("--phone", required=True, help="Phone number with country code")
    acc_add.add_argument("--code", default=None, help="Auth code received in Telegram")
    acc_add.add_argument("--password", default=None, help="2FA password (if required)")
    acc_add.add_argument("--api-id", type=int, default=None, dest="api_id",
                         help="Telegram API ID (uses stored if omitted)")
    acc_add.add_argument("--api-hash", default=None, dest="api_hash",
                         help="Telegram API hash (uses stored if omitted)")

    acc_sub.add_parser("flood-status", help="Show flood wait timers for all accounts")

    acc_flood_clear = acc_sub.add_parser("flood-clear", help="Clear flood wait for an account")
    acc_flood_clear.add_argument("--phone", required=True, help="Account phone number")

    acc_export = acc_sub.add_parser(
        "export-session",
        help="Print the decrypted StringSession for SSO (⚠️ full account access — keep secret)",
    )
    acc_export.add_argument("--id", type=int, default=None, help="Account id")
    acc_export.add_argument("--phone", default=None, help="Account phone number")
    acc_export.add_argument("--json", action="store_true", help="Emit {phone, session_string} JSON")

    acc_import = acc_sub.add_parser(
        "import",
        help="Add an account from a ready StringSession (SSO import, skips login)",
    )
    acc_import.add_argument("--phone", required=True, help="Phone number with country code")
    acc_import.add_argument(
        "--session-string", required=True, dest="session_string",
        help="Telegram StringSession to import (⚠️ full account access — keep secret)",
    )
