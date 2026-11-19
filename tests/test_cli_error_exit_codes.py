"""CLI failures must exit non-zero with the error on stderr (#1486).

Automation-first exit-code contract: a failed command raises ``typer.Exit``
(message on stderr, non-zero code); stdout stays for results. Empty-but-valid
results ("No messages found.") deliberately keep exit 0 — they are not failures
and are asserted as such by the existing behaviour tests.

Written red against the print-and-return pattern (errors exited 0) and pinning
the fixed behaviour: each driver runs the real ``*_impl`` body and asserts the
``typer.Exit`` plus the stderr fragment a script would match on.
"""
from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import typer
from telethon_floodgate import FloodWaitInfo, HandledFloodWaitError

from tg_agent.config import AppConfig
from tg_agent.models import Message
from tg_agent.services.telegram_actions import TelegramActionClientUnavailableError

pytestmark = pytest.mark.aiosqlite_serial

PHONE = "+10000000001"


def _flood_info(seconds: int = 90) -> FloodWaitInfo:
    return FloodWaitInfo(
        operation="cli_test",
        phone=PHONE,
        wait_seconds=seconds,
        next_available_at_utc=datetime.now(timezone.utc) + timedelta(seconds=seconds),
        detail="тестовый флуд",
    )


def _ns(**kw) -> argparse.Namespace:
    return argparse.Namespace(**kw)


def _assert_exit(excinfo, capsys, fragment: str, code: int = 1) -> None:
    assert excinfo.value.exit_code == code
    err = capsys.readouterr().err
    assert fragment in err


# ---------------------------------------------------------------------------
# messages read --live
# ---------------------------------------------------------------------------


def _live_pool(**kw):
    pool = MagicMock()
    pool.clients = kw.get("clients", {PHONE: object()})
    pool.disconnect_all = AsyncMock()
    pool.get_native_client_by_phone = AsyncMock(
        return_value=kw.get("native", (MagicMock(), PHONE))
    )
    if "resolve" in kw:
        pool.resolve_entity_with_warm = AsyncMock(side_effect=kw["resolve"])
    else:
        pool.resolve_entity_with_warm = AsyncMock(return_value=SimpleNamespace(id=1))
    return pool


def _run_live_impl(pool, *, identifier="@dlg", phone=PHONE):
    async def fake_init_pool(_, __):
        return MagicMock(), pool

    from tg_agent.cli.commands.messages import messages_read_impl

    with patch("tg_agent.cli.runtime.init_pool", side_effect=fake_init_pool):
        asyncio.run(
            messages_read_impl(
                "config.yaml",
                identifier=identifier,
                limit=10,
                live=True,
                phone=phone,
            )
        )


async def _empty_iter():
    return
    yield  # pragma: no cover


async def _boom_iter():
    raise RuntimeError("boom")
    yield  # pragma: no cover


class TestMessagesLiveExitCodes:
    def test_no_connected_accounts(self, cli_env, capsys):
        pool = _live_pool(clients={})
        with pytest.raises(typer.Exit) as excinfo:
            _run_live_impl(pool)
        _assert_exit(excinfo, capsys, "No connected accounts.")

    def test_phone_not_connected(self, cli_env, capsys):
        pool = _live_pool(clients={PHONE: object()})
        with pytest.raises(typer.Exit) as excinfo:
            _run_live_impl(pool, phone="+19999999999")
        _assert_exit(excinfo, capsys, "Account +19999999999 not connected")

    def test_client_unavailable(self, cli_env, capsys):
        pool = _live_pool(native=None)
        with pytest.raises(typer.Exit) as excinfo:
            _run_live_impl(pool)
        _assert_exit(excinfo, capsys, f"Client for {PHONE} unavailable")

    def test_resolve_failure(self, cli_env, capsys):
        from tg_agent.models import Account

        asyncio.run(cli_env.add_account(Account(phone=PHONE, session_string="s", is_primary=True)))
        pool = _live_pool(resolve=ValueError("Cannot find any entity"))
        with pytest.raises(typer.Exit) as excinfo:
            _run_live_impl(pool)
        _assert_exit(excinfo, capsys, f"Cannot resolve '@dlg' via account {PHONE}")

    def test_flood_wait(self, cli_env, capsys):
        pool = _live_pool()
        fake_client = MagicMock()
        fake_client.iter_messages = MagicMock(return_value=_empty_iter())
        pool.get_native_client_by_phone = AsyncMock(return_value=(fake_client, PHONE))

        async def _raise_flood(coro, **_):
            # Consume the unread coroutine before failing, or its GC shows up
            # as an unraisable warning attributed to a later test. The old
            # aclose-only probe silently no-oped — coroutine objects have
            # close(), not aclose() — so _read_messages leaked and flaked
            # random later serial-lane tests (#1471 CI).
            coro.close()
            raise HandledFloodWaitError(_flood_info())

        flood_patch = patch(
            "tg_agent.cli.commands.messages.run_with_flood_wait",
            new=_raise_flood,
        )
        with pytest.raises(typer.Exit) as excinfo, flood_patch:
            _run_live_impl(pool)
        _assert_exit(excinfo, capsys, "Flood wait")

    def test_read_error(self, cli_env, capsys):
        pool = _live_pool()
        fake_client = MagicMock()
        fake_client.iter_messages = MagicMock(return_value=_boom_iter())
        pool.get_native_client_by_phone = AsyncMock(return_value=(fake_client, PHONE))
        with pytest.raises(typer.Exit) as excinfo:
            _run_live_impl(pool)
        _assert_exit(excinfo, capsys, "Error reading messages")


# ---------------------------------------------------------------------------
# dialogs
# ---------------------------------------------------------------------------


def _dlg_pool(**kw):
    pool = MagicMock()
    pool.clients = kw.get("clients", {PHONE: object()})
    if "resolve_any" in kw:
        pool.resolve_any_entity = AsyncMock(side_effect=kw["resolve_any"])
    return pool


def _dlg_args(**kw) -> argparse.Namespace:
    kw.setdefault("phone", PHONE)
    return argparse.Namespace(**kw)


class TestDialogsExitCodes:
    @pytest.mark.parametrize(
        ("clients", "phone", "fragment"),
        [
            ({}, None, "No connected accounts."),
            ({PHONE: object()}, "+19999999999", "Account +19999999999 not connected"),
        ],
    )
    def test_resolve_phone_choke_point(self, clients, phone, fragment, capsys):
        from tg_agent.cli.commands.dialogs import _resolve_phone

        with pytest.raises(typer.Exit) as excinfo:
            asyncio.run(
                _resolve_phone(MagicMock(), _dlg_pool(clients=clients), _dlg_args(phone=phone))
            )
        _assert_exit(excinfo, capsys, fragment)

    @pytest.mark.parametrize(
        ("side_effect", "fragment"),
        [
            (RuntimeError("no_client left"), "No connected accounts."),
            (RuntimeError("db gone"), "Error resolving entity"),
        ],
    )
    def test_resolve_entity_error(self, side_effect, fragment, capsys):
        from tg_agent.cli.commands.dialogs import _dialogs_resolve

        pool = _dlg_pool(resolve_any=side_effect)
        with pytest.raises(typer.Exit) as excinfo:
            asyncio.run(_dialogs_resolve(_dlg_args(identifier="@dlg"), MagicMock(), pool))
        _assert_exit(excinfo, capsys, fragment)

    def test_resolve_entity_not_found(self, capsys):
        from tg_agent.cli.commands.dialogs import _dialogs_resolve

        pool = _dlg_pool()
        pool.resolve_any_entity = AsyncMock(return_value=None)
        with pytest.raises(typer.Exit) as excinfo:
            asyncio.run(_dialogs_resolve(_dlg_args(identifier="@dlg"), MagicMock(), pool))
        _assert_exit(excinfo, capsys, "Entity '@dlg' not found")

    @pytest.mark.parametrize(
        ("exc", "fragment"),
        [
            (TelegramActionClientUnavailableError("no client"), f"Client for {PHONE} unavailable"),
            (HandledFloodWaitError(_flood_info()), "Flood wait"),
            (RuntimeError("asr down"), "Error transcribing"),
        ],
    )
    def test_transcribe_errors(self, exc, fragment, capsys):
        from tg_agent.cli.commands import dialogs

        svc = MagicMock()
        svc.transcribe_voice = AsyncMock(side_effect=exc)
        with pytest.raises(typer.Exit) as excinfo, patch.object(
            dialogs, "TelegramActionService", return_value=svc
        ):
            asyncio.run(
                dialogs._dialogs_transcribe(
                    _dlg_args(engine="groq", chat_id=100, message_id=7),
                    MagicMock(),
                    _dlg_pool(),
                )
            )
        _assert_exit(excinfo, capsys, fragment)

    def test_edit_permissions_requires_a_flag(self, capsys):
        from tg_agent.cli.commands.dialogs import _dialogs_edit_permissions

        args = _dlg_args(
            yes=True,
            chat_id=100,
            user_id=5,
            send_messages=None,
            send_media=None,
            until_date=None,
        )
        with pytest.raises(typer.Exit) as excinfo:
            asyncio.run(_dialogs_edit_permissions(args, MagicMock(), _dlg_pool()))
        _assert_exit(excinfo, capsys, "specify at least one flag")

    def test_archive_history_invalid_chat_id(self, capsys):
        from tg_agent.cli.commands import dialogs

        args = _dlg_args(config="config.yaml", chat_id="not-a-number")
        with patch.object(dialogs.runtime, "load_config", return_value=AppConfig()):
            with pytest.raises(typer.Exit) as excinfo:
                asyncio.run(dialogs._dialogs_archive_history(args, MagicMock(), _dlg_pool()))
        _assert_exit(excinfo, capsys, "Invalid --chat-id")

    def test_create_channel_empty_response(self, capsys):
        from tg_agent.cli.commands import dialogs

        svc = MagicMock()
        svc.create_channel = AsyncMock(
            side_effect=RuntimeError("Telegram returned empty response — updates empty")
        )
        args = _dlg_args(dialogs_action="create-channel", title="T", about=None, username=None)
        with pytest.raises(typer.Exit) as excinfo, patch.object(
            dialogs, "TelegramActionService", return_value=svc
        ):
            asyncio.run(dialogs._dialogs_create_channel(args, MagicMock(), _dlg_pool()))
        _assert_exit(excinfo, capsys, "Telegram returned empty response")


# ---------------------------------------------------------------------------
# account (SSO export/import, auth codes)
# ---------------------------------------------------------------------------


class TestAccountExitCodes:
    @pytest.mark.parametrize(
        ("kwargs", "fragment"),
        [
            ({}, "provide exactly one of --id or --phone"),
            ({"id": 1, "phone": PHONE}, "provide exactly one of --id or --phone"),
            ({"id": 999}, "Account id=999 not found"),
        ],
    )
    def test_export_session_errors(self, cli_env, kwargs, fragment, capsys):
        from tg_agent.cli.commands.account import _run_export_session

        with pytest.raises(typer.Exit) as excinfo:
            asyncio.run(_run_export_session(_ns(json=False, **kwargs), cli_env))
        _assert_exit(excinfo, capsys, fragment)

    def test_export_session_no_session_string(self, capsys):
        from tg_agent.cli.commands.account import _run_export_session

        db_stub = MagicMock()
        db_stub.get_account_summaries = AsyncMock(
            return_value=[SimpleNamespace(id=1, phone=PHONE)]
        )
        db_stub.repos.accounts.get_decrypted_session = AsyncMock(return_value="")
        with pytest.raises(typer.Exit) as excinfo:
            asyncio.run(_run_export_session(_ns(json=False, phone=PHONE), db_stub))
        _assert_exit(excinfo, capsys, "has no session string to export")

    @pytest.mark.parametrize(
        ("kwargs", "fragment"),
        [
            ({"phone": None, "session_string": None}, "provide --phone and a session string"),
            ({"phone": PHONE, "session_string": "garbage"}, "invalid Telegram session string"),
        ],
    )
    def test_import_errors(self, cli_env, kwargs, fragment, capsys):
        from tg_agent.cli.commands.account import _run_import

        with pytest.raises(typer.Exit) as excinfo:
            asyncio.run(_run_import(_ns(**kwargs), cli_env))
        _assert_exit(excinfo, capsys, fragment)

    def test_send_code_without_credentials(self, cli_env, capsys):
        from tg_agent.cli.commands.account import send_code_impl

        with pytest.raises(typer.Exit) as excinfo:
            asyncio.run(send_code_impl("config.yaml", phone=PHONE, api_id=0, api_hash=None))
        _assert_exit(excinfo, capsys, "API credentials not configured")

    def test_send_code_send_failure(self, cli_env, capsys):
        from tg_agent.cli.commands import account

        auth = MagicMock()
        auth.send_code = AsyncMock(side_effect=RuntimeError("tg down"))
        auth.cleanup = AsyncMock()
        with pytest.raises(typer.Exit) as excinfo, patch.object(
            account, "TelegramAuth", return_value=auth
        ):
            asyncio.run(account.send_code_impl("config.yaml", phone=PHONE, api_id=1, api_hash="h"))
        _assert_exit(excinfo, capsys, "Error sending auth code")

    def test_verify_code_without_pending_auth(self, cli_env, capsys):
        from tg_agent.cli.commands.account import verify_code_impl

        with pytest.raises(typer.Exit) as excinfo:
            asyncio.run(
                verify_code_impl("config.yaml", phone=PHONE, code="12345", api_id=0, api_hash=None)
            )
        _assert_exit(excinfo, capsys, "No pending auth")


# ---------------------------------------------------------------------------
# settings / provider / export / scheduler / filter / photo-loader
# ---------------------------------------------------------------------------


class TestSettingsExitCodes:
    @pytest.mark.parametrize(
        ("kwargs", "fragment"),
        [
            ({"min_uniqueness": 150.0}, "Invalid --min-uniqueness"),
            ({"min_sub_ratio": 0.0}, "Invalid --min-sub-ratio"),
            ({"max_cross_dupe": -1.0}, "Invalid --max-cross-dupe"),
            ({"min_cyrillic": 200.0}, "Invalid --min-cyrillic"),
        ],
    )
    def test_filter_criteria_invalid_value(self, kwargs, fragment, capsys):
        from tg_agent.cli.commands.settings import filter_criteria_impl

        with pytest.raises(typer.Exit) as excinfo:
            asyncio.run(filter_criteria_impl("config.yaml", **kwargs))
        _assert_exit(excinfo, capsys, fragment)

    def test_filter_criteria_stdout_stays_clean(self, capsys):
        """The refusal goes to stderr — stdout stays clean for results."""
        from tg_agent.cli.commands.settings import filter_criteria_impl

        with pytest.raises(typer.Exit):
            asyncio.run(filter_criteria_impl("config.yaml", min_uniqueness=150.0))
        assert capsys.readouterr().out == ""


class TestProviderExitCodes:
    def test_add_unknown_provider(self, cli_env, capsys):
        from tg_agent.cli.commands.provider import add_impl

        with pytest.raises(typer.Exit) as excinfo:
            asyncio.run(add_impl("config.yaml", name="nope", api_key="k"))
        _assert_exit(excinfo, capsys, "Unknown provider: nope")

    def test_delete_without_encryption_key(self, cli_env, capsys):
        """cli_db has no SESSION_ENCRYPTION_KEY env — provider writes are refused."""
        from tg_agent.cli.commands.provider import delete_impl

        with pytest.raises(typer.Exit) as excinfo:
            asyncio.run(delete_impl("config.yaml", name="openai"))
        _assert_exit(excinfo, capsys, "SESSION_ENCRYPTION_KEY is required to manage providers")

    def test_probe_unconfigured_provider(self, cli_env, capsys):
        from tg_agent.cli.commands.provider import probe_impl

        with pytest.raises(typer.Exit) as excinfo:
            asyncio.run(probe_impl("config.yaml", name="openai"))
        _assert_exit(excinfo, capsys, "not configured")


class TestExportExitCodes:
    def test_telegram_requires_channel_id(self, cli_env, capsys):
        from tg_agent.cli.commands.export import telegram_impl

        with pytest.raises(typer.Exit) as excinfo:
            asyncio.run(telegram_impl("config.yaml", channel_id=None))
        _assert_exit(excinfo, capsys, "--channel-id is required")

    def test_unknown_format(self, cli_env, capsys):
        from tg_agent.cli.commands.export import export_impl

        asyncio.run(
            cli_env.insert_message(
                Message(
                    channel_id=100,
                    message_id=1,
                    text="seed",
                    date=datetime(2026, 1, 1, tzinfo=timezone.utc),
                )
            )
        )
        with pytest.raises(typer.Exit) as excinfo:
            asyncio.run(export_impl("config.yaml", fmt="bogus"))
        _assert_exit(excinfo, capsys, "Unknown format: bogus")


class TestSchedulerExitCodes:
    def test_set_interval_unknown_job(self, cli_env, capsys):
        from tg_agent.cli.commands.scheduler import set_interval_impl

        with pytest.raises(typer.Exit) as excinfo:
            asyncio.run(set_interval_impl("config.yaml", job_id="bogus", minutes=30))
        _assert_exit(excinfo, capsys, "Unknown job_id 'bogus'")

    def test_set_interval_missing_search_query(self, cli_env, capsys):
        from tg_agent.cli.commands.scheduler import set_interval_impl

        with pytest.raises(typer.Exit) as excinfo:
            asyncio.run(set_interval_impl("config.yaml", job_id="sq_999", minutes=30))
        _assert_exit(excinfo, capsys, "Search query 999 not found")


class TestFilterExitCodes:
    @pytest.mark.parametrize(
        ("kwargs", "fragment"),
        [
            ({"pk": 1, "set_flag": None, "unset_flag": None}, "exactly one action"),
            ({"pk": 1, "set_flag": "bogus", "unset_flag": None}, "Unknown operator flag"),
        ],
    )
    def test_flag_usage_errors(self, kwargs, fragment, capsys):
        from tg_agent.cli.commands.filter import flag_impl

        with pytest.raises(typer.Exit) as excinfo:
            asyncio.run(flag_impl("config.yaml", **kwargs))
        _assert_exit(excinfo, capsys, fragment)


class TestPhotoLoaderExitCodes:
    def test_auto_toggle_missing_job(self, capsys):
        from tg_agent.cli.commands import photo_loader

        async def fake_run(config_path, body):
            services = SimpleNamespace(
                auto=SimpleNamespace(get_job=AsyncMock(return_value=None))
            )
            await body(services)

        with pytest.raises(typer.Exit) as excinfo, patch.object(
            photo_loader, "_run_with_services", new=fake_run
        ):
            asyncio.run(photo_loader.auto_toggle_impl("config.yaml", job_id=5))
        _assert_exit(excinfo, capsys, "Auto job not found")
