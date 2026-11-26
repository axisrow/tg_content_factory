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
from functools import partial
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import typer
from telethon_floodgate import FloodWaitInfo, HandledFloodWaitError

from tg_agent.cli.commands import (
    account,
    agent,
    analytics,
    channel,
    collect,
    dialogs,
    photo_loader,
    ping,
    scheduler,
    search_query,
    translate,
)
from tg_agent.cli.commands import filter as filter_commands
from tg_agent.cli.pipeline import crud, generation, graph, moderation, responses
from tg_agent.cli.pipeline import requests as pipeline_requests
from tg_agent.config import AppConfig
from tg_agent.database.bundles import PipelineBundle, SearchQueryBundle
from tg_agent.models import Message
from tg_agent.services.pipeline_service import PipelineService
from tg_agent.services.search_query_service import SearchQueryService
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

    def test_send_partial_failure_names_what_went_out_and_exits_1(self, capsys):
        """#1943: some files live, some failed — the report says which, and exits 1."""
        from tg_agent.cli.commands import photo_loader

        item = SimpleNamespace(
            id=157,
            status="completed",
            error="partial send failure: [Errno 13] Permission denied: 'b.png'",
            telegram_message_ids=[635],
            file_paths=["a.png", "b.png"],
        )

        async def fake_run(config_path, body):
            services = SimpleNamespace(
                pool=None, tasks=SimpleNamespace(send_now=AsyncMock(return_value=item))
            )
            await body(services)

        with pytest.raises(typer.Exit) as excinfo, patch.object(
            photo_loader, "_run_with_services", new=fake_run
        ):
            asyncio.run(
                photo_loader.send_impl("config.yaml", phone="+1", target="100", files=["a.png", "b.png"])
            )
        assert excinfo.value.exit_code == 1
        captured = capsys.readouterr()
        assert "1 of 2 file(s) published, message ids [635]" in captured.err
        assert "Permission denied" in captured.err
        assert captured.out == ""

    def test_send_clean_item_keeps_the_success_line(self, capsys):
        from tg_agent.cli.commands import photo_loader

        item = SimpleNamespace(id=7, status="completed", error=None)

        async def fake_run(config_path, body):
            services = SimpleNamespace(
                pool=None, tasks=SimpleNamespace(send_now=AsyncMock(return_value=item))
            )
            await body(services)

        with patch.object(photo_loader, "_run_with_services", new=fake_run):
            asyncio.run(photo_loader.send_impl("config.yaml", phone="+1", target="100", files=["a.png"]))
        assert "Sent photo item #7 status=completed" in capsys.readouterr().out

    def test_items_listing_shows_the_stored_error(self, capsys):
        """#1943: the reason of a partial send is the part a reader needs."""
        from tg_agent.cli.commands import photo_loader

        items = [
            SimpleNamespace(
                id=157, batch_id=124, phone="+1", target_dialog_id=100, status="completed",
                error="partial send failure: boom",
            ),
            SimpleNamespace(
                id=158, batch_id=124, phone="+1", target_dialog_id=100, status="cancelled", error=None
            ),
        ]

        async def fake_run(config_path, body):
            services = SimpleNamespace(tasks=SimpleNamespace(list_items=AsyncMock(return_value=items)))
            await body(services)

        with patch.object(photo_loader, "_run_with_services", new=fake_run):
            asyncio.run(photo_loader.items_impl("config.yaml"))
        partial, clean, *rest = capsys.readouterr().out.splitlines()
        assert partial == "#157 batch=124 phone=+1 target=100 status=completed error=partial send failure: boom"
        assert clean == "#158 batch=124 phone=+1 target=100 status=cancelled"
        assert rest == []


# ---------------------------------------------------------------------------
# a missing target is a failure, not a no-op with exit 0 (#1933)
# ---------------------------------------------------------------------------


def _assert_exit_clean(excinfo, capsys, fragment: str) -> None:
    """Exit 1, the reason on stderr, nothing on stdout."""
    assert excinfo.value.exit_code == 1
    captured = capsys.readouterr()
    assert fragment in captured.err
    assert captured.out == ""


async def _drive(handler, command, db):
    """Call a pipeline handler the way the dispatcher does: real store, real service."""
    svc = PipelineService(PipelineBundle.from_database(db))
    await handler(command, db, AppConfig(), svc)


def _pipeline_driver(handler, command):
    return partial(_drive, handler, command)


class TestMissingTargetExitCodes:
    """A write or action command aimed at a target that does not exist exits 1 (#1933).

    Written red against the real store: 23 commands were run with an id absent from
    every table; 21 exited 0 and five printed a success line for a change that never
    happened. Each driver runs the real ``*_impl`` body over an empty ``cli_db``.
    """

    @pytest.mark.parametrize(
        ("fragment", "driver"),
        [
            pytest.param(
                "Search query id=999 not found",
                lambda db: search_query.get_impl("c", query_id=999),
                id="search-query get",
            ),
            pytest.param(
                "Search query id=999 not found",
                lambda db: search_query.edit_impl("c", query_id=999, query="x"),
                id="search-query edit",
            ),
            pytest.param(
                "Search query id=999 not found",
                lambda db: search_query.delete_impl("c", query_id=999),
                id="search-query delete",
            ),
            pytest.param(
                "Tag 'ghost' not found",
                lambda db: channel.tag_delete_impl(db, name="ghost"),
                id="channel tag delete",
            ),
            pytest.param(
                "Channel pk=999 not found",
                lambda db: channel.tag_set_impl(db, pk=999, tags="x"),
                id="channel tag set",
            ),
            pytest.param(
                "Channel pk=999 not found",
                lambda db: channel.tag_get_impl(db, pk=999),
                id="channel tag get",
            ),
            pytest.param(
                "Channel '999' not found",
                lambda db: channel.delete_impl("c", identifier="999"),
                id="channel delete",
            ),
            pytest.param(
                "Channel '999' not found",
                lambda db: channel.toggle_impl("c", identifier="999"),
                id="channel toggle",
            ),
            pytest.param(
                "Channel '999' not found",
                lambda db: channel.review_confirm_impl("c", identifier="999"),
                id="channel review-confirm",
            ),
            pytest.param(
                "Channel '999' not found",
                lambda db: channel.review_keep_impl("c", identifier="999"),
                id="channel review-keep",
            ),
            pytest.param(
                "Channel '999' not found",
                lambda db: channel.stats_impl(
                    "c", all_channels=False, identifier="999", max_channels=None
                ),
                id="channel stats",
            ),
            pytest.param(
                "Channel '999' not found",
                lambda db: channel._handoff_collect_to_worker(
                    db, identifier="999", full=False, wait=False
                ),
                id="channel collect",
            ),
            pytest.param(
                "Task 999 not found or already completed.",
                lambda db: scheduler.task_cancel_impl("c", task_id=999),
                id="scheduler task-cancel",
            ),
            pytest.param(
                "Command #999 not found or not pending",
                lambda db: dialogs._dialogs_queue(
                    _ns(queue_action="cancel", command_id=999, yes=True), db, None
                ),
                id="dialogs queue cancel",
            ),
            pytest.param(
                "Pipeline id=999 not found",
                _pipeline_driver(crud._pipeline_edit, pipeline_requests.PipelineEditCommand(id=999)),
                id="pipeline edit",
            ),
            pytest.param(
                "Pipeline id=999 not found",
                _pipeline_driver(crud._pipeline_toggle, pipeline_requests.PipelineToggleCommand(id=999)),
                id="pipeline toggle",
            ),
            pytest.param(
                "Pipeline id=999 not found",
                _pipeline_driver(crud._pipeline_delete, pipeline_requests.PipelineDeleteCommand(id=999)),
                id="pipeline delete",
            ),
            pytest.param(
                "Pipeline id=999 not found",
                _pipeline_driver(
                    crud._pipeline_refinement_steps, pipeline_requests.PipelineRefinementStepsCommand(id=999)
                ),
                id="pipeline refinement-steps",
            ),
            pytest.param(
                "Pipeline id=999 not found",
                _pipeline_driver(crud._pipeline_export, pipeline_requests.PipelineExportCommand(id=999)),
                id="pipeline export",
            ),
            pytest.param(
                "Ping monitor #999 not found",
                lambda db: ping.delete_impl("c", monitor_id=999),
                id="ping delete",
            ),
            pytest.param(
                "Тред #999 не найден.",
                lambda db: agent.thread_rename_impl("c", thread_id=999, title="x"),
                id="agent thread-rename",
            ),
        ],
    )
    def test_missing_target_exits_1(self, cli_env, capsys, fragment, driver):
        with pytest.raises(typer.Exit) as excinfo:
            asyncio.run(driver(cli_env))
        _assert_exit_clean(excinfo, capsys, fragment)

    def test_photo_batch_cancel_miss(self, capsys):
        async def fake_run(config_path, body):
            services = SimpleNamespace(
                tasks=SimpleNamespace(cancel_item=AsyncMock(return_value=False))
            )
            await body(services)

        with pytest.raises(typer.Exit) as excinfo, patch.object(
            photo_loader, "_run_with_services", new=fake_run
        ):
            asyncio.run(photo_loader.batch_cancel_impl("config.yaml", item_id=999))
        _assert_exit_clean(excinfo, capsys, "not cancelled")

    def test_search_query_add_invalid_regex_exits_1(self, cli_env, capsys):
        # A refused write is a failure, not a printed error with exit 0 (#1942).
        with pytest.raises(typer.Exit) as excinfo:
            asyncio.run(search_query.add_impl("c", query="[", is_regex=True))
        _assert_exit_clean(excinfo, capsys, "invalid regex")


# ---------------------------------------------------------------------------
# second wave (#1951): the handlers the first fix did not reach
# ---------------------------------------------------------------------------


def _render(call):
    """Wrap a synchronous renderer for the driver table."""

    async def _run(db):
        call()

    return _run


def _collect_driver(channel_id: int):
    async def _run(db):
        # Next to a running daemon the command only looks the channel up.
        with patch(
            "tg_agent.cli.commands.collect.worker_handoff.serve_is_running", return_value=True
        ):
            await collect.collect_impl("c", channel_id=channel_id)

    return _run


def _scratch_run(body):
    """Run ``body(db, run_id)`` with a generation run that belongs to no pipeline."""

    async def _run(db):
        run_id = await db.repos.generation_runs.create_run(None, "prompt")
        await body(db, run_id)

    return _run


# A pipeline holding ``source_0``, ``llm_generate_0`` and ``publish_0`` in a row.
DAG_GRAPH = {
    "nodes": [
        {"id": "source_0", "type": "source", "name": "source", "config": {}},
        {"id": "llm_generate_0", "type": "llm_generate", "name": "llm_generate", "config": {"prompt_template": "x"}},
        {"id": "publish_0", "type": "publish", "name": "publish", "config": {}},
    ],
    "edges": [
        {"from": "source_0", "to": "llm_generate_0"},
        {"from": "llm_generate_0", "to": "publish_0"},
    ],
}


async def _seed_pipeline(db, pipeline_json: dict | None = None) -> int:
    """A pipeline with no sources and no targets — ``import_json`` skips their validation."""
    svc = PipelineService(PipelineBundle.from_database(db))
    return await svc.import_json(
        {"name": "P", "prompt_template": "x", "source_ids": [], "target_refs": [], "pipeline_json": pipeline_json or {}}
    )


class TestMissingTargetExitCodesWave2:
    """A missing target exits 1 in the handlers the first fix did not reach (#1951).

    Written red against the real store: 36 commands ran with an id absent from
    every table; 34 printed the miss on stdout and 33 of them exited 0. Each
    driver runs the real handler body over an empty ``cli_db``.
    """

    @pytest.mark.parametrize(
        ("fragment", "driver"),
        [
            pytest.param(
                "Pipeline id=999 not found",
                _pipeline_driver(graph._pipeline_filter, pipeline_requests.PipelineFilterSetCommand(id=999)),
                id="pipeline filter set",
            ),
            pytest.param(
                "Pipeline id=999 not found",
                _pipeline_driver(graph._pipeline_filter, pipeline_requests.PipelineFilterShowCommand(id=999)),
                id="pipeline filter show",
            ),
            pytest.param(
                "Pipeline id=999 not found",
                _pipeline_driver(graph._pipeline_filter, pipeline_requests.PipelineFilterClearCommand(id=999)),
                id="pipeline filter clear",
            ),
            pytest.param(
                "Pipeline id=999 not found",
                _pipeline_driver(
                    graph._pipeline_node,
                    pipeline_requests.PipelineNodeRemoveCommand(pipeline_id=999, node_id="n1"),
                ),
                id="pipeline node remove",
            ),
            pytest.param(
                "Pipeline id=999 not found",
                _pipeline_driver(
                    graph._pipeline_node,
                    pipeline_requests.PipelineNodeReplaceCommand(pipeline_id=999, node_id="n1", node_spec="source"),
                ),
                id="pipeline node replace",
            ),
            pytest.param(
                "Pipeline id=999 not found",
                _pipeline_driver(
                    graph._pipeline_node, pipeline_requests.PipelineNodeAddCommand(pipeline_id=999, node_spec="source")
                ),
                id="pipeline node add",
            ),
            pytest.param(
                "Pipeline id=999 not found",
                _pipeline_driver(
                    graph._pipeline_edge,
                    pipeline_requests.PipelineEdgeAddCommand(pipeline_id=999, from_node="a", to_node="b"),
                ),
                id="pipeline edge add",
            ),
            pytest.param(
                "Pipeline id=999 not found",
                _pipeline_driver(
                    graph._pipeline_edge,
                    pipeline_requests.PipelineEdgeRemoveCommand(pipeline_id=999, from_node="a", to_node="b"),
                ),
                id="pipeline edge remove",
            ),
            pytest.param(
                "Pipeline id=999 not found",
                _pipeline_driver(graph._pipeline_graph, pipeline_requests.PipelineGraphCommand(id=999)),
                id="pipeline graph",
            ),
            pytest.param(
                "Pipeline id=999 not found",
                _pipeline_driver(moderation._pipeline_queue, pipeline_requests.PipelineQueueCommand(id=999)),
                id="pipeline queue",
            ),
            pytest.param(
                "Run id=999 not found",
                _pipeline_driver(moderation._pipeline_approve, pipeline_requests.PipelineApproveCommand(run_id=999)),
                id="pipeline approve",
            ),
            pytest.param(
                "Run id=999 not found",
                _pipeline_driver(moderation._pipeline_reject, pipeline_requests.PipelineRejectCommand(run_id=999)),
                id="pipeline reject",
            ),
            pytest.param(
                "Run id=999 not found",
                _pipeline_driver(
                    moderation._pipeline_resolve_unconfirmed,
                    pipeline_requests.PipelineResolveUnconfirmedCommand(run_id=999, target="t"),
                ),
                id="pipeline resolve-unconfirmed",
            ),
            pytest.param(
                "Run id=999 not found",
                _pipeline_driver(moderation._pipeline_publish, pipeline_requests.PipelinePublishCommand(run_id=999)),
                id="pipeline publish",
            ),
            pytest.param(
                "Pipeline id=999 not found",
                _pipeline_driver(generation._pipeline_run, pipeline_requests.PipelineRunCommand(id=999)),
                id="pipeline run",
            ),
            pytest.param(
                "Pipeline id=999 not found",
                _pipeline_driver(generation._pipeline_generate, pipeline_requests.PipelineGenerateCommand(id=999)),
                id="pipeline generate",
            ),
            pytest.param(
                "Pipeline id=999 not found",
                _pipeline_driver(
                    generation._pipeline_generate_stream,
                    pipeline_requests.PipelineGenerateStreamCommand(id=999),
                ),
                id="pipeline generate-stream",
            ),
            pytest.param(
                "Pipeline id=999 not found",
                _pipeline_driver(generation._pipeline_runs, pipeline_requests.PipelineRunsCommand(id=999)),
                id="pipeline runs",
            ),
            pytest.param(
                "Run id=999 not found",
                _pipeline_driver(generation._pipeline_variants, pipeline_requests.PipelineVariantsCommand(run_id=999)),
                id="pipeline variants",
            ),
            pytest.param(
                "999 not found",
                _pipeline_driver(
                    generation._pipeline_select_variant,
                    pipeline_requests.PipelineSelectVariantCommand(run_id=999, index=0),
                ),
                id="pipeline select-variant",
            ),
            pytest.param(
                # Never looked the run up: the service answered variant 0 for a missing one.
                "Run id=999 not found",
                _pipeline_driver(
                    generation._pipeline_auto_select, pipeline_requests.PipelineAutoSelectCommand(run_id=999)
                ),
                id="pipeline auto-select",
            ),
            pytest.param(
                "Pipeline id=999 not found",
                _render(
                    lambda: responses.render_pipeline_show(pipeline_requests.PipelineShowCommand(id=999), None)
                ),
                id="pipeline show",
            ),
            pytest.param(
                "Run id=999 not found",
                _render(
                    lambda: responses.render_pipeline_run_show(
                        pipeline_requests.PipelineRunShowCommand(run_id=999), None
                    )
                ),
                id="pipeline run-show",
            ),
            pytest.param(
                "Run id=999 not found",
                _pipeline_driver(
                    moderation._pipeline_moderation_view,
                    pipeline_requests.PipelineModerationViewCommand(run_id=999),
                ),
                id="pipeline moderation-view",
            ),
            pytest.param(
                "Channel pk=999 not found",
                lambda db: filter_commands.toggle_impl("c", pk=999),
                id="filter toggle",
            ),
            pytest.param(
                "Channel pk=999 not found",
                lambda db: filter_commands.flag_impl("c", pk=999, set_flag="manual"),
                id="filter flag",
            ),
            pytest.param("Channel 999 not found in DB", _collect_driver(999), id="collect --channel-id"),
            pytest.param(
                "Account id=999 not found",
                lambda db: account.toggle_impl("c", account_id=999),
                id="account toggle",
            ),
            pytest.param(
                "Account id=999 not found",
                lambda db: account.delete_impl("c", account_id=999),
                id="account delete",
            ),
            pytest.param(
                f"Account {PHONE} not found",
                lambda db: account.flood_clear_impl("c", phone=PHONE),
                id="account flood-clear",
            ),
            pytest.param(
                "Ping monitor #999 not found",
                lambda db: ping.update_impl(
                    "c", monitor_id=999, text=None, days=None, active=False, inactive=True
                ),
                id="ping update",
            ),
            pytest.param(
                "Channel 999 not found",
                lambda db: analytics.channel_impl("c", channel_id=999, days=7),
                id="analytics channel",
            ),
            pytest.param(
                "Message id=999 not found",
                lambda db: translate.message_impl("c", message_id=999),
                id="translate message",
            ),
            pytest.param(
                "Search query id=999 not found",
                lambda db: search_query.stats_impl("c", query_id=999),
                id="search-query stats",
            ),
        ],
    )
    def test_missing_target_exits_1(self, cli_env, capsys, fragment, driver):
        with pytest.raises(typer.Exit) as excinfo:
            asyncio.run(driver(cli_env))
        _assert_exit_clean(excinfo, capsys, fragment)


class TestRefusedArgumentExitCodesWave2:
    """A refused argument is a failure too — never `Error:` on stdout with exit 0 (#1951)."""

    def test_pipeline_node_add_invalid_spec(self, cli_env, capsys):
        driver = _pipeline_driver(
            graph._pipeline_node, pipeline_requests.PipelineNodeAddCommand(pipeline_id=999, node_spec="n1")
        )
        with pytest.raises(typer.Exit) as excinfo:
            asyncio.run(driver(cli_env))
        _assert_exit_clean(excinfo, capsys, "Invalid node spec")

    @pytest.mark.parametrize(
        ("kwargs", "fragment"),
        [
            (dict(name="x"), "--prompt-template is required"),
            (dict(name="x", prompt_template="p"), "--source is required"),
            (dict(name="x", prompt_template="p", source=[1]), "--target is required"),
            (dict(name="", prompt_template="p", source=[1], target=["phone|1"]), "не может быть пустым"),
        ],
    )
    def test_pipeline_add(self, cli_env, capsys, kwargs, fragment):
        driver = _pipeline_driver(crud._pipeline_add, pipeline_requests.PipelineAddCommand(**kwargs))
        with pytest.raises(typer.Exit) as excinfo:
            asyncio.run(driver(cli_env))
        _assert_exit_clean(excinfo, capsys, fragment)

    def test_pipeline_ai_edit_failure(self, cli_env, capsys):
        driver = _pipeline_driver(
            crud._pipeline_ai_edit, pipeline_requests.PipelineAiEditCommand(id=999, instruction="x")
        )
        with pytest.raises(typer.Exit) as excinfo:
            asyncio.run(driver(cli_env))
        _assert_exit_clean(excinfo, capsys, "не найден")

    def test_search_query_edit_invalid_regex(self, cli_env, capsys):
        async def _seed(db):
            return await SearchQueryService(SearchQueryBundle.from_database(db)).add("джомтьен*")

        query_id = asyncio.run(_seed(cli_env))
        with pytest.raises(typer.Exit) as excinfo:
            asyncio.run(search_query.edit_impl("c", query_id=query_id, query="[", is_regex=True))
        _assert_exit_clean(excinfo, capsys, "invalid regex")

    def test_ping_create_requires_a_target(self, cli_env, capsys):
        with pytest.raises(typer.Exit) as excinfo:
            asyncio.run(
                ping.create_impl(
                    "c", phone=PHONE, dialog_id=None, username=None, text="x", days=3, title=None
                )
            )
        _assert_exit_clean(excinfo, capsys, "--dialog-id or --username is required")

    def test_dialogs_react_requires_an_emoji(self, capsys):
        pool = MagicMock()
        pool._get_account_for_phone = AsyncMock(return_value=None)
        with patch.object(dialogs, "_resolve_phone", new=AsyncMock(return_value=PHONE)):
            with pytest.raises(typer.Exit) as excinfo:
                asyncio.run(dialogs._dialogs_react(_ns(clear=False, emoji=None), None, pool))
        _assert_exit_clean(excinfo, capsys, "emoji is required")

    def test_resolve_unconfirmed_names_an_untouched_target(self, cli_env, capsys):
        async def body(db, run_id):
            command = pipeline_requests.PipelineResolveUnconfirmedCommand(run_id=run_id, target="t")
            await _drive(moderation._pipeline_resolve_unconfirmed, command, db)

        with pytest.raises(typer.Exit) as excinfo:
            asyncio.run(_scratch_run(body)(cli_env))
        _assert_exit_clean(excinfo, capsys, "is not unconfirmed")

    def test_publish_names_a_run_without_pipeline(self, cli_env, capsys):
        async def body(db, run_id):
            await _drive(
                moderation._pipeline_publish,
                pipeline_requests.PipelinePublishCommand(run_id=run_id),
                db,
            )

        with pytest.raises(typer.Exit) as excinfo:
            asyncio.run(_scratch_run(body)(cli_env))
        _assert_exit_clean(excinfo, capsys, "has no pipeline")

    def test_publish_names_a_failed_target_and_closes_the_pool(self, cli_env, capsys):
        """A publish that failed is not a success line — and its pool is closed on the way out."""
        from tg_agent.services.publish_service import PublishResult

        pool = SimpleNamespace(clients={PHONE: object()}, disconnect_all=AsyncMock())
        publish_run = AsyncMock(return_value=[PublishResult(success=False, error="chat not found")])
        services: Any = SimpleNamespace(publish_service=lambda _db, _pool: SimpleNamespace(publish_run=publish_run))

        async def body(db):
            svc = PipelineService(PipelineBundle.from_database(db))
            pipeline_id = await _seed_pipeline(db)
            run_id = await db.repos.generation_runs.create_run(pipeline_id, "prompt")
            with patch(
                "tg_agent.cli.pipeline.moderation.runtime.init_pool",
                new=AsyncMock(return_value=(AppConfig(), pool)),
            ):
                await moderation._pipeline_publish(
                    pipeline_requests.PipelinePublishCommand(run_id=run_id),
                    db,
                    AppConfig(),
                    svc,
                    services=services,
                )

        with pytest.raises(typer.Exit) as excinfo:
            asyncio.run(body(cli_env))
        captured = capsys.readouterr()
        assert excinfo.value.exit_code == 1
        assert captured.out == ""
        assert "Failed to publish run id=" in captured.err
        assert "chat not found" in captured.err
        pool.disconnect_all.assert_awaited_once()

    @pytest.mark.parametrize(
        ("kwargs", "fragment"),
        [
            (dict(name="x", node_specs=["source"], edge=["bad"]), "Invalid edge format"),
            (dict(name="x", node_specs=["source"], node_configs=["source_0=["]), "Invalid JSON in --node-config"),
        ],
    )
    def test_pipeline_add_graph_validation(self, cli_env, capsys, kwargs, fragment):
        driver = _pipeline_driver(crud._pipeline_add, pipeline_requests.PipelineAddCommand(**kwargs))
        with pytest.raises(typer.Exit) as excinfo:
            asyncio.run(driver(cli_env))
        _assert_exit_clean(excinfo, capsys, fragment)

    @pytest.mark.parametrize(
        ("steps_json", "fragment"),
        [("[", "Invalid JSON:"), ('{"step": 1}', "must be a JSON array")],
    )
    def test_refinement_steps_refuse_bad_json(self, cli_env, capsys, steps_json, fragment):
        async def body(db):
            command = pipeline_requests.PipelineRefinementStepsCommand(
                id=await _seed_pipeline(db), steps_json=steps_json
            )
            await _drive(crud._pipeline_refinement_steps, command, db)

        with pytest.raises(typer.Exit) as excinfo:
            asyncio.run(body(cli_env))
        _assert_exit_clean(excinfo, capsys, fragment)

    def test_pipeline_import_names_the_validation_error(self, cli_env, capsys, tmp_path):
        bad = tmp_path / "pipeline.json"
        bad.write_text('{"name": "", "prompt_template": "x"}', encoding="utf-8")
        driver = _pipeline_driver(crud._pipeline_import, pipeline_requests.PipelineImportCommand(file=str(bad)))
        with pytest.raises(typer.Exit) as excinfo:
            asyncio.run(driver(cli_env))
        _assert_exit_clean(excinfo, capsys, "Validation error:")

    def test_pipeline_from_template_names_a_missing_template(self, cli_env, capsys):
        driver = _pipeline_driver(
            crud._pipeline_from_template,
            pipeline_requests.PipelineFromTemplateCommand(template_id=999, name="x"),
        )
        with pytest.raises(typer.Exit) as excinfo:
            asyncio.run(driver(cli_env))
        _assert_exit_clean(excinfo, capsys, "не найден")

    def test_ping_create_names_an_unresolvable_username(self, cli_env, capsys):
        pool = SimpleNamespace(resolve_any_entity=AsyncMock(return_value=None), disconnect_all=AsyncMock())
        with patch(
            "tg_agent.cli.commands.ping.runtime.init_pool",
            new=AsyncMock(return_value=(AppConfig(), pool)),
        ):
            with pytest.raises(typer.Exit) as excinfo:
                asyncio.run(
                    ping.create_impl(
                        "c", phone=PHONE, dialog_id=None, username="@ghost", text="x", days=3, title=None
                    )
                )
        _assert_exit_clean(excinfo, capsys, "could not resolve")

    @pytest.mark.parametrize(
        ("handler", "command_of", "fragment"),
        [
            pytest.param(
                graph._pipeline_node,
                lambda pid: pipeline_requests.PipelineNodeAddCommand(pipeline_id=pid, node_spec="source:id=source_0"),
                "Node 'source_0' already exists in pipeline id=",
                id="node add with a taken id",
            ),
            pytest.param(
                graph._pipeline_edge,
                lambda pid: pipeline_requests.PipelineEdgeAddCommand(
                    pipeline_id=pid, from_node="source_0", to_node="ghost"
                ),
                "Node 'ghost' not found in pipeline id=",
                id="edge add with an unknown endpoint",
            ),
            pytest.param(
                graph._pipeline_node,
                lambda pid: pipeline_requests.PipelineNodeReplaceCommand(
                    pipeline_id=pid, node_id="source_0", node_spec="publish:id=publish_0"
                ),
                "Node 'publish_0' already exists in pipeline id=",
                id="node replace onto a taken id",
            ),
        ],
    )
    def test_graph_edit_names_what_was_wrong(self, cli_env, capsys, handler, command_of, fragment):
        """A refused graph edit names the real reason — a node that exists is not "not found" (#1951)."""

        async def body(db):
            await _drive(handler, command_of(await _seed_pipeline(db, DAG_GRAPH)), db)

        with pytest.raises(typer.Exit) as excinfo:
            asyncio.run(body(cli_env))
        _assert_exit_clean(excinfo, capsys, fragment)

    def test_bulk_approve_with_every_id_missing(self, cli_env, capsys):
        driver = _pipeline_driver(
            moderation._pipeline_bulk_approve, pipeline_requests.PipelineBulkApproveCommand(run_ids=[999])
        )
        with pytest.raises(typer.Exit) as excinfo:
            asyncio.run(driver(cli_env))
        captured = capsys.readouterr()
        assert excinfo.value.exit_code == 1
        assert captured.out == "Bulk approved: 0/1\n"
        assert "run id=999 not found" in captured.err


# ---------------------------------------------------------------------------
# third wave (#1955): the filter commands and the gateway account branches
# ---------------------------------------------------------------------------

_FILTER = "tg_agent.cli.commands.filter"


async def _seed_channel(db, channel_id: int) -> int:
    from tg_agent.models import Channel

    return await db.add_channel(Channel(channel_id=channel_id, title="Scratch"))


class TestFilterMissExitCodes:
    """A filter command aimed at nothing, or at less of a list than it was given, fails (#1955).

    None of these bodies is run against the real store: `purge` and `hard-delete` delete.
    """

    @pytest.mark.parametrize("pks", ["abc", "12,abc"])
    def test_a_malformed_pks_list_is_refused_as_a_whole(self, cli_env, capsys, pks):
        with patch(f"{_FILTER}.ChannelAnalyzer") as analyzer:
            analyzer.return_value.reset_filters_for_pks = AsyncMock()
            with pytest.raises(typer.Exit) as excinfo:
                asyncio.run(filter_commands.reset_impl("c", pks=pks))
        # The bad token is named, and the good one next to it is not acted on either.
        _assert_exit_clean(excinfo, capsys, "Invalid pk(s) in --pks: abc")
        analyzer.return_value.reset_filters_for_pks.assert_not_awaited()

    def test_an_empty_pks_list_is_refused(self, cli_env, capsys):
        with pytest.raises(typer.Exit) as excinfo:
            asyncio.run(filter_commands.reset_impl("c", pks=" , "))
        _assert_exit_clean(excinfo, capsys, "No valid PKs provided")

    @pytest.mark.parametrize(
        ("impl", "kwargs"),
        [
            pytest.param(filter_commands.reset_impl, {}, id="filter reset"),
            pytest.param(filter_commands.purge_impl, {"yes": False}, id="filter purge"),
            pytest.param(filter_commands.hard_delete_impl, {"yes": False}, id="filter hard-delete"),
        ],
    )
    def test_a_missing_pk_stops_the_command_before_anything_runs(self, cli_env, capsys, impl, kwargs):
        """The one pk the store lacks is named; the prompt, the service and the analyzer never run."""
        asyncio.run(cli_env.set_setting("agent_dev_mode_enabled", "1"))  # so `hard-delete` reaches the lookup
        kept = asyncio.run(_seed_channel(cli_env, 4242))

        with (
            patch(f"{_FILTER}.sync_prompt", side_effect=AssertionError("prompted")) as prompt,
            patch(f"{_FILTER}._build_deletion_service") as build,
            patch(f"{_FILTER}.ChannelAnalyzer") as analyzer,
        ):
            analyzer.return_value.reset_filters_for_pks = AsyncMock()
            with pytest.raises(typer.Exit) as excinfo:
                asyncio.run(impl("c", pks=f"{kept},999", **kwargs))
        _assert_exit_clean(excinfo, capsys, "Channel pk(s) not found: 999")
        prompt.assert_not_called()
        build.assert_not_called()
        analyzer.return_value.reset_filters_for_pks.assert_not_awaited()

    def test_purge_messages_names_a_missing_channel(self, cli_env, capsys):
        with patch(f"{_FILTER}.sync_prompt", side_effect=AssertionError("prompted")) as prompt:
            with pytest.raises(typer.Exit) as excinfo:
                asyncio.run(filter_commands.purge_messages_impl("c", channel_id=999, yes=False))
        _assert_exit_clean(excinfo, capsys, "Channel 999 not found")
        prompt.assert_not_called()

    def test_hard_delete_without_developer_mode_exits_1(self, cli_env, capsys):
        with pytest.raises(typer.Exit) as excinfo:
            asyncio.run(filter_commands.hard_delete_impl("c", pks=None, yes=True))
        _assert_exit_clean(excinfo, capsys, "requires developer mode")

    def test_a_failed_channel_fails_the_purge(self, cli_env, capsys):
        """The report keeps its `Errors` block on stdout; the command itself does not pass."""
        from tg_agent.services.filter_deletion_service import PurgeResult

        pk = asyncio.run(_seed_channel(cli_env, 4242))
        result = PurgeResult(purged_count=1, purged_titles=["Scratch"], errors=["pk=7: disk I/O error"])
        service = SimpleNamespace(purge_channels_by_pks=AsyncMock(return_value=result))
        with patch(f"{_FILTER}._build_deletion_service", return_value=service):
            with pytest.raises(typer.Exit) as excinfo:
                asyncio.run(filter_commands.purge_impl("c", pks=str(pk), yes=True))
        captured = capsys.readouterr()
        assert excinfo.value.exit_code == 1
        assert "Purged messages from 1 channels" in captured.out
        assert "disk I/O error" in captured.err
