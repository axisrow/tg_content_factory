"""`dialogs` actions must go through the command queue while a server is running.

Same root cause as the collection hand-off: `serve` owns every
``data/telegram_sessions/<phone>.session`` file, so a CLI process that builds its
own ``ClientPool`` opens them a second time and risks ``database is locked``.

The ``telegram_commands`` queue already covers these operations — the web UI
enqueues ``dialogs.send`` and friends, and ``TelegramCommandDispatcher`` executes
them inside the worker. These tests pin the CLI to that same queue.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.cli.commands.dialogs import run_with_dependencies
from src.cli.worker_handoff import serve_is_running as _real_serve_is_running
from src.config import AppConfig
from tests.helpers import cli_ns as _ns

pytestmark = pytest.mark.aiosqlite_serial


def _mock_pool():
    pool = MagicMock()
    pool.clients = {"+1234567890": MagicMock()}
    pool.disconnect_all = AsyncMock()
    return pool


def _run(args, cli_db, *, serve_running: bool, pool=None):
    config = AppConfig()
    used_pool = pool or _mock_pool()

    async def fake_init_db(_):
        return config, cli_db

    async def fake_init_pool(_, __):
        from src.telegram.auth import TelegramAuth

        return TelegramAuth(0, ""), used_pool

    with (
        patch("src.cli.commands.dialogs.runtime.init_db", side_effect=fake_init_db),
        patch("src.cli.commands.dialogs.runtime.init_pool", side_effect=fake_init_pool) as init_pool,
        patch(
            "src.cli.commands.dialogs.worker_handoff.serve_is_running",
            return_value=serve_running,
        ),
    ):
        run_with_dependencies(args)
    return init_pool


def _commands(db):
    # The dispatcher closes the DB in its `finally`; tests reopen to inspect it.
    if db._connection.db is None:
        asyncio.run(db.initialize())
    return asyncio.run(db.repos.telegram_commands.list_commands(limit=10))


def test_send_enqueues_command_when_serve_is_running(cli_db, capsys):
    """`dialogs send` must queue the message rather than open its own connection."""
    init_pool = _run(
        _ns(
            dialogs_action="send",
            phone="+1234567890",
            recipient="100111",
            text="hello",
            yes=True,
            direct=False,
        ),
        cli_db,
        serve_running=True,
    )

    init_pool.assert_not_called()

    commands = _commands(cli_db)
    assert [c.command_type for c in commands] == ["dialogs.send"]
    assert commands[0].payload["recipient"] == "100111"
    assert commands[0].payload["text"] == "hello"
    assert commands[0].payload["phone"] == "+1234567890"

    assert "очередь" in capsys.readouterr().out.lower()


def test_join_enqueues_command_when_serve_is_running(cli_db, capsys):
    """A second action proves the mapping is table-driven, not special-cased."""
    _run(
        _ns(dialogs_action="join", phone="+1234567890", target="@somechannel", yes=True, direct=False),
        cli_db,
        serve_running=True,
    )

    commands = _commands(cli_db)
    assert [c.command_type for c in commands] == ["dialogs.join"]
    assert commands[0].payload["target"] == "@somechannel"


def test_send_runs_directly_when_serve_is_not_running(cli_db):
    """Offline behaviour is unchanged — the pool is still built and used."""
    pool = _mock_pool()
    pool.get_native_client_by_phone = AsyncMock(return_value=None)

    init_pool = _run(
        _ns(
            dialogs_action="send",
            phone="+1234567890",
            recipient="100111",
            text="hello",
            yes=True,
            direct=False,
        ),
        cli_db,
        serve_running=False,
        pool=pool,
    )

    init_pool.assert_called_once()
    assert _commands(cli_db) == []


def test_direct_flag_bypasses_the_queue(cli_db):
    """`--direct` keeps the escape hatch consistent with `channel collect`."""
    pool = _mock_pool()
    pool.get_native_client_by_phone = AsyncMock(return_value=None)

    init_pool = _run(
        _ns(
            dialogs_action="send",
            phone="+1234567890",
            recipient="100111",
            text="hello",
            yes=True,
            direct=True,
        ),
        cli_db,
        serve_running=True,
        pool=pool,
    )

    init_pool.assert_called_once()
    assert _commands(cli_db) == []


def test_read_only_actions_still_run_locally(cli_db):
    """Listing and cache inspection have no queue equivalent and must not hang.

    Only actions with a real ``dialogs.*`` command type are handed over; local
    read-only views keep working against the DB as before.
    """
    pool = _mock_pool()
    pool.get_dialogs_for_phone = AsyncMock(return_value=[])

    init_pool = _run(
        _ns(dialogs_action="cache-status", phone="", direct=False),
        cli_db,
        serve_running=True,
        pool=pool,
    )

    init_pool.assert_called_once()
    assert _commands(cli_db) == []


def test_edit_admin_handoff_preserves_is_admin_and_title(cli_db):
    """Promotion must not turn into demotion when handed off to the worker.

    Regression (Codex, PR #1324 round 1): `_HANDOFF_COMMANDS["edit-admin"]`
    previously forwarded only `chat_id` and `user_id`. The CLI defaults
    `is_admin=True` (promote), but the worker's `_handle_dialogs_edit_admin`
    reads a missing `is_admin` key as `False` (demote) via
    `payload.get("is_admin", False)`. So a confirmed promotion, queued while
    `serve` is running, was silently executed as a demotion, and any `--title`
    was discarded outright.
    """
    _run(
        _ns(
            dialogs_action="edit-admin",
            phone="+1234567890",
            chat_id="-100123",
            user_id="555",
            title="Moderator",
            is_admin=True,
            yes=True,
            direct=False,
        ),
        cli_db,
        serve_running=True,
    )

    commands = _commands(cli_db)
    assert [c.command_type for c in commands] == ["dialogs.edit_admin"]
    payload = commands[0].payload
    assert payload["chat_id"] == "-100123"
    assert payload["user_id"] == "555"
    assert payload["is_admin"] is True
    assert payload["title"] == "Moderator"


def test_edit_admin_handoff_preserves_demotion(cli_db):
    """The inverse must also round-trip: --no-admin must not vanish either."""
    _run(
        _ns(
            dialogs_action="edit-admin",
            phone="+1234567890",
            chat_id="-100123",
            user_id="555",
            title=None,
            is_admin=False,
            yes=True,
            direct=False,
        ),
        cli_db,
        serve_running=True,
    )

    commands = _commands(cli_db)
    payload = commands[0].payload
    assert payload["is_admin"] is False


def test_edit_permissions_handoff_includes_all_fields(cli_db):
    """The worker handler requires user_id — a missing key is a guaranteed crash.

    Regression (Codex, PR #1324 round 2): `_HANDOFF_COMMANDS["edit-permissions"]`
    forwarded only `chat_id`, though the Typer command also collects `user_id`,
    `until_date`, `send_messages`, and `send_media`. The worker's
    `_handle_dialogs_edit_permissions` does `payload["user_id"]` — a direct
    subscript, no default — so every handed-off invocation failed with a
    KeyError instead of applying the permission change.
    """
    _run(
        _ns(
            dialogs_action="edit-permissions",
            phone="+1234567890",
            chat_id="-100123",
            user_id="555",
            until_date="2027-01-01T00:00:00",
            send_messages=True,
            send_media=False,
            yes=True,
            direct=False,
        ),
        cli_db,
        serve_running=True,
    )

    commands = _commands(cli_db)
    assert [c.command_type for c in commands] == ["dialogs.edit_permissions"]
    payload = commands[0].payload
    assert payload["chat_id"] == "-100123"
    assert payload["user_id"] == "555"
    assert payload["until_date"] == "2027-01-01T00:00:00"
    assert payload["send_messages"] is True
    assert payload["send_media"] is False


def test_edit_permissions_handoff_normalizes_string_false_to_bool_false(cli_db):
    """`bool("false")` is `True` in Python — a raw string must never reach the worker.

    Regression (Codex, PR #1324 round 3): `dialogs_edit_permissions` declares
    `send_messages`/`send_media` as `str | None` Typer options (free-text
    "true/false", not a paired boolean flag), so the real CLI hands
    `_handoff_dialog_action` the literal string "false" for
    `--send-messages false`. Without normalization the payload stored that
    string verbatim, and the worker's `bool(payload["send_messages"])`
    evaluated any non-empty string — including "false" — as `True`. A command
    meant to forbid sending messages/media was silently enqueued as allowing
    it. Uses raw strings (as the real Typer parse produces), not the
    `_ns()`-default Python bools other tests use — those mask this exact bug.
    """
    _run(
        _ns(
            dialogs_action="edit-permissions",
            phone="+1234567890",
            chat_id="-100123",
            user_id="555",
            until_date=None,
            send_messages="false",
            send_media="true",
            yes=True,
            direct=False,
        ),
        cli_db,
        serve_running=True,
    )

    commands = _commands(cli_db)
    payload = commands[0].payload
    assert payload["send_messages"] is False
    assert payload["send_media"] is True


def test_react_clear_handoff_preserves_clear_flag(cli_db):
    """`dialogs react --clear` must clear the reaction, not error out.

    Regression (Codex, PR #1324 round 3): `_HANDOFF_COMMANDS["react"]` omitted
    `clear`, so the queued payload carried `emoji=None` with no signal that
    the caller wanted to *clear* the reaction rather than set an (invalid,
    empty) one. The worker handler always called
    `normalize_outgoing_reaction_emoji("")`, which raises for an empty/missing
    emoji instead of taking the clear path the in-process handler already has.
    """
    _run(
        _ns(
            dialogs_action="react",
            phone="+1234567890",
            chat_id="-100123",
            message_id=42,
            emoji=None,
            clear=True,
            yes=True,
            direct=False,
        ),
        cli_db,
        serve_running=True,
    )

    commands = _commands(cli_db)
    assert [c.command_type for c in commands] == ["dialogs.react"]
    assert commands[0].payload["clear"] is True


def test_mark_read_handoff_includes_max_id(cli_db):
    """A bounded mark-read must not silently mark the whole dialog read.

    Regression (Codex, PR #1324 round 2): `_HANDOFF_COMMANDS["mark-read"]`
    forwarded only `chat_id`, dropping `--max-id`. The worker handler reads a
    missing `max_id` as ``None``, which marks every message in the dialog as
    read instead of only those up to the requested id.
    """
    _run(
        _ns(
            dialogs_action="mark-read",
            phone="+1234567890",
            chat_id="-100123",
            max_id=999,
            direct=False,
        ),
        cli_db,
        serve_running=True,
    )

    commands = _commands(cli_db)
    assert [c.command_type for c in commands] == ["dialogs.mark_read"]
    assert commands[0].payload["max_id"] == 999


def test_mark_read_handoff_does_not_require_confirmation(cli_db):
    """mark-read must not need `args.yes` — the in-process handler never asks either.

    Regression (Codex, PR #1324 round 2): `dialogs.mark_read` was listed in
    `_HANDOFF_NEEDS_CONFIRMATION`, but `dialogs_mark_read` never builds its
    Namespace with a `yes` attribute (unlike every other confirmed action).
    `_confirm_or_abort` does `if args.yes:` with no default, so every
    handed-off `mark-read` crashed with AttributeError before ever reaching
    the queue — and the in-process `_dialogs_mark_read` handler doesn't
    prompt for confirmation at all, so gating the hand-off path was a
    behavior mismatch, not a deliberate safety net.
    """
    # No `yes` attribute on the Namespace at all — mirrors the real CLI
    # command, which never builds one for mark-read.
    _run(
        _ns(dialogs_action="mark-read", phone="+1234567890", chat_id="-100123", max_id=None, direct=False),
        cli_db,
        serve_running=True,
    )

    commands = _commands(cli_db)
    assert [c.command_type for c in commands] == ["dialogs.mark_read"]


def test_archive_and_unarchive_handoff_do_not_require_confirmation(cli_db):
    """Same AttributeError-on-missing-yes gap as mark-read, for archive/unarchive.

    Regression (Codex, PR #1324 round 2 audit): `dialogs_archive` and
    `dialogs_unarchive` also never build a `yes` attribute, and their
    in-process handlers don't prompt either — so they were removed from
    `_HANDOFF_NEEDS_CONFIRMATION` alongside mark-read.
    """
    for action in ("archive", "unarchive"):
        if cli_db._connection.db is None:
            asyncio.run(cli_db.initialize())
        _run(
            _ns(dialogs_action=action, phone="+1234567890", chat_id="-100123", direct=False),
            cli_db,
            serve_running=True,
        )

    commands = _commands(cli_db)
    assert {c.command_type for c in commands} == {"dialogs.archive", "dialogs.unarchive"}


def test_handoff_resolves_phone_when_not_given(cli_db):
    """Omitting --phone must not queue an empty phone the worker will reject.

    Regression (Codex, PR #1324 round 2): the hand-off path built
    `payload["phone"]` from `getattr(args, "phone", "") or ""`, never
    resolving a default the way `_resolve_phone` does for the in-process
    path (first connected account, sorted). `TelegramActionService._client`
    rejects an empty, non-``allow_any`` phone outright, so every handed-off
    action run without `--phone` broke only while `serve` was running.
    """
    from src.models import Account

    asyncio.run(cli_db.add_account(Account(phone="+70000000002", session_string="sess-b")))
    asyncio.run(cli_db.add_account(Account(phone="+70000000001", session_string="sess-a")))

    _run(
        _ns(dialogs_action="join", phone=None, target="@somechannel", yes=True, direct=False),
        cli_db,
        serve_running=True,
    )

    commands = _commands(cli_db)
    assert commands[0].payload["phone"] == "+70000000001"


def test_confirmation_is_still_required_before_enqueue(cli_db):
    """Declining the prompt must not queue anything.

    The hand-off happens after the confirmation gate, so a refused action leaves
    no trace — otherwise "no" would still send the message, just later.
    """
    with patch("builtins.input", return_value="n"):
        _run(
            _ns(
                dialogs_action="send",
                phone="+1234567890",
                recipient="100111",
                text="hello",
                yes=False,
                direct=False,
            ),
            cli_db,
            serve_running=True,
        )

    assert _commands(cli_db) == []


def test_direct_env_var_is_honoured(monkeypatch):
    """`dialogs` shares one bridge across 27 leaves, so its escape hatch is env-based."""
    from src.cli import worker_handoff

    monkeypatch.delenv(worker_handoff.DIRECT_ENV_VAR, raising=False)
    assert worker_handoff.direct_requested() is False

    for value in ("1", "true", "YES"):
        monkeypatch.setenv(worker_handoff.DIRECT_ENV_VAR, value)
        assert worker_handoff.direct_requested() is True

    monkeypatch.setenv(worker_handoff.DIRECT_ENV_VAR, "0")
    assert worker_handoff.direct_requested() is False


def test_serve_is_running_survives_a_broken_pid_file(tmp_path, monkeypatch):
    """A malformed PID file must degrade to "no server", never crash the command."""
    from src.cli import worker_handoff

    config = AppConfig()
    bad_pid = tmp_path / "broken.pid"
    bad_pid.write_text("not-a-number\n", encoding="utf-8")
    monkeypatch.setattr(worker_handoff, "pid_file_path", lambda _config: bad_pid)

    assert worker_handoff.serve_is_running(config) is False


def test_serve_is_running_is_false_for_no_worker_split_deployment(tmp_path, monkeypatch):
    """`serve --no-worker` must not look like a valid hand-off target.

    Regression (Codex, PR #1324 round 2): `serve_is_running` only checked the
    command line for `src.main serve`, not whether `--no-worker` was passed.
    In the documented split-deployment mode ("For split deployments (Docker/k8s)
    pass --no-worker and run `worker` separately" — CLAUDE.md), a CLI process
    that hands off while only a workerless `serve` is running would queue
    commands nothing ever picks up, silently reporting success. Fail safe:
    treat this as "no reliable worker" so the caller falls back to the direct
    path instead of enqueuing into a black hole.

    Uses the real function (`_real_serve_is_running`, imported at module load
    before the autouse `no_running_server` fixture patches the module
    attribute) — that fixture exists precisely to stop tests from exercising
    this logic by accident, so it must be bypassed here on purpose.
    """
    from src.cli import process_control, worker_handoff

    config = AppConfig()
    pid_path = tmp_path / "serve.pid"
    pid_path.write_text("4321\n", encoding="utf-8")
    monkeypatch.setattr(worker_handoff, "pid_file_path", lambda _config: pid_path)

    with (
        patch.object(process_control, "is_process_alive", return_value=True),
        patch.object(process_control, "_process_command", return_value="python -m src.main serve --no-worker"),
    ):
        assert _real_serve_is_running(config) is False


def test_serve_is_running_is_true_for_plain_serve(tmp_path, monkeypatch):
    """Plain `serve` (the documented default: web + embedded worker) still hands off."""
    from src.cli import process_control, worker_handoff

    config = AppConfig()
    pid_path = tmp_path / "serve.pid"
    pid_path.write_text("4321\n", encoding="utf-8")
    monkeypatch.setattr(worker_handoff, "pid_file_path", lambda _config: pid_path)

    with (
        patch.object(process_control, "is_process_alive", return_value=True),
        patch.object(process_control, "_process_command", return_value="python -m src.main serve"),
    ):
        assert _real_serve_is_running(config) is True
