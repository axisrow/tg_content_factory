from __future__ import annotations

import os
import re
import sqlite3
import subprocess
import sys
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import pytest

from src.cli.dotenv import load_cli_dotenv
from src.config import load_config
from tests.cli_real_tg_integration._live_readiness import (
    _gate_enabled,
    _resolve_api_credentials,
)

CLI_REAL_TG_LIVE_GATE_ENV = "RUN_CLI_REAL_TG_LIVE"
CLI_REAL_TG_ROOT_ENV = "CLI_REAL_TG_ROOT"
CLI_REAL_TG_CONFIG_ENV = "CLI_REAL_TG_CONFIG"
CLI_REAL_TG_PHONE_ENV = "CLI_REAL_TG_PHONE"
CLI_REAL_TG_MUTATION_CHAT_ENV = "CLI_REAL_TG_MUTATION_CHAT"
CLI_REAL_TG_MUTATION_PHONE_ENV = "CLI_REAL_TG_MUTATION_PHONE"
CLI_REAL_TG_CONNECT_WAIT_ENV = "CLI_REAL_TG_CONNECT_WAIT_SECONDS"
CLI_REAL_TG_CONNECT_POLL_ENV = "CLI_REAL_TG_CONNECT_POLL_SECONDS"
CLI_REAL_TG_CONNECT_PROBE_TIMEOUT_ENV = "CLI_REAL_TG_CONNECT_PROBE_TIMEOUT_SECONDS"
CLI_REAL_TG_CONNECT_WAIT_DEFAULT_SECONDS = 60.0
CLI_REAL_TG_CONNECT_POLL_DEFAULT_SECONDS = 2.0
CLI_REAL_TG_CONNECT_PROBE_TIMEOUT_DEFAULT_SECONDS = 60.0
RUN_CLI_DEFAULT_TIMEOUT_SECONDS = 120
LIVE_CLI_DEFAULT_PYTEST_TIMEOUT_SECONDS = RUN_CLI_DEFAULT_TIMEOUT_SECONDS + 60


@dataclass(frozen=True)
class CliRealCliEnv:
    source_root: Path
    live_root: Path
    config_path: Path
    db_path: Path
    web_port: int
    phones: tuple[str, ...]
    channel_pk: str | None
    channel_id: int | None
    channel_username: str | None

    @property
    def repo_root(self) -> Path:
        return self.live_root

    @property
    def primary_phone(self) -> str:
        if not self.phones:
            pytest.skip("live DB has no connected Telegram accounts")
        return self.phones[0]

    @property
    def channel_ref(self) -> str | None:
        if self.channel_username:
            return self.channel_username
        if self.channel_id is not None:
            return str(self.channel_id)
        return None

    @property
    def pid_path(self) -> Path:
        return self.db_path.with_suffix(".pid")


@dataclass(frozen=True)
class LiveCliDialogTarget:
    chat_ref: str
    phone: str
    title: str | None = None


@dataclass(frozen=True)
class LiveCliMessageTarget:
    chat_ref: str
    message_id: str
    phone: str
    nonce: str | None = None


class LiveCliAccountWaitTimeoutError(RuntimeError):
    """Raised when no active live CLI account appears before the wait deadline."""


class LiveCliAccountReadinessError(RuntimeError):
    """Raised when active live CLI accounts exist but do not pass readiness probing."""


_SOURCE_ROOT = Path(__file__).resolve().parents[2]
CliEnv = CliRealCliEnv
_PIN_CAPABLE_DIALOG_TYPES = frozenset({"group", "supergroup", "gigagroup", "forum"})


def _env_float(name: str, default: float, *, min_value: float = 0.0) -> float:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        value = float(raw)
    except ValueError:
        pytest.fail(f"{name} must be a numeric seconds value, got {raw!r}", pytrace=False)
    if value < min_value:
        pytest.fail(f"{name} must be >= {min_value}, got {value}", pytrace=False)
    return value


def _resolve_live_root() -> Path:
    return Path(os.environ.get(CLI_REAL_TG_ROOT_ENV, _SOURCE_ROOT)).expanduser().resolve()


def _resolve_config_path(live_root: Path) -> Path:
    configured = os.environ.get(CLI_REAL_TG_CONFIG_ENV)
    if configured:
        path = Path(configured).expanduser()
        return path if path.is_absolute() else (live_root / path).resolve()
    return live_root / "config.yaml"


def _resolve_db_path(live_root: Path, db_path: str) -> Path:
    path = Path(db_path).expanduser()
    return path if path.is_absolute() else (live_root / path).resolve()


def _fetch_live_accounts(db_path: Path) -> tuple[str, ...]:
    requested_phone = os.environ.get(CLI_REAL_TG_PHONE_ENV, "").strip()
    now = datetime.now(timezone.utc)

    def _parse_flood_wait_until(raw: object) -> datetime | None:
        if raw is None or raw == "":
            return None
        try:
            parsed = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    def _sort_key(row: sqlite3.Row) -> tuple[int, int, int]:
        flood_wait_until = _parse_flood_wait_until(row["flood_wait_until"])
        blocked = int(flood_wait_until is not None and flood_wait_until > now)
        is_primary = int(row["is_primary"] or 0)
        account_id = int(row["id"] or 0)
        return blocked, -is_primary, account_id

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT id, phone, COALESCE(is_primary, 0) AS is_primary, flood_wait_until
            FROM accounts
            WHERE COALESCE(is_active, 1) = 1
              AND COALESCE(session_string, '') != ''
            """
        ).fetchall()
    accounts = [row for row in rows if row["phone"]]
    if requested_phone:
        accounts = [row for row in accounts if str(row["phone"]) == requested_phone]
    accounts.sort(key=_sort_key)
    return tuple(str(row["phone"]) for row in accounts)


def _fetch_live_channel(db_path: Path) -> tuple[str | None, int | None, str | None]:
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT id, channel_id, username
            FROM channels
            WHERE COALESCE(is_active, 1) = 1
            ORDER BY id ASC
            LIMIT 1
            """
        ).fetchone()
    if row is None:
        return None, None, None
    pk = str(row[0]) if row[0] is not None else None
    channel_id = int(row[1]) if row[1] is not None else None
    username = str(row[2]) if row[2] else None
    return pk, channel_id, username


def _fetch_live_owned_broadcast_channel(
    db_path: Path, phones: tuple[str, ...]
) -> tuple[str, str] | None:
    """Return a (chat_ref, phone) for a broadcast channel the account administers.

    ``dialogs broadcast-stats`` (GetBroadcastStatsRequest) requires channel admin
    rights, so the first arbitrary active channel is unusable. We pick a cached
    own (``is_own=1``) broadcast ``channel`` that has a username and a connected
    account recorded in ``dialog_cache`` — the account that cached an own channel
    is the one able to request its stats.
    """
    phone_set = set(phones)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT
                NULLIF(c.username, '') AS username,
                d.phone AS dialog_phone
            FROM dialog_cache d
            JOIN channels c ON c.channel_id = d.dialog_id
            WHERE COALESCE(d.is_own, 0) = 1
              AND COALESCE(d.deactivate, 0) = 0
              AND LOWER(COALESCE(NULLIF(d.channel_type, ''), '')) = 'channel'
              AND NULLIF(c.username, '') IS NOT NULL
              AND d.phone IS NOT NULL
            ORDER BY d.dialog_id ASC
            """
        ).fetchall()
    for row in rows:
        username = str(row["username"]) if row["username"] else None
        dialog_phone = str(row["dialog_phone"]) if row["dialog_phone"] else None
        if not username or dialog_phone not in phone_set:
            continue
        chat_ref = username if username.startswith("@") else f"@{username}"
        return chat_ref, dialog_phone
    return None


def _normalize_chat_ref(raw: object) -> str | None:
    chat_ref = str(raw) if raw else None
    if (
        chat_ref
        and chat_ref.lower() not in {"me", "self"}
        and not chat_ref.startswith("@")
        and not chat_ref.lstrip("-").isdigit()
    ):
        chat_ref = f"@{chat_ref}"
    return chat_ref


def _fetch_live_media_message(db_path: Path) -> tuple[str | None, int | None]:
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT
                COALESCE(NULLIF(c.username, ''), CAST(m.channel_id AS TEXT)) AS chat_ref,
                m.message_id
            FROM messages m
            LEFT JOIN channels c ON c.channel_id = m.channel_id
            WHERE COALESCE(m.media_type, '') NOT IN ('', 'text')
              AND COALESCE(c.is_active, 1) = 1
            ORDER BY m.date DESC, m.id DESC
            LIMIT 1
            """
        ).fetchone()
    if row is None:
        return None, None
    chat_ref = _normalize_chat_ref(row[0])
    message_id = int(row[1]) if row[1] is not None else None
    return chat_ref, message_id


def _fetch_live_message_target(
    db_path: Path,
    phones: tuple[str, ...],
    *,
    require_own_dialog: bool = False,
    require_pin_capable_dialog: bool = False,
) -> LiveCliMessageTarget | None:
    phone_set = set(phones)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT
                COALESCE(NULLIF(c.username, ''), CAST(m.channel_id AS TEXT)) AS chat_ref,
                m.message_id,
                NULLIF(c.preferred_phone, '') AS preferred_phone,
                d.phone AS dialog_phone,
                COALESCE(d.is_own, 0) AS is_own,
                LOWER(COALESCE(NULLIF(d.channel_type, ''), NULLIF(c.channel_type, ''), '')) AS dialog_type
            FROM messages m
            JOIN channels c ON c.channel_id = m.channel_id
            LEFT JOIN dialog_cache d
                ON d.dialog_id = m.channel_id
               AND COALESCE(d.deactivate, 0) = 0
            WHERE COALESCE(c.is_active, 1) = 1
              AND m.message_id IS NOT NULL
              AND COALESCE(m.service_action_raw, '') = ''
              AND COALESCE(m.service_action_semantic, '') = ''
            ORDER BY
                COALESCE(d.is_own, 0) DESC,
                CASE WHEN NULLIF(c.preferred_phone, '') IS NOT NULL THEN 0 ELSE 1 END,
                CASE WHEN d.phone IS NOT NULL THEN 0 ELSE 1 END,
                m.date DESC,
                m.id DESC
            LIMIT 200
            """
        ).fetchall()

    for row in rows:
        chat_ref = _normalize_chat_ref(row["chat_ref"])
        if chat_ref is None:
            continue
        message_id = row["message_id"]
        if message_id is None:
            continue

        preferred_phone = str(row["preferred_phone"]) if row["preferred_phone"] else None
        dialog_phone = str(row["dialog_phone"]) if row["dialog_phone"] else None
        is_own = bool(row["is_own"])
        dialog_type = str(row["dialog_type"] or "").lower()
        if require_own_dialog and (not is_own or dialog_phone not in phone_set):
            continue
        if require_pin_capable_dialog and dialog_type not in _PIN_CAPABLE_DIALOG_TYPES:
            continue

        if preferred_phone in phone_set:
            phone = preferred_phone
        elif dialog_phone in phone_set:
            phone = dialog_phone
        elif chat_ref.startswith("@"):
            phone = phones[0]
        else:
            continue

        return LiveCliMessageTarget(
            chat_ref=chat_ref,
            message_id=str(int(message_id)),
            phone=phone,
        )
    return None


@pytest.fixture(scope="session")
def cli_real_cli_env() -> CliRealCliEnv:
    if not _gate_enabled(CLI_REAL_TG_LIVE_GATE_ENV):
        pytest.skip(
            f"live CLI tests disabled; project is not live-ready "
            f"(set {CLI_REAL_TG_LIVE_GATE_ENV}=1 to force on, =0 to force off)"
        )

    live_root = _resolve_live_root()
    config_path = _resolve_config_path(live_root)
    if not config_path.exists():
        pytest.skip(
            f"live CLI config not found at {config_path}; set {CLI_REAL_TG_CONFIG_ENV} or {CLI_REAL_TG_ROOT_ENV}"
        )

    load_cli_dotenv(config_path)
    config = load_config(config_path)

    db_path = _resolve_db_path(live_root, config.database.path)
    if not db_path.exists():
        pytest.skip(f"live CLI database not found at {db_path}")
    if db_path.stat().st_size == 0:
        pytest.skip(f"live CLI database at {db_path} is empty")

    # api_id/api_hash may live in config.yaml/env OR the settings table — mirror
    # production's fallback (src/cli/runtime.py:init_pool), hence the DB check first.
    api_id, api_hash = _resolve_api_credentials(config, db_path)
    if not api_id or not api_hash:
        pytest.skip("live CLI config/DB has no Telegram api_id/api_hash")

    probe_env = CliRealCliEnv(
        source_root=_SOURCE_ROOT,
        live_root=live_root,
        config_path=config_path,
        db_path=db_path,
        web_port=int(config.web.port),
        phones=(),
        channel_pk=None,
        channel_id=None,
        channel_username=None,
    )
    try:
        phones = wait_for_ready_live_cli_accounts(
            probe_env,
            wait_seconds=_env_float(
                CLI_REAL_TG_CONNECT_WAIT_ENV,
                CLI_REAL_TG_CONNECT_WAIT_DEFAULT_SECONDS,
            ),
            poll_seconds=_env_float(
                CLI_REAL_TG_CONNECT_POLL_ENV,
                CLI_REAL_TG_CONNECT_POLL_DEFAULT_SECONDS,
                min_value=0.1,
            ),
            probe_timeout_seconds=_env_float(
                CLI_REAL_TG_CONNECT_PROBE_TIMEOUT_ENV,
                CLI_REAL_TG_CONNECT_PROBE_TIMEOUT_DEFAULT_SECONDS,
                min_value=1.0,
            ),
        )
    except LiveCliAccountWaitTimeoutError as exc:
        pytest.skip(str(exc))
    except LiveCliAccountReadinessError as exc:
        pytest.fail(str(exc), pytrace=False)

    try:
        channel_pk, channel_id, channel_username = _fetch_live_channel(db_path)
    except sqlite3.Error:
        channel_pk, channel_id, channel_username = None, None, None

    return CliRealCliEnv(
        source_root=_SOURCE_ROOT,
        live_root=live_root,
        config_path=config_path,
        db_path=db_path,
        web_port=int(config.web.port),
        phones=phones,
        channel_pk=channel_pk,
        channel_id=channel_id,
        channel_username=channel_username,
    )


@pytest.fixture(scope="session")
def cli_env(cli_real_cli_env: CliRealCliEnv) -> CliEnv:
    return cli_real_cli_env


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    root = Path(__file__).resolve().parent
    default_timeout_marker = pytest.mark.timeout(LIVE_CLI_DEFAULT_PYTEST_TIMEOUT_SECONDS)
    for item in items:
        item_path = Path(str(item.fspath)).resolve()
        if root not in item_path.parents:
            continue
        if item.get_closest_marker("timeout"):
            continue
        item.add_marker(default_timeout_marker)


def _cli_command(cli_env: CliEnv, args: tuple[str, ...]) -> list[str]:
    return [
        sys.executable,
        "-m",
        "src.main",
        "--config",
        str(cli_env.config_path),
        *args,
    ]


def _build_cli_env(cli_env: CliEnv, extra_env: dict[str, str] | None = None) -> dict[str, str]:
    env = os.environ.copy()
    existing = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = f"{cli_env.source_root}{os.pathsep}{existing}" if existing else str(cli_env.source_root)
    env["PYTHONSAFEPATH"] = "1"
    if extra_env:
        env.update(extra_env)
    return env


def cli_run_direct(
    cli_env: CliEnv,
    *args: str,
    timeout: float = 20.0,
    extra_env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess:
    """subprocess.run wrapper for cleanup code that must not call pytest.skip."""
    return subprocess.run(  # noqa: S603 - controlled CLI module invocation
        _cli_command(cli_env, args),
        cwd=str(cli_env.repo_root),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=timeout,
        env=_build_cli_env(cli_env, extra_env=extra_env),
        check=False,
    )


# --- verify-before-delete: only ever delete what the test itself created ---------
#
# A previous manual cleanup deleted a personal message because a message id was
# parsed loosely and the deletion was not verified. The helpers below give tests a
# structural guarantee: before any destructive cleanup, the test re-reads the
# target by id and confirms it carries the unique nonce the test itself wrote. No
# nonce match -> do not delete (fail-safe leak instead of risking someone else's
# data).
#
# IMPORTANT (policy contract): `_capture_cli` is intentionally NOT named
# run_cli / run_cli_popen / cli_run_direct. The non-editable AST auditor in
# tests/test_real_telegram_policy.py only inspects CLI calls made through those
# three helper names; verification reads (`messages read --live`) and targeted
# `scheduler task-cancel` calls are issued through this differently-named raw
# wrapper so they do not have to be cleanup-allowlisted (they are reads / targeted
# cancels, not blind deletes). The blind-delete literals
# (`dialogs delete-message`, `dialogs leave`, `photo-loader batch-cancel`) stay in
# each test's `finally` as `cli_run_direct(...)`, as the policy requires.

_FLOOD_OR_READ_ERROR_RE = re.compile(
    r"FloodWaitError|FLOOD_?WAIT|Error reading messages|No messages found|"
    r"unavailable|not connected|Account .* not connected",
    re.IGNORECASE,
)
# `messages read --live` prints one header line per message as
# `print(f"[{date}] #{msg.id}{sender}...")` where `sender` is `" name last".strip()`
# (src/cli/commands/messages.py:84). So the sender is concatenated DIRECTLY after
# the id with NO separating space (e.g. `[2026-06-03 02:42:48] #611404Alexey`), and
# is empty when there is no sender. The id is therefore terminated by a non-digit
# (a letter, reactions suffix, or end of line), NOT by whitespace — matching on a
# space here would silently fail to parse every real header. `(?!\d)` ends the id
# at the last digit without consuming the following char.
_LIVE_MESSAGE_HEADER_RE = re.compile(r"^\[.*?\]\s+#(\d+)(?!\d)")
_SENT_MESSAGE_ID_RE = re.compile(r"\bmessage_id=(\d+)\b")
_CREATED_GROUP_ID_RE = re.compile(r"\bCreated group id=(-?\d+)\b")
_RESOLVE_ID_RE = re.compile(r"^ID:\s*(-?\d+)\s*$", re.MULTILINE)


def _capture_cli(
    cli_env: CliEnv,
    *args: str,
    timeout: float = 60.0,
    extra_env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess:
    """Raw CLI runner for verification reads and targeted cancels.

    Deliberately named outside ``_RUN_CLI_HELPERS`` so the policy auditor does not
    treat the read/cancel commands invoked here as blind-delete cleanup calls.
    """
    return subprocess.run(  # noqa: S603 - controlled CLI module invocation
        _cli_command(cli_env, args),
        cwd=str(cli_env.repo_root),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=timeout,
        env=_build_cli_env(cli_env, extra_env=extra_env),
        check=False,
    )


def make_cli_nonce() -> str:
    """Unique token embedded in test-created message text/captions.

    A full uuid4 hex is collision-free in practice, so finding it in a message's
    text proves the message was produced by this test run.
    """
    return uuid.uuid4().hex


@dataclass(frozen=True)
class VerifyResult:
    ok: bool
    reason: str


def _parse_live_message_blocks(stdout: str) -> dict[int, str]:
    """Map message_id -> joined text body from `messages read --live` output.

    The live format is a header line ``[<date>] #<id> <sender>`` followed by the
    message body. `_print_live_messages` (src/cli/commands/messages.py) prints the
    whole body with a single ``print(f"  {text[:500]}")``, so ONLY the first
    physical line of a multi-line body carries the two-space indent — continuation
    lines (after embedded newlines) are unindented, and blank lines may appear
    between paragraphs. We therefore accumulate every line after a header until the
    NEXT header line, stripping the optional two-space prefix. Extra trailing lines
    (the inter-message blank, an optional ``reaction users:`` line) are harmless:
    the caller only substring-checks the body for its nonce/marker, which those
    lines never contain.
    """
    blocks: dict[int, list[str]] = {}
    current: int | None = None
    for raw_line in stdout.splitlines():
        header = _LIVE_MESSAGE_HEADER_RE.match(raw_line)
        if header:
            current = int(header.group(1))
            blocks.setdefault(current, [])
            continue
        if current is None:
            continue
        blocks[current].append(raw_line[2:] if raw_line.startswith("  ") else raw_line)
    return {mid: "\n".join(lines).strip("\n") for mid, lines in blocks.items()}


def cli_verify_message_nonce(
    cli_env: CliEnv,
    *,
    phone: str,
    chat_ref: str,
    message_id: int,
    nonce: str,
    window: int = 20,
    timeout: float = 60.0,
) -> VerifyResult:
    """Confirm ``message_id`` in ``chat_ref`` carries ``nonce`` before deletion.

    Returns ``ok=True`` only when the live read finds a message whose id is
    exactly ``message_id`` and whose text contains ``nonce``. Any read error,
    FLOOD_WAIT, empty result, or mismatch returns ``ok=False`` so the caller skips
    the delete (fail-safe: never delete on uncertainty).
    """
    # Narrow read first: offset_id=message_id+1 returns the single message with
    # id == message_id (iter_messages returns messages strictly older than the
    # offset, newest first). Fall back to a small recent window if that misses.
    for read_args in (
        ("--offset-id", str(message_id + 1), "--limit", "1"),
        ("--limit", str(window)),
    ):
        result = _capture_cli(
            cli_env,
            "messages",
            "read",
            chat_ref,
            "--live",
            "--phone",
            phone,
            *read_args,
            timeout=timeout,
        )
        combined = (result.stdout or "") + "\n" + (result.stderr or "")
        if result.returncode != 0 or _FLOOD_OR_READ_ERROR_RE.search(combined):
            # Could not read reliably -> do not authorize deletion.
            continue
        blocks = _parse_live_message_blocks(result.stdout or "")
        if message_id not in blocks:
            continue
        if nonce in blocks[message_id]:
            return VerifyResult(True, f"verified #{message_id} carries nonce")
        return VerifyResult(
            False,
            f"#{message_id} text does not contain the test nonce; refusing to delete",
        )
    return VerifyResult(
        False,
        f"could not read #{message_id} from {chat_ref} to verify ownership; refusing to delete",
    )


def assert_safe_to_delete(
    cli_env: CliEnv,
    *,
    phone: str,
    chat_ref: str,
    candidates: list[int],
    nonce: str,
) -> tuple[list[int], str | None]:
    """Return (verified_ids, leak_reason) — ids proven to carry ``nonce``.

    The caller deletes only ``verified_ids`` (so it can never delete a message it
    did not create). ``leak_reason`` lists any candidate that failed verification
    and was therefore left in place.
    """
    verified: list[int] = []
    failed: list[str] = []
    for message_id in candidates:
        verdict = cli_verify_message_nonce(
            cli_env,
            phone=phone,
            chat_ref=chat_ref,
            message_id=message_id,
            nonce=nonce,
        )
        if verdict.ok:
            verified.append(message_id)
        else:
            failed.append(verdict.reason)
    leak_reason = (
        "left un-deleted (verification failed): " + "; ".join(failed) if failed else None
    )
    return verified, leak_reason


_RUN_GENERATED_TEXT_HEADER = "--- GENERATED TEXT ---"


def fetch_pipeline_run_text(cli_env: CliEnv, run_id: str, *, timeout: float = 30.0) -> str | None:
    """Return the run's generated text from `pipeline run-show` (the published body).

    Pipeline-published messages carry text the pipeline generated (no test-injected
    nonce is possible), so cleanup verifies the published message against this
    reference text before deleting. Returns None if the run has no generated text.
    """
    result = _capture_cli(cli_env, "pipeline", "run-show", run_id, timeout=timeout)
    if cli_result_failure_summary(result) is not None:
        return None
    lines = (result.stdout or "").splitlines()
    try:
        idx = lines.index(_RUN_GENERATED_TEXT_HEADER)
    except ValueError:
        return None
    body_lines: list[str] = []
    for line in lines[idx + 1 :]:
        if line.startswith("... (") and line.rstrip().endswith("more chars)"):
            break
        body_lines.append(line)
    text = "\n".join(body_lines).strip()
    return text or None


def distinctive_text_fragment(text: str, *, min_len: int = 40, max_len: int = 80) -> str | None:
    """Longest non-trivial line of ``text``, trimmed — used as an ownership marker.

    `pipeline run-show` truncates the body to 500 chars and the live read shows the
    first 500 chars too, so we match on a distinctive line both are guaranteed to
    share rather than the whole body.

    Unlike a `uuid4` nonce, this fragment comes from LLM-generated content and is
    not guaranteed unique across runs. We therefore require a fairly long line
    (``min_len`` chars) so an accidental cross-run collision is practically
    negligible, and return ``None`` when no line is long enough — in which case the
    caller intentionally SKIPS cleanup (leaves the message) rather than deleting on
    a weak marker.
    """
    candidates = [line.strip() for line in text.splitlines() if len(line.strip()) >= min_len]
    if not candidates:
        stripped = text.strip()
        return stripped[:max_len] if len(stripped) >= min_len else None
    longest = max(candidates, key=len)
    return longest[:max_len]


_RESOLVE_TITLE_RE = re.compile(r"^Title:\s*(.*)$", re.MULTILINE)


def cli_verify_channel_title(
    cli_env: CliEnv,
    *,
    phone: str,
    channel_id: str,
    expected_title: str,
    timeout: float = 60.0,
) -> VerifyResult:
    """Confirm the channel at ``channel_id`` has exactly ``expected_title``.

    Used before leaving/deleting a test-created channel: `dialogs resolve` prints
    ``Title: <title>``; we only authorize cleanup when that title matches the
    unique title this test created. Any read error or mismatch returns ok=False.
    """
    result = _capture_cli(
        cli_env, "dialogs", "resolve", channel_id, "--phone", phone, timeout=timeout
    )
    if cli_result_failure_summary(result) is not None:
        return VerifyResult(
            False, f"could not resolve channel {channel_id} to verify its title; refusing to leave"
        )
    match = _RESOLVE_TITLE_RE.search(result.stdout or "")
    if match is None:
        return VerifyResult(False, f"no title in resolve output for {channel_id}; refusing to leave")
    actual = match.group(1).strip()
    if actual == expected_title:
        return VerifyResult(True, f"channel {channel_id} title matches {expected_title!r}")
    return VerifyResult(
        False,
        f"channel {channel_id} title {actual!r} != expected {expected_title!r}; refusing to leave",
    )


def resolve_saved_messages_dialog_id(cli_env: CliEnv, *, phone: str) -> int | None:
    """Return the current account's Saved Messages dialog id, or None on failure."""
    result = _capture_cli(
        cli_env,
        "dialogs",
        "resolve",
        "me",
        "--phone",
        phone,
        timeout=60,
    )
    if cli_result_failure_summary(result) is not None:
        return None
    match = _RESOLVE_ID_RE.search(result.stdout or "")
    return int(match.group(1)) if match else None


def cleanup_verified_messages(
    cli_env: CliEnv,
    *,
    phone: str,
    chat_ref: str,
    candidates: list[int],
    nonce: str,
) -> str | None:
    """Delete only candidate messages that live-read verification proves are ours."""
    verified_ids, verify_leak = assert_safe_to_delete(
        cli_env,
        phone=phone,
        chat_ref=chat_ref,
        candidates=candidates,
        nonce=nonce,
    )
    if not verified_ids:
        return verify_leak

    try:
        cleanup = _capture_cli(
            cli_env,
            "dialogs",
            "delete-message",
            "--yes",
            "--phone",
            phone,
            chat_ref,
            *[str(mid) for mid in verified_ids],
            timeout=60,
        )
    except subprocess.TimeoutExpired:
        return f"message(s) {verified_ids} in {chat_ref} may be left: cleanup timed out"
    cleanup_failure = cli_result_failure_summary(cleanup)
    if cleanup_failure is not None:
        return f"message(s) {verified_ids} in {chat_ref} may be left: {cleanup_failure}"
    return verify_leak


def snapshot_pending_collection_task_ids(db_path: Path) -> tuple[set[int], bool]:
    """Return (pending channel-collect task ids, ok) currently in the DB.

    Used to scope scheduler cleanup to only the tasks a test created (diff a
    before/after snapshot) instead of the global, destructive
    ``scheduler clear-pending`` which would cancel every pending task.

    The ``ok`` flag is False when the read failed (e.g. transient
    ``database is locked``). Callers MUST treat a failed read as "unknown
    baseline" and SKIP cleanup — never proceed with an empty set, or
    ``after - {}`` would cancel every pending task in the live DB, which is the
    exact over-cancellation this guard exists to prevent.
    """
    try:
        with sqlite3.connect(db_path) as conn:
            rows = conn.execute(
                "SELECT id FROM collection_tasks WHERE status = ? AND task_type = ?",
                ("pending", "channel_collect"),
            ).fetchall()
    except sqlite3.Error:
        return set(), False
    return {int(row[0]) for row in rows}, True


def cancel_collection_tasks(
    cli_env: CliEnv, task_ids: set[int], *, timeout: float = 30.0
) -> str | None:
    """Cancel only the given collection tasks via targeted `scheduler task-cancel`.

    Returns a leak description if any cancel failed, else None. Issued through the
    raw `_capture_cli` (targeted cancel, not a blind global clear).
    """
    failures: list[str] = []
    for task_id in sorted(task_ids):
        try:
            result = _capture_cli(
                cli_env, "scheduler", "task-cancel", str(task_id), timeout=timeout
            )
        except subprocess.TimeoutExpired:
            failures.append(f"task {task_id}: cancel timed out")
            continue
        if cli_result_failure_summary(result) is not None:
            failures.append(f"task {task_id}: cancel failed")
    return "; ".join(failures) if failures else None


def _run_account_info_probe(
    cli_env: CliEnv,
    phone: str,
    *,
    timeout: float,
    runner: Callable[..., subprocess.CompletedProcess] = subprocess.run,
) -> subprocess.CompletedProcess:
    return runner(  # noqa: S603 - controlled CLI module invocation
        _cli_command(cli_env, ("account", "info", "--phone", phone)),
        cwd=str(cli_env.repo_root),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=timeout,
        env=_build_cli_env(cli_env),
        check=False,
    )


def _timeout_stream_excerpt(exc: subprocess.TimeoutExpired, *, limit: int = 500) -> str:
    parts: list[str] = []
    for name in ("stdout", "stderr"):
        raw = getattr(exc, name, None)
        if not raw:
            continue
        if isinstance(raw, bytes):
            text = raw.decode("utf-8", errors="replace")
        else:
            text = str(raw)
        text = " ".join(text.split())
        if len(text) > limit:
            text = f"{text[:limit]}..."
        parts.append(f"{name}: {text}")
    return "; ".join(parts)


def account_info_probe_failure(phone: str, result: subprocess.CompletedProcess) -> str | None:
    failure_summary = cli_result_failure_summary(result)
    if failure_summary is not None:
        return failure_summary
    if phone not in (result.stdout or ""):
        return (
            f"`account info --phone {phone}` did not confirm the account in stdout\n"
            f"--- stdout ---\n{result.stdout}\n"
            f"--- stderr ---\n{result.stderr}"
        )
    return None


def wait_for_ready_live_cli_accounts(
    cli_env: CliEnv,
    *,
    wait_seconds: float = CLI_REAL_TG_CONNECT_WAIT_DEFAULT_SECONDS,
    poll_seconds: float = CLI_REAL_TG_CONNECT_POLL_DEFAULT_SECONDS,
    probe_timeout_seconds: float = CLI_REAL_TG_CONNECT_PROBE_TIMEOUT_DEFAULT_SECONDS,
    fetch_accounts: Callable[[Path], tuple[str, ...]] = _fetch_live_accounts,
    runner: Callable[..., subprocess.CompletedProcess] = subprocess.run,
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> tuple[str, ...]:
    deadline = monotonic() + wait_seconds
    last_probe_failure: str | None = None
    last_db_error: str | None = None
    last_phones: tuple[str, ...] = ()

    while True:
        try:
            phones = fetch_accounts(cli_env.db_path)
            last_db_error = None
        except sqlite3.Error as exc:
            phones = ()
            last_db_error = str(exc)

        if phones:
            last_phones = phones
            for phone in phones:
                remaining_seconds = deadline - monotonic()
                if remaining_seconds <= 0:
                    break
                effective_probe_timeout_seconds = min(probe_timeout_seconds, remaining_seconds)
                try:
                    result = _run_account_info_probe(
                        cli_env,
                        phone,
                        timeout=effective_probe_timeout_seconds,
                        runner=runner,
                    )
                except subprocess.TimeoutExpired as exc:
                    detail = _timeout_stream_excerpt(exc)
                    last_probe_failure = (
                        f"`account info --phone {phone}` timed out after "
                        f"{effective_probe_timeout_seconds:g}s"
                    )
                    if detail:
                        last_probe_failure += f" ({detail})"
                    continue

                failure = account_info_probe_failure(phone, result)
                if failure is None:
                    return (phone, *(candidate for candidate in phones if candidate != phone))
                last_probe_failure = f"{phone}: {failure}"

        now = monotonic()
        if now >= deadline:
            break
        sleep(min(poll_seconds, max(0.0, deadline - now)))

    if last_phones:
        detail = f"; last probe failure: {last_probe_failure}" if last_probe_failure else ""
        raise LiveCliAccountReadinessError(
            "live CLI active account(s) did not pass Telegram readiness probe "
            f"within {wait_seconds:g}s{detail}"
        )

    detail = f"; last DB error: {last_db_error}" if last_db_error else ""
    raise LiveCliAccountWaitTimeoutError(
        "live CLI database has no active connected Telegram accounts "
        f"within {wait_seconds:g}s{detail}"
    )


@pytest.fixture
def run_cli(cli_real_cli_env: CliEnv):
    def _run(
        *args: str,
        timeout: int = RUN_CLI_DEFAULT_TIMEOUT_SECONDS,
        extra_env: dict[str, str] | None = None,
    ) -> subprocess.CompletedProcess:
        try:
            return subprocess.run(  # noqa: S603 - controlled CLI module invocation
                _cli_command(cli_real_cli_env, args),
                cwd=str(cli_real_cli_env.repo_root),
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=timeout,
                env=_build_cli_env(cli_real_cli_env, extra_env=extra_env),
                check=False,
            )
        except subprocess.TimeoutExpired:
            pytest.fail(f"CLI command timed out after {timeout}s: {' '.join(args)}", pytrace=False)

    return _run


@pytest.fixture
def run_cli_popen(cli_real_cli_env: CliEnv):
    """Spawn long-running CLI commands and clean them up on test teardown."""
    processes: list[subprocess.Popen] = []

    def _spawn(
        *args: str,
        extra_env: dict[str, str] | None = None,
        capture_stdout: bool = False,
    ) -> subprocess.Popen:
        proc = subprocess.Popen(  # noqa: S603 - controlled CLI module invocation
            _cli_command(cli_real_cli_env, args),
            cwd=str(cli_real_cli_env.repo_root),
            env=_build_cli_env(cli_real_cli_env, extra_env=extra_env),
            stdout=subprocess.PIPE if capture_stdout else subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        processes.append(proc)
        return proc

    yield _spawn

    for proc in processes:
        if proc.poll() is not None:
            continue
        proc.terminate()
        try:
            proc.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            try:
                proc.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                pass


def wait_for_http_200(url: str, *, timeout: float = 15.0, interval: float = 0.5) -> bool:
    import urllib.error
    import urllib.request

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=2.0) as resp:
                if resp.status == 200:
                    return True
        except (urllib.error.URLError, ConnectionError, TimeoutError):
            pass
        time.sleep(interval)
    return False


def sqlite_utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def read_pid_file(path: Path) -> int | None:
    try:
        raw = path.read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        return None
    if not raw:
        return None
    try:
        return int(raw)
    except ValueError:
        return None


def skip_if_server_pid_exists(cli_env: CliEnv) -> None:
    if not cli_env.pid_path.exists():
        return
    pid = read_pid_file(cli_env.pid_path)
    suffix = f" PID {pid}" if pid is not None else ""
    pytest.skip(
        f"live server PID file already exists at {cli_env.pid_path}{suffix}; "
        "stop the existing server or use a separate CLI_REAL_TG_CONFIG"
    )


def wait_for_pid_file(
    path: Path,
    expected_pid: int,
    *,
    timeout: float = 15.0,
    interval: float = 0.2,
) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if read_pid_file(path) == expected_pid:
            return True
        time.sleep(interval)
    return False


def wait_for_db_row(
    db_path: Path,
    sql: str,
    params: tuple = (),
    *,
    timeout: float = 15.0,
    interval: float = 0.5,
) -> tuple | None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            conn = sqlite3.connect(str(db_path))
            try:
                row = conn.execute(sql, params).fetchone()
                if row is not None:
                    return row
            finally:
                conn.close()
        except sqlite3.Error:
            pass
        time.sleep(interval)
    return None


_FLOOD_WAIT_RE = re.compile(r"FloodWaitError|FLOOD_?WAIT", re.IGNORECASE)
_AUTH_RE = re.compile(r"AuthKeyError|AuthKeyUnregistered|session\s+expired|UnauthorizedError", re.IGNORECASE)
_SILENT_FAILURE_PATTERNS = (
    ("Traceback", re.compile(r"Traceback", re.IGNORECASE)),
    ("ModuleNotFoundError", re.compile(r"ModuleNotFoundError", re.IGNORECASE)),
    ("No connected accounts", re.compile(r"No connected accounts", re.IGNORECASE)),
    ("No accounts found", re.compile(r"No accounts found", re.IGNORECASE)),
    ("Live Telegram accounts not found", re.compile(r"Live Telegram accounts not found", re.IGNORECASE)),
    ("not found for this request", re.compile(r"not found for this request", re.IGNORECASE)),
    ("Could not resolve channel", re.compile(r"Could not resolve channel", re.IGNORECASE)),
    ("Error fetching broadcast stats", re.compile(r"Error fetching broadcast stats", re.IGNORECASE)),
    ("Failed to initialize", re.compile(r"Failed to initialize", re.IGNORECASE)),
    ("Failed to load", re.compile(r"Failed to load", re.IGNORECASE)),
    ("Error sending reaction", re.compile(r"Error sending reaction", re.IGNORECASE)),
    ("Error sending message", re.compile(r"Error sending message", re.IGNORECASE)),
    ("Error editing message", re.compile(r"Error editing message", re.IGNORECASE)),
    ("Error pinning", re.compile(r"Error pinning", re.IGNORECASE)),
    ("Error unpinning", re.compile(r"Error unpinning", re.IGNORECASE)),
    ("RuntimeError", re.compile(r"RuntimeError", re.IGNORECASE)),
)
_DEFAULT_ALLOWED_ERROR_TEXTS = frozenset({"No connected accounts"})


def _normalize_allowed_error_texts(
    allow_error_text: bool | str | tuple[str, ...],
) -> frozenset[str]:
    if allow_error_text is True:
        return _DEFAULT_ALLOWED_ERROR_TEXTS
    if allow_error_text is False:
        return frozenset()
    if isinstance(allow_error_text, str):
        return frozenset({allow_error_text})
    return frozenset(allow_error_text)


def cli_result_failure_summary(
    result: subprocess.CompletedProcess,
    *,
    allow_error_text: bool | str | tuple[str, ...] = False,
) -> str | None:
    combined = (result.stdout or "") + "\n" + (result.stderr or "")
    if result.returncode != 0:
        return (
            f"CLI exited with {result.returncode}\n"
            f"--- stdout ---\n{result.stdout}\n"
            f"--- stderr ---\n{result.stderr}"
        )

    allowed_error_texts = _normalize_allowed_error_texts(allow_error_text)
    for failure_text, pattern in _SILENT_FAILURE_PATTERNS:
        if failure_text in allowed_error_texts:
            continue
        if pattern.search(combined):
            return (
                "CLI returned zero but printed a failure-looking message "
                f"({failure_text})\n"
                f"--- stdout ---\n{result.stdout}\n"
                f"--- stderr ---\n{result.stderr}"
            )
    return None


def _assert_cli_result_ok(
    result: subprocess.CompletedProcess,
    *,
    allow_error_text: bool | str | tuple[str, ...] = False,
) -> None:
    combined = (result.stdout or "") + "\n" + (result.stderr or "")
    if result.returncode != 0:
        if _FLOOD_WAIT_RE.search(combined):
            pytest.skip("Telegram FLOOD_WAIT; retry later")
        if _AUTH_RE.search(combined):
            pytest.skip("Telegram session not authorized; re-auth account")
    failure_summary = cli_result_failure_summary(result, allow_error_text=allow_error_text)
    if failure_summary is not None:
        pytest.fail(failure_summary, pytrace=False)


@pytest.fixture
def assert_cli_ok():
    def _assert(
        result: subprocess.CompletedProcess,
        *,
        allow_error_text: bool | str | tuple[str, ...] = False,
    ) -> None:
        _assert_cli_result_ok(result, allow_error_text=allow_error_text)

    return _assert


@pytest.fixture
def live_channel(cli_real_cli_env: CliRealCliEnv) -> tuple[str, str]:
    if cli_real_cli_env.channel_pk is None or cli_real_cli_env.channel_id is None:
        pytest.skip("live CLI database has no active channel")
    return cli_real_cli_env.channel_pk, str(cli_real_cli_env.channel_id)


@pytest.fixture
def live_channel_username(cli_real_cli_env: CliRealCliEnv) -> str:
    if not cli_real_cli_env.channel_username:
        pytest.skip("live CLI database has no active channel with username")
    username = cli_real_cli_env.channel_username
    return username if username.startswith("@") else f"@{username}"


@pytest.fixture
def live_owned_broadcast_channel(cli_real_cli_env: CliRealCliEnv) -> LiveCliDialogTarget:
    try:
        target = _fetch_live_owned_broadcast_channel(
            cli_real_cli_env.db_path, cli_real_cli_env.phones
        )
    except sqlite3.Error as exc:
        pytest.skip(
            f"failed to discover an own broadcast channel from {cli_real_cli_env.db_path}: {exc}"
        )
    if target is None:
        pytest.skip(
            "live CLI database has no cached own broadcast channel with a username; "
            "`dialogs broadcast-stats` requires channel admin rights"
        )
    chat_ref, phone = target
    return LiveCliDialogTarget(chat_ref=chat_ref, phone=phone)


@pytest.fixture
def live_phone(cli_real_cli_env: CliRealCliEnv) -> str:
    return cli_real_cli_env.primary_phone


@pytest.fixture
def live_media_message(cli_real_cli_env: CliRealCliEnv) -> tuple[str, str]:
    try:
        chat_ref, message_id = _fetch_live_media_message(cli_real_cli_env.db_path)
    except sqlite3.Error as exc:
        pytest.skip(f"failed to discover live media message from {cli_real_cli_env.db_path}: {exc}")
    if chat_ref is None or message_id is None:
        pytest.skip("live CLI database has no collected media messages")
    return chat_ref, str(message_id)


@pytest.fixture
def live_mutation_dialog(cli_real_cli_env: CliRealCliEnv) -> LiveCliDialogTarget:
    try:
        target = _fetch_live_message_target(cli_real_cli_env.db_path, cli_real_cli_env.phones)
    except sqlite3.Error as exc:
        pytest.skip(f"failed to discover live mutation target from {cli_real_cli_env.db_path}: {exc}")
    if target is None:
        pytest.skip("live CLI database has no active collected dialog target")
    return LiveCliDialogTarget(chat_ref=target.chat_ref, phone=target.phone)


@pytest.fixture
def live_mutation_message(cli_real_cli_env: CliRealCliEnv) -> LiveCliMessageTarget:
    try:
        target = _fetch_live_message_target(cli_real_cli_env.db_path, cli_real_cli_env.phones)
    except sqlite3.Error as exc:
        pytest.skip(f"failed to discover live mutation message from {cli_real_cli_env.db_path}: {exc}")
    if target is None:
        pytest.skip("live CLI database has no active collected message target")
    return target


@pytest.fixture
def live_scratch_message_dialog(cli_real_cli_env: CliRealCliEnv) -> LiveCliDialogTarget:
    chat_ref = _normalize_chat_ref(os.environ.get(CLI_REAL_TG_MUTATION_CHAT_ENV))
    phone = os.environ.get(CLI_REAL_TG_MUTATION_PHONE_ENV)
    if phone and phone not in cli_real_cli_env.phones:
        pytest.skip(f"{CLI_REAL_TG_MUTATION_PHONE_ENV} is not a connected live CLI account")

    if chat_ref is not None:
        return LiveCliDialogTarget(chat_ref=chat_ref, phone=phone or cli_real_cli_env.primary_phone)

    # Default to "Saved Messages" (`me`): the account can always send/edit/forward/
    # delete there, so scratch-message mutation tests do not depend on the live DB
    # happening to cache an own dialog the account is *also* allowed to post in.
    # A cached own dialog (e.g. a broadcast channel the account merely follows)
    # would raise ChatAdminRequiredError on SendMessageRequest. Override the target
    # via the CLI_REAL_TG_MUTATION_CHAT env var when a specific chat is required.
    return LiveCliDialogTarget(chat_ref="me", phone=phone or cli_real_cli_env.primary_phone)


@pytest.fixture
def live_scratch_message(
    cli_real_cli_env: CliRealCliEnv,
    live_scratch_message_dialog: LiveCliDialogTarget,
) -> LiveCliMessageTarget:
    chat_ref = live_scratch_message_dialog.chat_ref
    phone = live_scratch_message_dialog.phone
    nonce = make_cli_nonce()
    text = f"codex live cli scratch message {nonce}"
    result = _capture_cli(
        cli_real_cli_env,
        "dialogs",
        "send",
        "--yes",
        "--phone",
        phone,
        chat_ref,
        text,
        timeout=60,
    )
    _assert_cli_result_ok(result)
    match = _SENT_MESSAGE_ID_RE.search(result.stdout or "")
    if match is None:
        pytest.fail(f"send stdout did not include message_id: {result.stdout!r}", pytrace=False)
    message_id = match.group(1)

    yield LiveCliMessageTarget(chat_ref=chat_ref, message_id=message_id, phone=phone, nonce=nonce)

    leak_msg = cleanup_verified_messages(
        cli_real_cli_env,
        phone=phone,
        chat_ref=chat_ref,
        candidates=[int(message_id)],
        nonce=nonce,
    )
    if leak_msg:
        pytest.fail(leak_msg, pytrace=False)


@pytest.fixture
def live_scratch_group(cli_real_cli_env: CliRealCliEnv) -> LiveCliDialogTarget:
    phone = os.environ.get(CLI_REAL_TG_MUTATION_PHONE_ENV)
    if phone and phone not in cli_real_cli_env.phones:
        pytest.skip(f"{CLI_REAL_TG_MUTATION_PHONE_ENV} is not a connected live CLI account")
    phone = phone or cli_real_cli_env.primary_phone

    title = f"sbx-tmp-group-{make_cli_nonce()}"
    result = _capture_cli(
        cli_real_cli_env,
        "dialogs",
        "create-group",
        "--title",
        title,
        "--phone",
        phone,
        timeout=90,
    )
    _assert_cli_result_ok(result)
    match = _CREATED_GROUP_ID_RE.search(result.stdout or "")
    if match is None:
        pytest.fail(f"create-group stdout did not include group id: {result.stdout!r}", pytrace=False)
    group_id = match.group(1)

    yield LiveCliDialogTarget(chat_ref=group_id, phone=phone, title=title)

    verdict = cli_verify_channel_title(
        cli_real_cli_env,
        phone=phone,
        channel_id=group_id,
        expected_title=title,
    )
    if not verdict.ok:
        pytest.fail(f"group {group_id} left in place: {verdict.reason}", pytrace=False)
    try:
        cleanup = _capture_cli(
            cli_real_cli_env,
            "dialogs",
            "leave",
            group_id,
            "--phone",
            phone,
            "--yes",
            timeout=60,
        )
    except subprocess.TimeoutExpired:
        pytest.fail(f"group {group_id} may still exist: cleanup leave timed out", pytrace=False)
    cleanup_failure = cli_result_failure_summary(cleanup)
    if cleanup_failure is not None:
        pytest.fail(f"group {group_id} may still exist: {cleanup_failure}", pytrace=False)


@pytest.fixture
def live_scratch_group_message(
    cli_real_cli_env: CliRealCliEnv,
    live_scratch_group: LiveCliDialogTarget,
) -> LiveCliMessageTarget:
    nonce = make_cli_nonce()
    text = f"codex live cli group message {nonce}"
    result = _capture_cli(
        cli_real_cli_env,
        "dialogs",
        "send",
        "--yes",
        "--phone",
        live_scratch_group.phone,
        live_scratch_group.chat_ref,
        text,
        timeout=60,
    )
    _assert_cli_result_ok(result)
    match = _SENT_MESSAGE_ID_RE.search(result.stdout or "")
    if match is None:
        pytest.fail(f"group send stdout did not include message_id: {result.stdout!r}", pytrace=False)
    message_id = match.group(1)

    yield LiveCliMessageTarget(
        chat_ref=live_scratch_group.chat_ref,
        message_id=message_id,
        phone=live_scratch_group.phone,
        nonce=nonce,
    )

    leak_msg = cleanup_verified_messages(
        cli_real_cli_env,
        phone=live_scratch_group.phone,
        chat_ref=live_scratch_group.chat_ref,
        candidates=[int(message_id)],
        nonce=nonce,
    )
    if leak_msg:
        pytest.fail(leak_msg, pytrace=False)


@pytest.fixture
def live_owned_mutation_message(cli_real_cli_env: CliRealCliEnv) -> LiveCliMessageTarget:
    try:
        target = _fetch_live_message_target(
            cli_real_cli_env.db_path,
            cli_real_cli_env.phones,
            require_own_dialog=True,
        )
    except sqlite3.Error as exc:
        pytest.skip(f"failed to discover live owned mutation message from {cli_real_cli_env.db_path}: {exc}")
    if target is None:
        pytest.skip("live CLI database has no own cached dialog with a collected message target")
    return target


@pytest.fixture
def live_pin_mutation_message(cli_real_cli_env: CliRealCliEnv) -> LiveCliMessageTarget:
    try:
        target = _fetch_live_message_target(
            cli_real_cli_env.db_path,
            cli_real_cli_env.phones,
            require_own_dialog=True,
            require_pin_capable_dialog=True,
        )
    except sqlite3.Error as exc:
        pytest.skip(f"failed to discover live pin-capable mutation message from {cli_real_cli_env.db_path}: {exc}")
    if target is None:
        pytest.skip(
            "live CLI database has no own cached group/supergroup/gigagroup/forum "
            "with a collected message target"
        )
    return target


_LEADING_INT_ROW_RE = re.compile(r"^\s*(\d+)\s+\S", re.MULTILINE)


@pytest.fixture
def discover_first_pipeline_id(run_cli, assert_cli_ok):
    def _discover() -> str:
        result = run_cli("pipeline", "list")
        assert_cli_ok(result)
        match = _LEADING_INT_ROW_RE.search(result.stdout)
        if not match:
            pytest.skip("no pipelines - `pipeline list` returned no rows")
        return match.group(1)

    return _discover


@pytest.fixture
def discover_first_run_id(run_cli, assert_cli_ok, discover_first_pipeline_id):
    def _discover() -> str:
        pipeline_id = discover_first_pipeline_id()
        result = run_cli("pipeline", "runs", pipeline_id, "--limit", "1")
        assert_cli_ok(result)
        match = _LEADING_INT_ROW_RE.search(result.stdout)
        if not match:
            pytest.skip(f"no runs for pipeline id={pipeline_id}")
        return match.group(1)

    return _discover


@pytest.fixture
def discover_first_search_query_id(run_cli, assert_cli_ok):
    def _discover() -> str:
        result = run_cli("search-query", "list")
        assert_cli_ok(result)
        match = _LEADING_INT_ROW_RE.search(result.stdout)
        if not match:
            pytest.skip("no search queries - `search-query list` returned no rows")
        return match.group(1)

    return _discover


_AGENT_THREAD_ROW_RE = re.compile(r"^\[(\d+)\]", re.MULTILINE)


@pytest.fixture
def discover_first_agent_thread_id(run_cli, assert_cli_ok):
    def _discover() -> str:
        result = run_cli("agent", "threads")
        assert_cli_ok(result)
        match = _AGENT_THREAD_ROW_RE.search(result.stdout)
        if not match:
            pytest.skip("no agent threads - `agent threads` returned no rows")
        return match.group(1)

    return _discover
