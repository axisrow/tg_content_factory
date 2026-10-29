from __future__ import annotations

import logging
from collections.abc import Iterable
from logging.handlers import RotatingFileHandler
from pathlib import Path

import click

from tg_agent.cli import worker_handoff
from tg_agent.config import load_config, resolve_session_encryption_secret
from tg_agent.database import Database
from tg_agent.settings_utils import parse_int_setting
from tg_agent.telegram.client_pool import ClientPool
from tg_agent.utils.safe_logging import RedactingFormatter
from tg_agent.web.paths import data_root
from tg_shared.auth import TelegramAuth

_LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
_LOG_DATEFMT = "%Y-%m-%d %H:%M:%S"


def app_log_path() -> Path:
    """app.log under the runtime data root, resolved at call time (#1595)."""
    return data_root() / "app.log"


def install_log_redaction(handler: logging.Handler, *, fmt: str = _LOG_FORMAT) -> None:
    """Make a handler redact phone/query PII in its emitted output (#785).

    Swaps the handler's formatter for a ``RedactingFormatter`` (idempotent),
    preserving the existing format/datefmt. Redaction is per-handler output only
    and does not mutate the shared LogRecord, so record-level consumers such as
    pytest ``caplog`` keep seeing the original message.
    """
    if isinstance(handler.formatter, RedactingFormatter):
        return
    existing = handler.formatter
    datefmt = getattr(existing, "datefmt", None) or _LOG_DATEFMT
    fmt = getattr(existing, "_fmt", None) or fmt
    handler.setFormatter(RedactingFormatter(fmt, datefmt=datefmt))


def setup_logging(log_path: Path | None = None) -> None:
    log_path = log_path or app_log_path()
    logging.basicConfig(
        level=logging.INFO,
        format=_LOG_FORMAT,
        datefmt=_LOG_DATEFMT,
    )
    # aiosqlite echoes every executed statement WITH its bound params at DEBUG —
    # that includes the encrypted session blob on account inserts. The redaction
    # filter masks phones/queries, not arbitrary SQL params, so pin the driver to
    # INFO so a session secret can't leak even if the root logger is raised to
    # DEBUG for diagnostics (#1146 review).
    logging.getLogger("aiosqlite").setLevel(logging.INFO)
    root = logging.getLogger()
    for h in root.handlers:
        install_log_redaction(h)
    for h in root.handlers:
        if isinstance(h, RotatingFileHandler) and getattr(h, "baseFilename", None) == str(log_path):
            return
    log_path.parent.mkdir(parents=True, exist_ok=True)
    rfh = RotatingFileHandler(
        str(log_path),
        maxBytes=10 * 1024 * 1024,
        backupCount=3,
        encoding="utf-8",
    )
    install_log_redaction(rfh)
    root.addHandler(rfh)


def ensure_data_dirs() -> None:
    """Create all data subdirectories once at startup."""
    for sub in ("image", "images", "downloads", "photo_uploads", "telegram_sessions"):
        (data_root() / sub).mkdir(parents=True, exist_ok=True)


def redirect_logging_to_file(path: str | Path | None = None) -> list[logging.Handler]:
    """Replace console handlers with file handler for TUI mode. Returns removed handlers."""
    path = path or data_root() / "agent_tui.log"
    root = logging.getLogger()
    removed: list[logging.Handler] = []
    for h in root.handlers[:]:
        if isinstance(h, logging.StreamHandler) and not isinstance(h, logging.FileHandler):
            removed.append(h)
            root.removeHandler(h)
    fh = logging.FileHandler(str(path), encoding="utf-8")
    install_log_redaction(fh)
    root.addHandler(fh)
    return removed


def restore_logging(removed_handlers: list[logging.Handler] | logging.Handler | None) -> None:
    """Restore console handlers after TUI exits."""
    root = logging.getLogger()
    for h in root.handlers[:]:
        # Remove only plain FileHandlers (TUI log), not RotatingFileHandler (app.log)
        if type(h) is logging.FileHandler:
            h.close()
            root.removeHandler(h)
    if isinstance(removed_handlers, list):
        for h in removed_handlers:
            root.addHandler(h)
    elif removed_handlers:
        root.addHandler(removed_handlers)


async def init_db(config_path: str):
    config = load_config(config_path)
    db = Database(
        config.database.path,
        session_encryption_secret=resolve_session_encryption_secret(config),
        cache_size_kb=config.database.cache_size_kb,
        mmap_size_mb=config.database.mmap_size_mb,
    )
    await db.initialize()
    if isinstance(config.gateway, dict) and config.gateway:
        import httpx

        from tg_agent.gateway.catalog import ProjectAccountCatalog
        from tg_agent.gateway.client import TelegramGatewayClient

        client = None
        try:
            client = await TelegramGatewayClient.from_config(config)
            await ProjectAccountCatalog(db, client).refresh()
        except (OSError, RuntimeError, httpx.HTTPError) as exc:
            logging.getLogger(__name__).warning("Gateway unavailable; using cached account metadata: %s",
                                              type(exc).__name__)
        finally:
            if client is not None:
                await client.close()
    return config, db


class DaemonOwnsSessionsError(click.ClickException):
    """A live worker daemon owns the Telegram sessions; refuse a second pool (#1521).

    A ``ClickException`` so ``main()`` prints it to stderr and exits 1 without a
    traceback; callers that deliberately swallow pool-init failures (best-effort
    warm-ups) keep doing so and get a readable ``str(exc)``.
    """


def _refuse_if_daemon_owns_sessions(config, *, allow_live_daemon: bool) -> None:
    """Fail fast instead of building a pool next to a live daemon (#1521).

    The daemon holds every account session for its lifetime (#1548 session
    lock), so an in-process pool here would wait out the lock grace per account
    and end up empty — or, without the lock, put a second MTProto connection on
    the same auth key. Commands with a queue equivalent hand off to the worker
    *before* reaching this point; everything else is refused here, in the one
    entry point all of them share.
    """
    if allow_live_daemon or worker_handoff.direct_requested():
        return
    if not worker_handoff.serve_is_running(config):
        return
    pid = worker_handoff.running_daemon_pid(config)
    holder = f"PID {pid}" if pid is not None else "the PID file"
    raise DaemonOwnsSessionsError(
        f"A running tg-agent daemon ({holder}) owns the Telegram sessions, and this "
        "command needs its own connection (it has no worker hand-off). Stop the "
        "daemon first (`tg-agent stop`), run the command, then `tg-agent restart`."
    )


async def init_pool(
    config,
    db: Database,
    *,
    phones: Iterable[str] | None = None,
    allow_live_daemon: bool = False,
):
    """Build and initialize this process's ``ClientPool``.

    Refuses with :class:`DaemonOwnsSessionsError` while a worker daemon is live,
    unless ``allow_live_daemon`` (an explicit ``--direct``, or a long-lived
    legacy surface with its own lifecycle) or ``TGCF_CLI_DIRECT=1`` is set.
    """
    if isinstance(config.gateway, dict) and config.gateway:
        from tg_agent.gateway.pool import GatewayPool

        pool = GatewayPool(config, db)
        await pool.initialize()
        return None, pool
    _refuse_if_daemon_owns_sessions(config, allow_live_daemon=allow_live_daemon)
    api_id = config.telegram.api_id
    api_hash = config.telegram.api_hash
    if api_id == 0 or not api_hash:
        stored_id = await db.get_setting("tg_api_id")
        stored_hash = await db.get_setting("tg_api_hash")
        if stored_id and stored_hash:
            api_id = parse_int_setting(
                stored_id,
                setting_name="tg_api_id",
                default=0,
                logger=logging.getLogger(__name__),
            )
            api_hash = stored_hash

    auth = TelegramAuth(api_id, api_hash, proxy=config.telegram_runtime.proxy)
    pool = ClientPool(
        auth,
        db,
        config.scheduler.max_flood_wait_sec,
        runtime_config=config.telegram_runtime,
    )
    await pool.initialize(phones=phones)
    return auth, pool
