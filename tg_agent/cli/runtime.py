from __future__ import annotations

import asyncio
import logging
from collections.abc import Iterable
from logging.handlers import RotatingFileHandler
from pathlib import Path

import click

from tg_agent.cli import worker_handoff
from tg_agent.config import load_config, resolve_session_encryption_secret
from tg_agent.database import Database
from tg_agent.paths import data_root
from tg_agent.settings_utils import parse_int_setting
from tg_agent.telegram.client_pool import ClientPool
from tg_agent.utils.safe_logging import RedactingFormatter
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


NO_ACCOUNTS_HINT = (
    "No connected accounts. Add one with `tg-agent account add` "
    "(or attach shared ones: `tg-agent global enable --account PHONE`)."
)


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


def _install_daemon_waker(config) -> None:
    """Enqueues from this CLI process wake the live daemon's queues at once (#1868).

    Without a daemon (or with one that serves no relay) the hook does nothing and
    the work waits for its next start, exactly as before. The daemon lookup — a
    process probe plus the endpoint file — runs once per event loop, so a bulk
    hand-off (``collect`` over hundreds of channels) pays one loopback round trip
    per row, not a process probe.
    """
    from tg_agent.runtime import wake

    lookup: dict = {}

    def daemon_endpoint():
        loop = asyncio.get_running_loop()
        if lookup.get("loop") is not loop:
            endpoint = None
            if worker_handoff.serve_is_running(config):
                from tg_agent.telegram.relay import read_endpoint, relay_endpoint_path

                endpoint = read_endpoint(relay_endpoint_path(config.database.path))
            lookup.update(loop=loop, endpoint=endpoint)
        return lookup["endpoint"]

    async def wake_daemon(queue: str) -> None:
        endpoint = daemon_endpoint()
        if endpoint is None:
            return
        from tg_agent.telegram.relay import RelayConnection

        try:
            async with asyncio.timeout(2.0):
                connection = await RelayConnection.open(endpoint)
                try:
                    await connection.call("wake", queue=queue)
                finally:
                    await connection.close()
        except BaseException:
            lookup["endpoint"] = None  # the daemon went away: stop knocking for this run
            raise

    wake.set_remote(wake_daemon)


async def init_db(config_path: str):
    config = load_config(config_path)
    _install_daemon_waker(config)
    db = Database(
        config.database.path,
        session_encryption_secret=resolve_session_encryption_secret(config),
        cache_size_kb=config.database.cache_size_kb,
        mmap_size_mb=config.database.mmap_size_mb,
    )
    await db.initialize()
    return config, db


async def refresh_gateway_catalog(config, db) -> None:
    """Gateway-bound project: mirror the shared catalog (best effort, cached on failure)."""
    from tg_agent.gateway.catalog import ProjectAccountCatalog
    from tg_agent.gateway.client import TelegramGatewayClient

    client = None
    try:
        client = await TelegramGatewayClient.from_config(config)
        await ProjectAccountCatalog(db, client).refresh()
    except (OSError, RuntimeError) as exc:
        logging.getLogger(__name__).warning("Gateway unavailable; using cached account metadata: %s",
                                            type(exc).__name__)
    finally:
        if client is not None:
            await client.close()


async def _gateway_relay(config, db):
    """Gateway-bound project: the shared gateway owns the connections (#1868)."""
    from tg_agent.gateway.catalog import connect_project

    return (await connect_project(config, db)).connection


class DaemonRelayUnavailableError(click.ClickException):
    """A live daemon owns the sessions but serves no usable relay (#1868).

    A ``ClickException`` so ``main()`` prints it to stderr and exits 1 without a
    traceback. It only happens across an upgrade: the daemon still runs the old
    version (no relay, or another relay protocol) and must be restarted.
    """


async def _daemon_relay(config):
    """The relay of this project's live daemon, or ``None`` when no daemon runs.

    While a daemon runs it owns every account connection; this process then runs
    its Telegram requests on the daemon's connections instead of opening its own
    (#1868). A daemon that is still starting gets a short grace to publish its
    relay; one that has none is an older version and must be restarted.
    """
    if not worker_handoff.serve_is_running(config):
        return None
    from tg_agent.telegram.relay import RelayConnection, RelayError

    pid = worker_handoff.running_daemon_pid(config)
    holder = f"PID {pid}" if pid is not None else "see `tg-agent daemon status`"
    if worker_handoff.daemon_is_legacy_panel(config):
        raise DaemonRelayUnavailableError(
            f"The running tg-agent daemon ({holder}) is the legacy `serve` panel, which does not serve the "
            "CLI relay. Run the worker daemon instead (once): `tg-agent restart`."
        )
    endpoint = await worker_handoff.wait_for_daemon_relay(config)
    if endpoint is None:
        raise DaemonRelayUnavailableError(
            f"The running tg-agent daemon ({holder}) does not serve the CLI relay — it predates "
            "this tg-agent version. Restart it once: `tg-agent restart`."
        )
    try:
        return await RelayConnection.open(endpoint)
    except (RelayError, OSError) as exc:
        raise DaemonRelayUnavailableError(
            f"Cannot use the running tg-agent daemon ({holder}): {exc}. Restart it: `tg-agent restart`."
        ) from exc


async def init_pool(
    config,
    db: Database,
    *,
    phones: Iterable[str] | None = None,
):
    """Build and initialize this process's ``ClientPool``.

    The pool runs on the connections of whoever owns them: the shared gateway
    (gateway-bound project) or the live daemon, through their relay; otherwise
    it connects the accounts itself.
    """
    if isinstance(config.gateway, dict) and config.gateway:
        relay = await _gateway_relay(config, db)
    else:
        relay = await _daemon_relay(config)
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
        relay=relay,
    )
    await pool.initialize(phones=phones)
    return auth, pool
