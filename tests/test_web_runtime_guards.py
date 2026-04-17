from __future__ import annotations

from pathlib import Path

# Tokens that indicate a live ClientPool / Collector / scheduler call happening
# inside a web request. All Telegram RPC must go through telegram_commands
# enqueued by the route and executed by the worker process.
_DISALLOWED = (
    # Direct client acquisition
    "request.app.state.pool.add_client",
    "request.app.state.pool.get_client_by_phone",
    "request.app.state.pool.get_native_client_by_phone",
    "request.app.state.pool.get_available_client",
    "request.app.state.pool.get_premium_client",
    "request.app.state.pool.get_dialogs_for_phone",
    "request.app.state.pool.warm_all_dialogs",
    "pool.add_client(",
    "pool.get_client_by_phone(",
    "pool.get_native_client_by_phone(",
    "pool.get_available_client(",
    "pool.get_premium_client(",
    "pool.get_dialogs_for_phone(",
    "pool.warm_all_dialogs(",
    "pool.resolve_channel(",
    "pool.get_forum_topics(",
    "pool.remove_client(",
    # Mutation of the shim's clients dict (reads via `pool.clients` are OK —
    # SnapshotClientPool exposes a readonly property).
    "pool.clients[",
    "pool.clients.pop(",
    "pool.clients.update(",
    "pool.clients.clear(",
    # Private-attribute reads that bypass the snapshot contract. If anyone
    # needs one of these in web mode, wrap it with `getattr(pool, "_…", …)`
    # (which is how src/web/routes/debug.py already does it).
    "pool._dialogs_cache",
    "pool._active_leases",
    "pool._in_use",
    "pool._session_overrides",
    "pool._premium_flood_wait_until",
    # Collector live calls
    "collector.collect_",
    "collector.resolve_channel(",
    # Scheduler live mutation from request context
    "scheduler.start(",
    "scheduler.trigger(",
)

# Routes allowed to call *specific* live methods (by design).
# scheduler.py needs start/stop of the local shim, debug.py may inspect
# shim attributes. Keep the allowlist narrow.
_ALLOWLIST: dict[str, tuple[str, ...]] = {
    "scheduler.py": (
        "scheduler.start(",  # local shim toggle; worker is the real owner
        "scheduler.trigger(",  # trigger_warm_background wrapper
    ),
}


def test_web_routes_do_not_call_live_pool_methods() -> None:
    routes_dir = Path("src/web/routes")
    offenders: list[str] = []
    for path in sorted(routes_dir.glob("*.py")):
        if path.name == "__init__.py":
            continue
        text = path.read_text(encoding="utf-8")
        allow = _ALLOWLIST.get(path.name, ())
        for token in _DISALLOWED:
            if token in allow:
                continue
            if token in text:
                offenders.append(f"{path}: {token}")

    assert offenders == [], "\n".join(offenders)


def test_web_services_do_not_call_live_pool_methods() -> None:
    """Services consumed by web routes must also stay off the live pool."""
    services_dir = Path("src/web")
    offenders: list[str] = []
    skip_dirs = {"templates", "static", "routes", "__pycache__"}
    for path in services_dir.rglob("*.py"):
        if any(part in skip_dirs for part in path.parts):
            continue
        # runtime_shims defines the stubs; it is expected to mention pool methods.
        if path.name == "runtime_shims.py":
            continue
        # bootstrap is the only legitimate place where worker-mode initializes
        # the live pool/scheduler — gated behind `if runtime_mode == "worker":`.
        if path.name == "bootstrap.py":
            continue
        text = path.read_text(encoding="utf-8")
        for token in _DISALLOWED:
            if token in text:
                offenders.append(f"{path}: {token}")

    assert offenders == [], "\n".join(offenders)


# src/services/* files that are reachable from src/web/* (constructed via
# deps.py, imported from routes, or aggregated into containers). These
# services must never call live ClientPool methods inside a web request —
# everything must go through telegram_commands + worker dispatcher.
#
# NOTE: keep this list tight. A service only belongs here if the web
# process can actually import/instantiate it (directly or transitively).
# Included on purpose: services where EVERY code path is safe for web mode.
# Excluded on purpose:
# - channel_service.py / collection_service.py — genuinely hybrid today
#   (web uses some methods, worker uses others that touch the live
#   pool/collector). Moving them fully off the live pool is a larger
#   follow-up tracked separately.
# - account_service.py — still calls pool.add_client/remove_client, but it
#   is NOT wired into web anymore (see `test_account_service_not_imported_by_web`
#   below) so the web path cannot reach those calls.
_WEB_CONSUMED_SERVICES = (
    "filter_deletion_service.py",
    "notification_service.py",
    "notification_target_service.py",
    "telegram_command_service.py",
)


def test_account_service_not_imported_by_web() -> None:
    """AccountService still calls pool.add_client/remove_client directly,
    which is only safe from worker-side code paths (telegram_command_dispatcher,
    CLI `account` command). Any re-introduction of the import into src/web/
    would re-open the HTTP 500 regression fixed for #453.
    """
    token = "src.services.account_service"
    web_dir = Path("src/web")
    offenders: list[str] = []
    for path in web_dir.rglob("*.py"):
        if "__pycache__" in path.parts:
            continue
        text = path.read_text(encoding="utf-8")
        if token in text:
            offenders.append(str(path))

    assert offenders == [], "\n".join(offenders)


def test_web_consumed_services_do_not_call_live_pool_methods() -> None:
    """Services imported by web routes/deps must stay off the live pool."""
    services_dir = Path("src/services")
    offenders: list[str] = []
    for name in _WEB_CONSUMED_SERVICES:
        path = services_dir / name
        if not path.exists():
            offenders.append(f"{path}: missing (update _WEB_CONSUMED_SERVICES)")
            continue
        text = path.read_text(encoding="utf-8")
        for token in _DISALLOWED:
            if token in text:
                offenders.append(f"{path}: {token}")

    assert offenders == [], "\n".join(offenders)
