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
