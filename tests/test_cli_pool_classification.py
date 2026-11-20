"""Every CLI site that opens a Telegram pool must declare how it behaves next to the daemon.

While the worker daemon runs it owns every account connection. ``runtime.init_pool()``
then builds a pool on the daemon's relay (#1868): the command runs exactly as without
a daemon, on the daemon's connection. Some work is not a request but a job the daemon
already runs — collection, the hourly ping sweep — and those sites hand it to the
daemon's queues instead of doing it here.

A new ``init_pool`` call site is a deliberate decision; leaving it unclassified fails
here rather than silently shipping.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

CLI_DIR = Path(__file__).resolve().parent.parent / "tg_agent" / "cli"

# Consult the daemon before opening a pool: queue the job for the running worker.
HANDS_OFF = {
    ("commands/channel.py", "collect_impl"),
    ("commands/collect.py", "collect_impl"),
    # ping run-due (#1415): the worker already runs ping_due hourly; a parallel
    # local run would pass the same threshold rule and double-send the nudge —
    # hand off via the PING_DUE task whose has_active_task dedup blocks overlap.
    ("commands/ping.py", "run_due_impl"),
}

# Run in this process; next to a live daemon the pool runs on its relay.
RELAYED = {
    ("commands/agent.py", "chat_impl"),
    ("commands/plugin.py", "plugin_host"),
    ("commands/account.py", "verify_code_impl"),
    ("commands/account.py", "info_impl"),
    ("commands/search.py", "search_impl"),
    ("commands/messages.py", "messages_read_impl"),
    ("commands/photo_loader.py", "_run_with_services"),
    ("commands/test.py", "_run_tg_pool_init_step"),
    ("commands/channel.py", "refresh_types_impl"),
    ("commands/channel.py", "refresh_meta_impl"),
    ("commands/channel.py", "refresh_card_impl"),
    ("commands/channel.py", "add_impl"),
    ("commands/channel.py", "import_impl"),
    ("commands/channel.py", "add_bulk_impl"),
    ("commands/channel.py", "list_for_import_impl"),
    ("commands/channel.py", "probe_impl"),
    ("commands/collect.py", "collect_sample_impl"),
    ("commands/notification.py", "_build"),
    ("commands/dialogs.py", "_dispatch"),
    ("commands/ping.py", "create_impl"),
    ("commands/seeding.py", "_fetch_live_chat_messages"),
    ("pipeline/generation.py", "_generation_pool"),
    ("pipeline/moderation.py", "_pipeline_publish"),
}

CLASSIFIED = HANDS_OFF | RELAYED


def _is_init_pool_call(node: ast.AST) -> bool:
    """``runtime.init_pool(...)`` or a bare ``init_pool(...)`` (``from ... import``)."""
    if not isinstance(node, ast.Call):
        return False
    func = node.func
    if isinstance(func, ast.Attribute):
        return func.attr == "init_pool"
    return isinstance(func, ast.Name) and func.id == "init_pool"


def _pool_calls() -> list[tuple[str, str, ast.Call]]:
    """Every ``init_pool`` call with its file and enclosing function."""
    found: list[tuple[str, str, ast.Call]] = []
    for path in sorted(CLI_DIR.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            for inner in ast.walk(node):
                if _is_init_pool_call(inner):
                    found.append((path.relative_to(CLI_DIR).as_posix(), node.name, inner))
    return found


def _pool_call_sites() -> set[tuple[str, str]]:
    """Find every ``init_pool`` call, attributed to its enclosing function."""
    return {(file_name, func_name) for file_name, func_name, _call in _pool_calls()}


@pytest.mark.mutmut_skip
def test_every_pool_call_site_is_classified():
    """A new ``init_pool`` call must be classified before it can ship.

    Add the (file, function) pair to HANDS_OFF or RELAYED above — HANDS_OFF only
    when the operation is a job the daemon's own queues already run.
    """
    unclassified = _pool_call_sites() - CLASSIFIED

    assert not unclassified, (
        "These CLI functions open a Telegram pool without declaring how they "
        f"behave next to the daemon: {sorted(unclassified)}. "
        "Classify each in tests/test_cli_pool_classification.py."
    )


def test_classification_has_no_stale_entries():
    """Keep the lists honest: a removed call site must not linger as a claim."""
    stale = CLASSIFIED - _pool_call_sites()

    assert not stale, (
        f"These entries no longer open a pool and should be dropped: {sorted(stale)}"
    )


def test_the_two_classes_are_disjoint():
    assert not (HANDS_OFF & RELAYED)


@pytest.mark.parametrize("file_name, func_name", sorted(HANDS_OFF))
def test_hands_off_sites_consult_the_detector(file_name: str, func_name: str):
    """A HANDS_OFF claim must be backed by a real ``serve_is_running`` check."""
    source = (CLI_DIR / file_name).read_text(encoding="utf-8")
    tree = ast.parse(source)

    target = next(
        node
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == func_name
    )
    calls = {
        inner.func.attr
        for inner in ast.walk(target)
        if isinstance(inner, ast.Call) and isinstance(inner.func, ast.Attribute)
    }

    assert "serve_is_running" in calls, (
        f"{file_name}:{func_name} is listed as handing off to the worker but never "
        "calls worker_handoff.serve_is_running()"
    )


# mutmut_skip: repo-contract AST scan — mutmut trampoline defs (x_*__mutmut_N)
# duplicate the guarded call sites inside the mutants sandbox.
@pytest.mark.mutmut_skip
def test_no_site_can_bypass_the_daemon():
    """There is no escape hatch past the owner any more (#1868): ``phones`` only.

    The former ``allow_live_daemon`` flag let a process connect the daemon's auth
    key a second time; with the relay nothing needs it.
    """
    extra = {
        (file_name, func_name, kw.arg)
        for file_name, func_name, call in _pool_calls()
        for kw in call.keywords
        if kw.arg not in {"phones"}
    }

    assert not extra
