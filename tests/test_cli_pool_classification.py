"""Every CLI site that opens a Telegram pool must declare how it behaves vs `serve`.

`serve` owns the Telegram session files (``data/telegram_sessions/<phone>.session``,
keyed by phone alone), so a CLI process building its own ``ClientPool`` opens the
same files and risks ``database is locked``. Two commands already learned that the
hard way.

This is the guard that stops the class from coming back: a new ``init_pool`` call
site is a deliberate decision about concurrency, and leaving it unclassified fails
here rather than silently shipping another competing connection.

Since #1521 ``runtime.init_pool()`` itself refuses while a daemon is live, so the
default for an unlisted-but-classified site is REFUSED. Only a site that passes
``allow_live_daemon=...`` can build a pool next to the daemon, and only the
sites listed in ``BYPASSES_GUARD`` may pass it.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

COMMANDS_DIR = Path(__file__).resolve().parent.parent / "tg_agent" / "cli" / "commands"

# Consult the daemon before opening a pool: hand work to the running worker
# through a queue, or (generation) run without Telegram access.
HANDS_OFF = {
    ("channel.py", "collect_impl"),
    ("collect.py", "collect_impl"),
    ("dialogs.py", "_dispatch"),
    # pipeline run / generate (#1521): next to a live daemon generation runs
    # without a pool — Telegram-side nodes fail without one, so pipelines
    # containing them need the daemon stopped.
    ("pipeline.py", "_generation_pool"),
    # ping run-due (#1415): the worker already runs ping_due hourly; a parallel
    # local run would pass the same threshold rule and double-send the nudge —
    # hand off via the PING_DUE task whose has_active_task dedup blocks overlap.
    ("ping.py", "run_due_impl"),
}

# Refused while a daemon is live (#1521): no queue equivalent, so
# ``runtime.init_pool()`` raises ``DaemonOwnsSessionsError`` (exit 1, "tg-agent
# stop" hint) before any pool is built. These used to "run anyway", but since the
# per-session lock (#1548) the daemon holds every session for its lifetime: the
# CLI pool only waited out the lock grace per account and came up empty.
REFUSED_WHILE_DAEMON = {
    # plugin run --with-pool: generic tools have no worker-queue handoff.
    # Use the normal guard (or configured gateway), never allow_live_daemon.
    ("plugin.py", "plugin_host"),
    # verify-code (#1521): the account is persisted first; the pool is only a
    # best-effort premium warm-up whose failure the command already reports.
    ("account.py", "verify_code_impl"),
    ("account.py", "info_impl"),
    ("search.py", "search_impl"),
    ("messages.py", "messages_read_impl"),
    ("photo_loader.py", "_run_with_services"),
    ("test.py", "_run_tg_pool_init_step"),
    ("channel.py", "refresh_types_impl"),
    ("channel.py", "refresh_meta_impl"),
    ("channel.py", "add_impl"),
    ("channel.py", "import_impl"),
    ("channel.py", "add_bulk_impl"),
    ("channel.py", "list_for_import_impl"),
    ("collect.py", "collect_sample_impl"),
    ("notification.py", "_build"),
    ("pipeline.py", "_pipeline_publish"),
    # ping create (#1415): --username resolve is a read-only RPC, one RPC per
    # operator-invoked create — same shape as the diagnostics above.
    ("ping.py", "create_impl"),
    # probe (#1622): read-only live quality card for a channel that is not in
    # the DB yet — the worker queue has no equivalent (it only touches
    # collected channels).
    ("channel.py", "probe_impl"),
}

# Intentionally untouched: frozen long-lived legacy surfaces with their own
# lifecycle. They pass ``allow_live_daemon=True`` and keep the pre-#1521
# behaviour (the session lock still keeps them off the daemon's sessions).
EXEMPT = {
    ("agent.py", "chat_impl"),
    ("mcp_server.py", "_serve"),
}

CLASSIFIED = HANDS_OFF | REFUSED_WHILE_DAEMON | EXEMPT

# The only sites allowed to pass ``allow_live_daemon``: the exempt surfaces, and
# hand-off sites forwarding an explicit operator ``--direct``.
BYPASSES_GUARD = EXEMPT | {
    ("channel.py", "collect_impl"),
    ("dialogs.py", "_dispatch"),
}


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
    for path in sorted(COMMANDS_DIR.glob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            for inner in ast.walk(node):
                if _is_init_pool_call(inner):
                    found.append((path.name, node.name, inner))
    return found


def _pool_call_sites() -> set[tuple[str, str]]:
    """Find every ``init_pool`` call, attributed to its enclosing function."""
    return {(file_name, func_name) for file_name, func_name, _call in _pool_calls()}


@pytest.mark.mutmut_skip
def test_every_pool_call_site_is_classified():
    """A new ``init_pool`` call must be classified before it can ship.

    Add the (file, function) pair to HANDS_OFF, REFUSED_WHILE_DAEMON or EXEMPT above —
    and prefer HANDS_OFF whenever the operation has a queue equivalent.
    """
    unclassified = _pool_call_sites() - CLASSIFIED

    assert not unclassified, (
        "These CLI functions open a Telegram pool without declaring how they "
        f"behave while `serve` is running: {sorted(unclassified)}. "
        "Classify each in tests/test_cli_pool_classification.py."
    )


def test_classification_has_no_stale_entries():
    """Keep the lists honest: a removed call site must not linger as a claim."""
    stale = CLASSIFIED - _pool_call_sites()

    assert not stale, (
        f"These entries no longer open a pool and should be dropped: {sorted(stale)}"
    )


def test_the_three_classes_are_disjoint():
    """A call site has exactly one behaviour; overlap would make the guard lie."""
    assert not (HANDS_OFF & REFUSED_WHILE_DAEMON)
    assert not (HANDS_OFF & EXEMPT)
    assert not (REFUSED_WHILE_DAEMON & EXEMPT)


@pytest.mark.parametrize("file_name, func_name", sorted(HANDS_OFF))
def test_hands_off_sites_consult_the_detector(file_name: str, func_name: str):
    """A HANDS_OFF claim must be backed by a real ``serve_is_running`` check.

    Without this the entry is just a comment: the function could keep connecting
    unconditionally while the list claims otherwise.
    """
    source = (COMMANDS_DIR / file_name).read_text(encoding="utf-8")
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
def test_only_listed_sites_bypass_the_daemon_guard():
    """``allow_live_daemon`` re-opens the #1521 hole, so every use must be listed.

    A refused site passing it would silently build a second pool next to the
    daemon again; a listed site that stopped passing it makes its claim stale.
    """
    passing = {
        (file_name, func_name)
        for file_name, func_name, call in _pool_calls()
        if any(kw.arg == "allow_live_daemon" for kw in call.keywords)
    }

    assert passing == BYPASSES_GUARD
