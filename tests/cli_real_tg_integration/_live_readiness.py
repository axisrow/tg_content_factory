"""Env-gate helper for the CLI real-Telegram integration suite.

This is a leaf module (no pytest fixtures) so both the root ``tests/conftest.py``
and the per-folder conftests can import the env-gate helper without pulling in
``tests/cli_real_tg_integration/conftest.py`` at module-load time (which would
risk a circular import / double fixture registration).

Every live-Telegram gate is **opt-in only**: a test runs solely when its
``RUN_*`` env var is explicitly set to a true token. There is no auto-enable —
these tests touch a real account and must never start on their own (e.g. just
because a dev machine happens to be configured for live Telegram). ``_gate_enabled``
is the single source of truth for that policy.

``_resolve_api_credentials`` lives here (rather than in conftest) so it stays a
fixture-free import for the live CLI fixture in conftest.
"""
from __future__ import annotations

import os
import sqlite3
from collections.abc import Mapping
from pathlib import Path

# Tokens that explicitly force a gate on/off when the env var is set.
TRUE_TOKENS = frozenset({"1", "true", "yes", "on"})
FALSE_TOKENS = frozenset({"0", "false", "no", "off"})


def _resolve_api_credentials(config, db_path: Path) -> tuple[int | None, str | None]:
    """Resolve Telegram api_id/api_hash the same way production does.

    Mirrors ``src/cli/runtime.py:init_pool`` (the config → settings-table
    fallback), but synchronously via sqlite3. ``config.telegram.*`` already
    folds in the ``TG_API_ID``/``TG_API_HASH`` env vars (see ``load_config`` in
    ``src/config.py``); only when those are empty do we read the ``settings``
    table keys ``tg_api_id``/``tg_api_hash``.

    Never raises: any sqlite error or incomplete pair yields ``(None, None)``.
    """
    api_id = config.telegram.api_id
    api_hash = config.telegram.api_hash
    if api_id == 0 or not api_hash:
        try:
            with sqlite3.connect(db_path) as conn:
                stored_id_row = conn.execute(
                    "SELECT value FROM settings WHERE key = ?", ("tg_api_id",)
                ).fetchone()
                stored_hash_row = conn.execute(
                    "SELECT value FROM settings WHERE key = ?", ("tg_api_hash",)
                ).fetchone()
        except sqlite3.Error:
            return None, None
        stored_id = stored_id_row[0] if stored_id_row else None
        stored_hash = stored_hash_row[0] if stored_hash_row else None
        if stored_id and stored_hash:
            try:
                api_id = int(stored_id)
            except (TypeError, ValueError):
                api_id = 0
            api_hash = stored_hash
    if not api_id or not api_hash:
        return None, None
    return int(api_id), str(api_hash)


def _gate_enabled(env_name: str, environ: Mapping[str, str] = os.environ) -> bool:
    """Single source of truth for whether a live-Telegram gate is open.

    Opt-in only — the gate opens only when the env var is explicitly truthy:

    - env set to a true token (1/true/yes/on)   → True  (run);
    - env set to a false token (0/false/no/off) → False (skip / kill switch);
    - env set to anything else                  → False (strict "must be 1");
    - env unset                                 → False (skip — no auto-enable).

    These tests act on a real Telegram account, so they must never start
    without an explicit opt-in.
    """
    raw = environ.get(env_name)
    if raw is None:
        return False
    token = raw.strip().lower()
    if token in TRUE_TOKENS:
        return True
    if token in FALSE_TOKENS:
        return False
    return False
