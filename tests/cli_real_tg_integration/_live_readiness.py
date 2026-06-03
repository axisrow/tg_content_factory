"""Live-readiness predicate for the CLI real-Telegram integration suite.

This is a leaf module (no pytest fixtures) so both the root ``tests/conftest.py``
and the per-folder conftests can import the env-gate helper without pulling in
``tests/cli_real_tg_integration/conftest.py`` at module-load time (which would
risk a circular import / double fixture registration). The path resolvers and
``_fetch_live_accounts`` already live in that conftest, so we import them lazily
*inside* the function bodies here: conftest imports from this module at the top
level, this module imports conftest only when called.

The point of the predicate is to let ``pytest tests/cli_real_tg_integration/...``
run without any manual ``RUN_*`` env vars whenever the project is genuinely
configured for live Telegram work. Env vars stay as an optional override
(``=1`` forces on, ``=0`` forces off). When the project is not ready (e.g. CI
with no populated DB / accounts), the predicate returns False and every gate
stays closed — identical to today's graceful-skip behaviour.
"""
from __future__ import annotations

import functools
import os
import sqlite3
from collections.abc import Mapping
from pathlib import Path

from src.config import load_config

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


@functools.lru_cache(maxsize=1)
def live_cli_project_ready() -> bool:
    """True iff the project is actually configured for live Telegram tests.

    All of the following must hold (any sqlite/IO/parse error → False):
      1. ``config.yaml`` is resolvable and exists (needed at least for the DB path);
      2. ``load_config`` succeeds and ``config.database.path`` exists and is non-empty;
      3. both api_id and api_hash are present (config → settings-table fallback);
      4. at least one active account exists (``_fetch_live_accounts`` non-empty).

    Cheap (synchronous sqlite reads + ``load_config`` only — no CLI subprocess,
    no Telegram connection) and cached for one process. Unit tests that vary the
    environment must call ``live_cli_project_ready.cache_clear()`` between cases.
    """
    try:
        # Lazy import to avoid a module-load-time cycle with conftest.
        from src.cli.dotenv import load_cli_dotenv
        from tests.cli_real_tg_integration import conftest as _cf

        live_root = _cf._resolve_live_root()
        config_path = _cf._resolve_config_path(live_root)
        if not config_path.exists():
            return False
        load_cli_dotenv(config_path)
        config = load_config(config_path)
        db_path = _cf._resolve_db_path(live_root, config.database.path)
        if not db_path.exists() or db_path.stat().st_size == 0:
            return False
        api_id, api_hash = _resolve_api_credentials(config, db_path)
        if not api_id or not api_hash:
            return False
        if not _cf._fetch_live_accounts(db_path):
            return False
        return True
    except (sqlite3.Error, OSError, ValueError):
        return False


def _gate_enabled(env_name: str, environ: Mapping[str, str] = os.environ) -> bool:
    """Single source of truth for whether a live-Telegram gate is open.

    - env set to a true token (1/true/yes/on)  → True  (force on);
    - env set to a false token (0/false/no/off) → False (force off / kill switch);
    - env set to anything else                  → False (keeps the old strict
      "value must be 1" semantics);
    - env unset                                 → ``live_cli_project_ready()`` (auto).
    """
    raw = environ.get(env_name)
    if raw is None:
        return live_cli_project_ready()
    token = raw.strip().lower()
    if token in TRUE_TOKENS:
        return True
    if token in FALSE_TOKENS:
        return False
    return False
