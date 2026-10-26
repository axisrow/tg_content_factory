# Contributing to tg-agent

Telegram automation for coding agents — shipped as a **Claude Code plugin** (skill + CLI). The CLI is the contract; the skill drives it.

## Current status: stabilization

The project is in a **bug-fixing phase** — new features are paused. The most
valuable contributions right now are **bug reports with repro steps**. If you
want to write code, pick from the open bug issues or ask first before starting
a feature.

## Reporting bugs

Include:

- `tg-agent --version` (or install commit)
- the exact command and its full output (add `--debug` if available)
- how many accounts are configured and whether a `worker` is running
- what you expected vs what happened

Never paste session strings, phone numbers, or `.env` contents — those are
credentials, not logs.

## Architecture map

```
entrypoints (tg_agent/cli, tg_agent/web, tg_agent/agent)   ← no telethon imports allowed here
        ↓
services + repositories (tg_agent/services, tg_agent/database)
        ↓
telegram backends (tg_agent/telegram)            ← all raw Telethon lives here
```

- **Layering is enforced**, not conventional: `import-linter` (`.importlinter`)
  fails CI if an entrypoint imports `telethon` directly. Raw calls belong in
  `tg_agent/telegram` behind `TelegramTransportSession` and the backend router.
- **Client pool** (`tg_agent/telegram/client_pool.py` + `pool_lifecycle.py` /
  `pool_dialogs.py` / `pool_flood.py`): per-account lifecycle, lease
  acquisition/release, flood-wait rotation. Read the module docstrings first —
  they document the invariants (lock order, pool↔DB ordering, lease teardown).
- **Flood control** is `telethon-floodgate` (circuit breaker + rate gate);
  security-warning recovery lives in `mtproto_watchdog.py`.
- Broader overview: [docs/architecture.md](docs/architecture.md).

## Development setup

```bash
python 3.11+
pip install -e '.[dev]'
```

### Local venv policy

- Any Python within `requires-python` (3.11+) works for local runs; CI pins
  3.11. There is no local version pin — the "py3.13 failures" investigated in
  #1649 were a stale install plus a test-helper leak, not a Python-version
  behavior difference.
- Keep the editable install fresh. An install that predates a packaging
  change (e.g. the #1582 `src/` → `tg_agent/` rename) keeps its metadata
  pointing at the old layout, and console tests fail with
  `ModuleNotFoundError: No module named 'src'`.
  `tests/test_local_install_consistency.py` detects this and fails once with
  the diagnosis instead of leaving cryptic failures to triage.
- Repair is always the same: `pip install -e '.[dev]'`, then
  `python -m pip show tg-agent` to confirm.
- AO worktrees: run the suite with the main checkout's `.venv/bin/pytest` —
  the worktree code is picked up via conftest sys.path prepend. The editable
  install lives in the main checkout; reinstall it there, not per worktree.

Create `.env` in the working directory (never commit it — session strings are
full account access):

```
TG_API_ID=...
TG_API_HASH=...
```

## Tests

The suite runs **offline by default** — no real Telegram connection is needed
or made.

| Marker | Meaning |
|---|---|
| `unit` / `integration` / `smoke` | auto-applied by the conftest heuristic |
| `real_tg_safe` | opt-in real-API test, no Telegram-visible mutations |
| `real_tg_mutation_safe` | opt-in, bounded operator-approved mutations |
| `real_tg_manual` | opt-in, manual/high-risk scenarios |
| `slow` | excluded from the fast loop with `-m 'not slow'` |

```bash
pytest -m 'not slow'          # fast feedback loop
ruff check .
lint-imports                  # import-linter contract
```

Advisory tools wired into CI: `bandit`, `pip-audit`. Optional: `mutmut`
(mutation testing — see `pyproject.toml` notes before running it).

## Where to start

**Lower-bar surfaces** — good first contributions:

- skills and recipes (`skills/`), documentation (`docs/`)
- individual CLI commands (the CLI is the contract — new capabilities land here)
- web dashboard fixes (`tg_agent/web`, frozen but accepting bug fixes)

**Higher-bar core** — read first, then propose:

- `tg_agent/telegram/` (pool, flood, watchdog) — invariants are documented in
  module docstrings and issue references; changes here deserve an issue first

## PR expectations

- one logical change per PR
- acceptance criterion defined before implementation: observable result (condition → action → where to check), touched paths, explicit exclusions from the PR's promise
- tests with the correct level markers; bug fixes need a failing-test-first commit
- PR body states commands, results, and a CI link; partial/cancelled/skipped runs are not a full pass — reconcile the description with the final diff before merging
- docstrings on public functions (interrogate ratchet is a soft floor, don't lower it)
- `ruff check .` and `lint-imports` clean; CI green
