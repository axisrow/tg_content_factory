# tg-shared

Shared Telegram core for two projects ([epic #1416](https://github.com/axisrow/tg_content_factory/issues/1416)):

- **tg-agent** (this repository) — the content factory;
- **tg_messenger** — the standalone single-account TUI messenger.

**Status: scaffold only** (stage 1.1, #1420). No factory code has moved yet —
module migration happens in wave A (#1421, zero-coupling modules) and wave B
(#1422, the flood-protection layer).

The package must work **without the factory at all**: one account, its own
storage, no `AppContainer`.

## Boundary — what lives where

| Capability | Home | Why |
|---|---|---|
| connection, sessions | **this package** | needed by both |
| message models, history reading, sending | **this package** | needed by both |
| flood protection (`flood_wait`, `flood_breaker`, `rate_limit_gate`, `rate_limiter`, `mtproto_watchdog`) | **this package** (wave B) | needed by both |
| incoming-event subscription plumbing | **this package** (stage 2) | needed by both |
| multi-account pool, rotation, leasing (`client_pool`, `pool_lifecycle`, `pool_dialogs`, `account_lease_pool`, `pool_flood`) | **factory** | a single-account messenger needs none of it |
| post collection (`collector_*`) | **factory** | factory's domain logic |
| TUI, web chat, auto-responder | **messenger** | its product |

### `backends.py` split (defined here, executed in the waves)

`tg_agent/telegram/backends.py` splits along its two halves:

- **To the package (transport):** `TelegramTransportSession` — the whole
  send/read/resolve/media/event-handler surface (lines ≈196–981), plus the
  dialog-request gate and flood-context translation it carries. It operates on
  a client the *caller* provides; it knows nothing about pools.
- **Stays in the factory (acquisition):** `TelegramBackend` ABC,
  `NativeTelethonBackend`, `TelethonCliBackend`, `BackendClientLease`,
  `BackendRouter` (≈983–1183). Choosing and leasing a client per account is
  pool-specific; the messenger passes its single client directly.

## Saved-session client preparation (0.2.1)

`TelegramAuth.create_client_from_session(..., before_connect=prepare)` accepts
an optional synchronous callback. It receives the new native client before
connection and authorization RPCs, allowing consumers to attach dispatch
guards. Existing calls without the callback retain their signature and flow.
Preparation, connection or authorization failures disconnect the partial
client before propagating the original error. Pool-owned guard disposal
remains the pool's responsibility.

## Storage contract

The formalization of `tg_agent/database/live_accounts.py`: the shared layer
needs exactly **three things** from storage — account data, `flood_wait_until`
write, and a string key-value for flood-layer persistence (resolve-backoff
payload, consumed in wave B). No factory schema leaks.

```python
from tg_shared import JsonFileStore, SharedAccount, SharedAccountStore

store: SharedAccountStore = JsonFileStore("messenger.json")   # single messenger
store: SharedAccountStore = factory_db                        # runtime conformance; static at #1424
```

Method names mirror the factory facade's existing methods
(`get_accounts` / `update_account_flood` / `get_setting` / `set_setting`), so
the factory `Database` satisfies the Protocol at **runtime** — the
`isinstance` check in the scaffold tests passes, no adapter needed. Static
conformance is deliberately not claimed yet: `Database.get_accounts()`
returns `list[Account]` and `list` is invariant, so a type checker rejects it
as `list[SharedAccount]`. At #1424 the factory closes that gap (returns
`SharedAccount` or adapts through a typed wrapper); until then the factory
line in the snippet is illustrative, not statically checked. The package
never imports factory models (guarded by `tests/test_tg_shared_scaffold.py`).

## Versioning and pinning

- SemVer; version lives in `pyproject.toml` + `CHANGELOG.md` entry per change.
- **Every consumer pins exact versions** (`tg-shared==0.1.0`). Precedent not to
  repeat: `tg-messenger` was declared `>=0.1.0` with no tags and no changelog,
  leaving the factory on an untracked editable checkout.
- Tags (`v0.1.0` …) start once the package has its own repository (below).

## Repository split

While the package lives in this monorepo (`packages/tg_shared/`), the factory
suite imports it via a `sys.path` shim in the scaffold test; CI covers it with
zero workflow changes. Creating the standalone GitHub repository is an owner
decision (external publishing). When it happens:

1. `git subtree split -P packages/tg_shared -b tg-shared-split` → push to the
   new repository; tag `v0.1.0`.
2. Move `tests/test_tg_shared_scaffold.py` (package-owned half) into the new
   repository's test tree; keep the factory-side conformance test here.
3. Factory/messenger switch their dependencies to the exact pin (#1424/#1425).

## Migration strategy (waves, epic #1416)

1. **1.1 #1420 (this scaffold)** — package, Protocol, boundary; nothing moves.
2. **1.2 #1421 wave A** — zero-coupling modules (`auth`, `session_materializer`,
   `botfather`, `identity`, `media`) move into the package; the factory imports
   them from the package. Behavior unchanged, tests move with the modules.
3. **1.3 #1422 wave B** — flood-protection layer moves; its DB touchpoints go
   through `SharedAccountStore` (settings kv for resolve-backoff persistence).
4. **1.5 #1424** — the factory declares the exact-pin dependency and drops the
   monorepo shim.
5. **1.6 #1425** — the messenger switches to the package; its vendored flood
   fork is deleted.

Intermediate states stay green because each wave flips the factory's imports
in the same commit as the module move — no copy-then-switch window.
