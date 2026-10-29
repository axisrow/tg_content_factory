# Shared Telegram gateway implementation

## Acceptance contract

Two independent tg-agent projects, each with its own database and worker,
must collect through one gateway-owned Telegram session. Project-local
stop/restart/cancellation must leave the other project operational. Shared
accounts require explicit grants and project-specific primary selection.
All existing Telegram capabilities must work through the gateway before
the new global mode is enabled for users.

## Current implementation checkpoint

The internal gateway service is implemented but is not connected to the
normal CLI/Web/project-worker paths or enabled as a replacement for global
mode. The existing global mode still redirects the whole database.

Implemented internal components:

- A dedicated encrypted account DB, stable project identities, hashed project
  tokens, explicit grants and independent project primary accounts.
- A typed, authenticated `/v1` HTTP API and client for bounded history pages,
  send, edit, delete and generic file publishing. Binary artifacts are scoped
  to the uploading project, limited to 128 MiB and expire after 24 hours when
  accessed. Background cleanup and complete photo/album semantics remain pending.
  Other existing Telegram operations are still pending.
- A durable request journal, payload-conflict checks, fair project dispatch,
  project-local outbox IDs and uncertain-write recovery without automatic replay.
- A loopback-only subprocess runtime with fixed launcher/dependency paths,
  cross-process startup locking and private nonce-bound endpoint discovery.
- One cached Telegram client per account, shared flood state and transport
  gates; native DM listeners write deduplicated events into the seven-day
  journal. Projects can independently read/acknowledge events. Automatic sends
  require the unique dialog owner.

Tests exercise two HTTP clients persisting results into independent real
project databases, a restarted project outbox, permission isolation, ambiguous
delivery, event fan-out and a real concurrent subprocess startup. Live Telegram
transport is replaced by a local double in these tests; they are not evidence
of a successful real Telegram run.

The first process-control safeguard records provenance beside the existing
numeric PID file (`.pid.owner.json`): project root, store root, executable,
PID, start timestamp, role and command. A stop from a different project,
a mismatching recorded PID/command, or malformed provenance is refused.
Numeric PID compatibility is preserved for existing readers and hooks.
Legacy processes without provenance retain existing behavior; this is not
the complete #1714 solution or a PID-reuse guarantee.

Verification includes real child processes: a foreign project cannot stop
the process; its owner can stop it. The original implementation was checked
with the same foreign-directory scenario and terminated the process.

## Remaining implementation sequence

1. Wire the new store/identities into project configuration and account commands;
   implement resumable legacy migration and project-local metadata mapping.
2. Complete the typed operation inventory, photo/album semantics and artifact cleanup.
3. Move collection, search and other reads to gateway-owned clients, with
   independent transactional project cursors and fair page scheduling.
4. Move authentication, publishing, media, native operations, BotFather and
   notifications. Persist request IDs before submission; preserve uncertain
   delivery without automatically repeating writes.
5. Wire native DM events into the existing project archives/catch-up services;
   complete separate worker/gateway lifecycle records, explicit shared
   administration, restart preflight and status surfaces.
6. Complete CLI/Web/tools coverage, both pytest lanes, Ruff and the live
   two-project Telegram scenario. Report offline and live evidence separately.

Do not switch global enable to the new mode until the full transport and
all user surfaces are connected. No live Telegram checks have been run
at this checkpoint.

## Verification at this checkpoint

- `ruff check tg_agent/ tests/ conftest.py`: passed.
- Parallel-safe lane (`-m 'not aiosqlite_serial' -n 2`): 10,288 passed,
  152 skipped, 355 subtests passed.
- DB-sensitive lane (`-m aiosqlite_serial -n 2 --dist=loadfile`): 1,150 passed.
- Latest gateway/provenance checks after the final edits: 19 passed.
- Live Telegram checks: not run; the execution environment has no configured
  `TG_API_ID`, `TG_API_HASH` or `SESSION_ENCRYPTION_KEY`.

The full lanes ran while internal gateway work continued; the final focused
run covers the latest gateway edits. These results do not establish completion
of the remaining product integration or a successful live deployment.
