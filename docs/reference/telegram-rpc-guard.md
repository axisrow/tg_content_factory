# Shared RPC flood control

The publishing/read integration requires **telethon-floodgate 0.1.10** and
**tg-shared 0.2.1** (the pre-connect callback). It
attaches the upstream dispatch guard before a pool-owned native or
telethon-cli client connects. Rebinding an adapter does not install another
guard or reset account state. Gateway projects use the gateway account UUID
as the stable budget key; standalone pools use their existing phone key.

## Accounting

The account gate, peer buckets, username limiter, breaker and process ceiling
remain the existing pool objects. The upstream two-phase admission composes
them before dispatch; a local refusal does not spend an unsent RPC's budgets.
The process ceiling waits for capacity, then rechecks the other budgets.
Short category/peer deferrals (up to five seconds per check) wait at the same
boundary too, allowing BotFather prompt sequences to finish. Longer deferrals
and username-budget exhaustion return structured refusal. Existing operation
timeouts bound the wait; no already-dispatched command is replayed.

- Each physical RPC attempt spends one process-ceiling slot, including nested
  username lookups, pages, retries, conversations, native Message operations,
  event-filter resolution, transfers and exported/CDN sender traffic.
- Username burst/sustained/ramp-up policy applies only to real
  ResolveUsername RPCs. Session-cache hits are free. Explicit resolve wrappers
  do not reserve a second username slot on an already guarded client.
- Logical history/dialog admission is separate from physical dispatch. The
  adapter/page proxy no longer duplicates the guard's physical charges.
- Category/peer limits retain their configured values. Transfers do not use
  send quotas. Unknown RPCs still pass the bounded default policy.

## Flood state and delivery

Read-side floods are attributed to the actual RPC family, including resolves
performed while parsing a publication. GatewayPoolStorage stores their UTC
deadlines in `account_family_floods`, keyed by `(account_id, family)`. Schema
creation is additive and idempotent. Mutating or unattributed floods retain
account-wide protection. Existing account-wide deadlines are not cleared by
this upgrade. Resolve snapshots, tightening and backoff continue to persist
through the existing settings store and are restored before clients connect.
Restore runs once per pool lifetime, so a repeated initialization does not
replay the running pool's own snapshot and multiply already spent slots.

New receipts may include `diagnostics` with `outer_operation`, `rpc`,
`category`, `source` and `delivery_state`. Local throttling and Telegram waits
retain `retry_at`; the CLI names the RPC and deadline. Existing receipts
without diagnostics remain readable. Request arguments, message bodies and
raw exception text are not exported in this metadata.

`not_sent` means no mutating request reached the sender. `partial` means a
prior mutation completed before a later failure. `uncertain` preserves the
existing delivery precaution after a possible send or an untracked failure.
Partial/uncertain writes keep their original intent and are never
automatically replayed. Gateway notifications opt into structured throttle
propagation; ordinary `Notifier.notify()` retains its bool default.

## Release verification

Publish both fixed library releases before finishing the application's
dependency update. Use the PyPI wheel, then update the minimum version and
run `uv lock --upgrade-package telethon-floodgate --upgrade-package tg-shared`. Do not substitute a Git
dependency or guess future artifact hashes. A local wheel is sufficient for
developing the integration, but is not the final lock-file verification.

The offline regressions use real Telethon parsing and sender Futures, plus
the actual CLI, gateway HTTP API and SQLite store. They check mention fan-out,
warm caches, pagination/retries, shared-account budgets, persisted family
floods and non-replay of interrupted/partial publications. Live Telegram
operations are not part of these checks.
