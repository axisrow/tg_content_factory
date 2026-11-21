"""Flood-guard state that survives the process (#1868 follow-up).

The pool's proactive flood guards keep their state in memory: the per-category
and per-peer sliding windows of the ``TelegramRateLimitGate`` (burst *and* the
hour-long sustained tier), the ``FloodCircuitBreaker`` suspensions, and the
process-wide ``OutgoingRateCeiling``. Without a snapshot every daemon restart —
and every CLI run that connects the account itself — started on a full budget,
so the volume Telegram had already counted was spent a second time.

This module serializes those guards into one DB setting with wall-clock stamps
(monotonic stamps die with the process) and replays them into a fresh pool:
expired entries drop out, open circuits resume their remaining cooldown, and a
half-open trial that died with the old process becomes a fresh trial. The resolve
limiter has its own snapshot (``resolve_guard.RESOLVE_LIMITER_SNAPSHOT_SETTING``).

Only the session owner writes and restores (a pool on an owner's relay is
gated by the owner, whose snapshot is authoritative). Restore runs once per pool
lifetime, before any traffic: ``restore_state`` merges stamps without
de-duplication, so replaying our own snapshot twice would double-count.

The gate and breaker internals belong to telethon-floodgate (pinned <0.2);
every component is restored independently and a drifted shape is skipped with
a warning, never a failed boot.
"""

from __future__ import annotations

import json
import logging
import time
from typing import Any

logger = logging.getLogger(__name__)

FLOOD_GUARD_SNAPSHOT_SETTING = "flood_guard_snapshot"
# The worker and the gateway persist on this cadence while running, and always on
# teardown; a crash loses at most this much accounting.
FLOOD_GUARD_PERSIST_INTERVAL_SEC = 30.0
_VERSION = 1
_BREAKER_STATES = frozenset({"closed", "open", "half_open"})


def _live_entries(limiter_snapshot: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Per-phone entries that still carry something: in-window stamps or a tightened budget."""
    return {
        str(phone): entry
        for phone, entry in limiter_snapshot.items()
        if entry.get("burst") or entry.get("sustained") or entry.get("tightened")
    }


def _snapshot_gate(gate: Any) -> dict[str, Any]:
    limiters = getattr(gate, "_limiters", None)
    if not isinstance(limiters, dict):
        return {}
    categories = {}
    for category, limiter in limiters.items():
        entries = _live_entries(limiter.snapshot_state())
        if entries:
            categories[str(category)] = entries
    peers = []
    for key, limiter in list(getattr(gate, "_peer_buckets", {}).items()):
        phone, category, peer = key
        entry = _live_entries(limiter.snapshot_state()).get(str(phone))
        if entry:
            peers.append([str(phone), str(category), str(peer), entry])
    state: dict[str, Any] = {}
    if categories:
        state["categories"] = categories
    if peers:
        state["peers"] = peers
    return state


def _snapshot_breaker(breaker: Any) -> list[dict[str, Any]]:
    states = getattr(breaker, "_states", None)
    if not isinstance(states, dict):
        return []
    fails = getattr(breaker, "_fails", {})
    open_until = getattr(breaker, "_open_until", {})
    now = breaker._time()
    entries = []
    for key, state in states.items():
        operation, phone = key
        count = int(fails.get(key, 0))
        if state == "closed" and count == 0:
            continue
        deadline = open_until.get(key)
        entries.append({
            "operation": str(operation),
            "phone": str(phone),
            "state": str(state),
            "fails": count,
            "open_for": max(0.0, deadline - now) if deadline is not None else 0.0,
        })
    return entries


def snapshot_flood_guards(*, gate: Any = None, breaker: Any = None, ceiling: Any = None) -> dict[str, Any]:
    """A JSON-ready snapshot of the pool's flood guards (empty components are omitted)."""
    # Times stay exact (rounding one up lengthened a restored pause), and the wall
    # clock is read before any component clock: the gap can only shorten a cooldown.
    payload: dict[str, Any] = {"version": _VERSION, "taken_at": time.time()}
    for name, build in (
        ("gate", lambda: _snapshot_gate(gate)),
        ("breaker", lambda: _snapshot_breaker(breaker)),
        ("ceiling", lambda: _live_entries(ceiling.snapshot_state()) if ceiling is not None else {}),
    ):
        try:
            state = build()
        except (AttributeError, TypeError, ValueError) as exc:
            logger.warning("flood-guard snapshot: %s skipped (%s)", name, type(exc).__name__)
            continue
        if state:
            payload[name] = state
    return payload


def _restore_gate(gate: Any, state: dict[str, Any]) -> tuple[int, int]:
    limiters = gate._limiters
    categories = 0
    for category, entries in (state.get("categories") or {}).items():
        limiter = limiters.get(category)
        if limiter is None:  # a category this build no longer configures
            continue
        limiter.restore_state(entries)
        categories += 1
    peers = 0
    for phone, category, peer, entry in state.get("peers") or ():
        bucket = gate._peer_bucket(str(phone), str(category), str(peer))
        if bucket is None:  # the <category>:<kind> pair has no peer spec here
            continue
        bucket.restore_state({str(phone): entry})
        peers += 1
    return categories, peers


def _restore_breaker(breaker: Any, entries: list[dict[str, Any]], taken_at: Any) -> int:
    # The breaker clock first, the wall clock second: time passing between the two
    # reads then counts as elapsed instead of stretching the resumed cooldown.
    now = breaker._time()
    try:
        elapsed = max(0.0, time.time() - float(taken_at))
    except (TypeError, ValueError):
        elapsed = 0.0
    restored = 0
    for entry in entries:
        key = (str(entry["operation"]), str(entry["phone"]))
        state = str(entry["state"])
        if key in breaker._states or state not in _BREAKER_STATES:
            continue
        remaining = float(entry.get("open_for") or 0.0) - elapsed
        if state == "half_open":
            # The granted trial died with the old process: still suspect, so a
            # fresh trial is due now rather than a free pass for every caller.
            state, remaining = "open", 0.0
        if state == "open":
            breaker._open_until[key] = now + max(0.0, remaining)
        breaker._states[key] = state
        fails = int(entry.get("fails") or 0)
        if fails:
            breaker._fails[key] = fails
        restored += 1
    return restored


def restore_flood_guards(
    payload: Any, *, gate: Any = None, breaker: Any = None, ceiling: Any = None
) -> dict[str, int]:
    """Replay :func:`snapshot_flood_guards` into fresh guards; returns what was restored.

    Each component is restored on its own: a malformed or drifted one is skipped
    with a warning and the rest still apply.
    """
    counts = {"categories": 0, "peers": 0, "breaker": 0, "ceiling": 0}
    if not isinstance(payload, dict) or payload.get("version") != _VERSION:
        logger.warning("flood-guard snapshot ignored: unknown format")
        return counts
    if gate is not None and payload.get("gate"):
        try:
            counts["categories"], counts["peers"] = _restore_gate(gate, payload["gate"])
        except (AttributeError, TypeError, ValueError, KeyError) as exc:
            logger.warning("flood-guard snapshot: gate skipped (%s)", type(exc).__name__)
    if breaker is not None and payload.get("breaker"):
        try:
            counts["breaker"] = _restore_breaker(breaker, payload["breaker"], payload.get("taken_at"))
        except (AttributeError, TypeError, ValueError, KeyError) as exc:
            logger.warning("flood-guard snapshot: breaker skipped (%s)", type(exc).__name__)
    if ceiling is not None and payload.get("ceiling"):
        try:
            ceiling.restore_state(payload["ceiling"])
            counts["ceiling"] = 1
        except (AttributeError, TypeError, ValueError, KeyError) as exc:
            logger.warning("flood-guard snapshot: ceiling skipped (%s)", type(exc).__name__)
    return counts


class FloodSnapshotMixin:
    """Persist and restore a pool's flood guards (owner pools only)."""

    def _flood_guards(self) -> dict[str, Any]:
        return {
            "gate": getattr(self, "_rate_limit_gate", None),
            "breaker": getattr(self, "_flood_breaker", None),
            "ceiling": getattr(self, "_outgoing_ceiling", None),
        }

    async def persist_flood_guard_snapshot(self) -> None:
        """Best-effort: write the guards' state; a failure never breaks the caller."""
        db = getattr(self, "_db", None)
        if db is None or vars(self).get("_relay") is not None:
            return  # a relayed pool is gated by the owner, whose snapshot is authoritative
        restored = vars(self).get("_flood_guard_restore_task")
        if restored is None or not restored.done():
            return  # a pool that never replayed the stored state must not overwrite it
        try:
            payload = snapshot_flood_guards(**self._flood_guards())
            await db.set_setting(FLOOD_GUARD_SNAPSHOT_SETTING, json.dumps(payload, separators=(",", ":")))
        except Exception:
            logger.debug("Failed to persist the flood-guard snapshot", exc_info=True)

    async def restore_flood_guard_snapshot(self, db: Any) -> None:
        """Replay the persisted guards once, before the pool sends anything."""
        if vars(self).get("_relay") is not None:
            return
        try:
            value = await db.get_setting(FLOOD_GUARD_SNAPSHOT_SETTING)
        except Exception:
            return
        if not value:
            return
        try:
            payload = json.loads(value)
        except (TypeError, ValueError):
            logger.warning("flood-guard snapshot ignored: not JSON")
            return
        counts = restore_flood_guards(payload, **self._flood_guards())
        if any(counts.values()):
            logger.info(
                "flood-guard snapshot restored: %d category window(s), %d peer bucket(s), "
                "%d breaker entr(ies), ceiling=%s",
                counts["categories"], counts["peers"], counts["breaker"], bool(counts["ceiling"]),
            )
