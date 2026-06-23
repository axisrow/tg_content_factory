"""Characterization guard for the ClientPool decomposition (#1046).

`ClientPool` (src/telegram/client_pool.py) is a tier-1 hot zone: flood-wait
rotation, primary-race (#733), StringSession auth, pool↔DB race ordering
(#449). The decomposition splits its responsibilities into composition mixins
WITHOUT changing behaviour, so this module pins the public contract and the
invariants that the split must preserve:

* the full public method surface stays callable on ``ClientPool`` (web shim,
  CLI runtime, agent tools, collector all rely on duck-typed access);
* the per-instance private attributes the test harnesses poke at
  (``_in_use``, ``_active_leases``, ``_lease_pool``, ``_backend_router``,
  ``_session_overrides`` …) live on a single instance — mixins must not move
  them onto sub-objects;
* the flood-rotation / lifecycle / auth behaviours observed end-to-end through
  the real pool harness are unchanged.

These tests are GREEN on the monolith and MUST stay GREEN after the split.
"""

from __future__ import annotations

import inspect

from src.telegram.client_pool import ClientPool
from src.telegram.resolve_guard import ResolveGuardMixin
from tests.helpers import FakeCliTelethonClient

# The frozen public surface of ClientPool. Every name here must remain a
# callable attribute on the class after the decomposition. Losing one would
# silently break a consumer (web shim, collector, agent tools, CLI runtime).
PUBLIC_CONTRACT: frozenset[str] = frozenset(
    {
        # channel ↔ phone routing map
        "is_dialogs_fetched",
        "mark_dialogs_fetched",
        "connected_phones",
        "get_phone_for_channel",
        "register_channel_phone",
        "clear_channel_phone",
        "remember_channel_phone",
        "forget_channel_phone",
        # warm / dialog cache
        "is_warming",
        "wait_for_warm",
        "warm_all_dialogs",
        "invalidate_dialogs_cache",
        "resolve_entity_with_warm",
        "resolve_dialog_entity",
        "get_dialogs_for_phone",
        "get_dialogs",
        "get_forum_topics",
        "leave_channels",
        # lifecycle / acquisition
        "initialize",
        "add_client",
        "remove_client",
        "disconnect_all",
        "reconnect_phone",
        "force_reconnect_phone",
        "release_client",
        "get_available_client",
        "get_client_by_phone",
        "get_native_client_by_phone",
        "get_users_info",
        "get_mtproto_watchdog_stats",
        # flood-wait rotation
        "report_flood",
        "report_premium_flood",
        "clear_flood",
        "clear_premium_flood",
        "get_premium_client",
        "get_premium_unavailability_reason",
        "get_stats_availability",
        "get_premium_stats_availability",
        "available_stats_client_count",
        "available_collection_client_count",
        "has_rotatable_resolve_phone",
        "next_resolve_capable_at",
        # resolve / meta
        "resolve_channel",
        "resolve_any_entity",
        "fetch_channel_meta",
    }
)

# Per-instance attributes the harnesses / unit tests construct or assert on.
# A mixin split keeps a single `self`, so these must remain instance attributes
# of a fully constructed ClientPool — never relocated onto a delegate object.
REQUIRED_INSTANCE_ATTRS: frozenset[str] = frozenset(
    {
        "clients",
        "_in_use",
        "_active_leases",
        "_lease_pool",
        "_backend_router",
        "_session_overrides",
        "_premium_flood_wait_until",
        "_materializer",
        "_mtproto_watchdog",
        "_lock",
        "_channel_phone_map",
    }
)


def test_public_contract_is_fully_present_and_callable():
    """No public method is lost or turned non-callable by the split."""
    for name in PUBLIC_CONTRACT:
        attr = getattr(ClientPool, name, None)
        assert attr is not None, f"ClientPool lost public method {name!r}"
        assert callable(attr), f"ClientPool.{name} is no longer callable"


def test_public_contract_matches_actual_public_surface():
    """The frozen contract and the live public surface stay in sync.

    Catches BOTH directions: a removed method (contract has it, class doesn't)
    and a new public method added without registering it here (so the guard
    keeps covering the real surface). Dunder and private (_-prefixed) names are
    excluded — only the user-facing API is contractual. Names inherited from the
    pre-existing ``ResolveGuardMixin`` are out of scope for THIS decomposition,
    so they are excluded from the "unregistered" direction.
    """
    actual_public = {
        name
        for name, member in inspect.getmembers(ClientPool)
        if not name.startswith("_") and callable(member)
    }
    resolve_guard_public = {
        name for name in dir(ResolveGuardMixin) if not name.startswith("_")
    }
    missing = PUBLIC_CONTRACT - actual_public
    assert not missing, f"contract names not present on ClientPool: {sorted(missing)}"
    unregistered = actual_public - PUBLIC_CONTRACT - resolve_guard_public
    assert not unregistered, (
        "new public ClientPool methods are not in PUBLIC_CONTRACT — register "
        f"them so the decomposition guard covers them: {sorted(unregistered)}"
    )


async def test_required_private_attrs_live_on_the_instance(real_pool_harness_factory):
    """Harness-poked private state stays on a single ClientPool instance.

    The lifecycle/unit tests build pools via ``ClientPool.__new__`` and assert
    on ``_in_use`` / ``_active_leases`` etc., or read them after acquisition.
    Mixins share one ``self``; a regression that moved this state onto a
    sub-object would break those tests — pin it here too.
    """
    harness = real_pool_harness_factory()
    pool = harness.pool
    for attr in REQUIRED_INSTANCE_ATTRS:
        assert hasattr(pool, attr), f"ClientPool instance is missing {attr!r}"


async def test_flood_rotation_skips_flooded_phone_end_to_end(real_pool_harness_factory):
    """get_available_client() skips a phone in flood-wait and returns the next.

    The CLAUDE.md "Flood wait rotation" invariant: a phone whose
    flood_wait_until is in the future is skipped, and an unflooded peer is
    handed out instead. Exercised through the real pool so the split of the
    rotation logic into its own mixin cannot change the observable outcome.
    """
    harness = real_pool_harness_factory()
    harness.queue_cli_client(phone="+70000000001", client=FakeCliTelethonClient())
    harness.queue_cli_client(phone="+70000000002", client=FakeCliTelethonClient())
    await harness.add_account("+70000000001", session_string="session-a", is_primary=True)
    await harness.add_account("+70000000002", session_string="session-b")
    await harness.initialize_connected_accounts()

    # Flood the first account far into the future.
    await harness.pool.report_flood("+70000000001", wait_seconds=600)

    result = await harness.pool.get_available_client()
    assert result is not None
    _, phone = result
    assert phone == "+70000000002", "rotation handed out the flooded phone"
    await harness.pool.release_client(phone)


async def test_report_and_clear_flood_round_trip(real_pool_harness_factory):
    """report_flood persists a deadline; clear_flood removes it (DB-backed)."""
    harness = real_pool_harness_factory()
    harness.queue_cli_client(phone="+70000000001", client=FakeCliTelethonClient())
    await harness.add_account("+70000000001", session_string="session-a", is_primary=True)
    await harness.initialize_connected_accounts()

    async def _flood_until() -> object | None:
        accounts = await harness.db.get_accounts()
        account = next((a for a in accounts if a.phone == "+70000000001"), None)
        assert account is not None
        return account.flood_wait_until

    await harness.pool.report_flood("+70000000001", wait_seconds=600)
    assert await _flood_until() is not None

    await harness.pool.clear_flood("+70000000001")
    assert await _flood_until() is None


async def test_lifecycle_add_then_remove_client(real_pool_harness_factory):
    """add_client connects a session; remove_client tears it down cleanly."""
    harness = real_pool_harness_factory()
    harness.queue_cli_client(phone="+70000000009", client=FakeCliTelethonClient())
    await harness.add_account("+70000000009", session_string="seed-session")

    await harness.pool.add_client("+70000000009", "live-session-string")
    assert "+70000000009" in harness.pool.clients
    assert harness.pool._session_overrides.get("+70000000009") == "live-session-string"

    await harness.pool.remove_client("+70000000009")
    assert "+70000000009" not in harness.pool.clients
    assert "+70000000009" not in harness.pool._session_overrides


async def test_primary_account_sorts_first_in_users_info(real_pool_harness_factory):
    """get_users_info() reports the primary account first (primary-selection).

    Pins the primary ordering tied to the partial-unique-index work (#733): the
    primary flag drives ordering, so the split of lifecycle/primary logic must
    not perturb it.
    """
    harness = real_pool_harness_factory()
    harness.queue_cli_client(phone="+70000000001", client=FakeCliTelethonClient())
    harness.queue_cli_client(phone="+70000000002", client=FakeCliTelethonClient())
    await harness.add_account("+70000000002", session_string="session-b")
    await harness.add_account("+70000000001", session_string="session-a", is_primary=True)
    await harness.initialize_connected_accounts()

    infos = await harness.pool.get_users_info(include_avatar=False)
    assert infos, "expected at least one connected account"
    assert infos[0].is_primary is True
    assert infos[0].phone == "+70000000001"
