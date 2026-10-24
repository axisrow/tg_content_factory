"""Provenance of ``accounts.is_active`` (#1315): re-auth must not undo the operator."""

from __future__ import annotations

from tg_agent.models import Account


async def _account(accounts_repo, phone: str):
    accounts = await accounts_repo.get_accounts()
    return next(a for a in accounts if a.phone == phone)


async def test_reauth_keeps_operator_disabled_account_off(accounts_repo):
    pk = await accounts_repo.add_account(Account(phone="+100", session_string="old"))
    assert await accounts_repo.set_account_active(pk, False, origin="human", actor="cli") == 1

    # Re-auth: the same phone comes back with a fresh session and the model
    # default is_active=True, as every auth flow builds it.
    await accounts_repo.add_account(Account(phone="+100", session_string="fresh"))

    account = await _account(accounts_repo, "+100")
    assert account.session_string == "fresh"
    assert account.is_active is False
    assert account.active_origin == "human"


async def test_reauth_still_upserts_is_active_without_human_decision(accounts_repo):
    await accounts_repo.add_account(Account(phone="+101", session_string="s", is_active=False))
    await accounts_repo.add_account(Account(phone="+101", session_string="s2", is_active=True))

    account = await _account(accounts_repo, "+101")
    assert account.is_active is True
    assert account.active_origin == "auto"


async def test_auto_toggle_over_human_decision_is_suppressed_and_journaled(accounts_repo):
    pk = await accounts_repo.add_account(Account(phone="+102", session_string="s"))

    assert await accounts_repo.set_account_active(pk, False, origin="human", actor="cli") == 1
    assert await accounts_repo.set_account_active(pk, True, origin="auto", actor="agent") == 0

    account = await _account(accounts_repo, "+102")
    assert account.is_active is False
    assert account.active_origin == "human"
    history = await accounts_repo._database.repos.decisions.history("account", pk, field="is_active")
    assert [(d.origin, d.actor, d.new_value) for d in history] == [
        ("auto", "agent", "1"),
        ("human", "cli", "0"),
    ]


async def test_human_reenable_overrides_human_disable(accounts_repo):
    pk = await accounts_repo.add_account(Account(phone="+103", session_string="s"))

    assert await accounts_repo.set_account_active(pk, False, origin="human") == 1
    assert await accounts_repo.set_account_active(pk, True, origin="human") == 1

    account = await _account(accounts_repo, "+103")
    assert account.is_active is True
    assert account.is_primary is True


async def test_set_account_active_missing_id_is_noop(accounts_repo):
    assert await accounts_repo.set_account_active(999, False, origin="human") == 0
    assert await accounts_repo._database.repos.decisions.history("account", 999) == []
