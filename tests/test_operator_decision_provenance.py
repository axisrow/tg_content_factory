"""Automation must not override operator decisions (#1315), end to end on a real DB.

Covers the three harm paths of the issue through their real entry points:
agent moderation tools, re-auth + pool for accounts, scheduler startup
canonicalization for operator-disabled generation jobs.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

from tests.agent_tools_helpers import _get_tool_handlers, _text
from tg_agent.database.bundles import SchedulerBundle
from tg_agent.models import Account
from tg_agent.scheduler.service import SchedulerManager
from tg_agent.services.telegram_command_dispatcher import TelegramCommandDispatcher

# --- moderation: agent bulk approve vs a human rejection --------------------


async def test_agent_bulk_approve_cannot_resurrect_human_rejected_run(db):
    rejected = await db.repos.generation_runs.create_run(None, "a")
    pending = await db.repos.generation_runs.create_run(None, "b")
    await db.repos.generation_runs.set_moderation_status(rejected, "rejected", origin="human", actor="cli")

    handlers = _get_tool_handlers(db)
    result = await handlers["bulk_approve_runs"]({"run_ids": f"{rejected},{pending}", "confirm": True})

    text = _text(result)
    assert f"Одобрено 1 run(s): [{pending}]" in text
    assert f"пропущено 1: [{rejected}]" in text
    run = await db.repos.generation_runs.get(rejected)
    assert run is not None and run.moderation_status == "rejected"
    attempts = await db.repos.decisions.history("generation_run", rejected, field="moderation_status")
    assert (attempts[0].origin, attempts[0].actor, attempts[0].new_value) == ("auto", "agent", "approved")


async def test_agent_single_approve_is_suppressed_over_human_rejection(db):
    run_id = await db.repos.generation_runs.create_run(None, "a")
    await db.repos.generation_runs.set_moderation_status(run_id, "rejected", origin="human")

    handlers = _get_tool_handlers(db)
    result = await handlers["approve_run"]({"run_id": run_id})

    assert "подавлено" in _text(result)
    run = await db.repos.generation_runs.get(run_id)
    assert run is not None and run.moderation_status == "rejected"


async def test_agent_owner_claim_without_confirmation_is_journaled_not_trusted(db):
    run_id = await db.repos.generation_runs.create_run(None, "a")
    await db.repos.generation_runs.set_moderation_status(run_id, "rejected", origin="human")

    handlers = _get_tool_handlers(db)
    await handlers["approve_run"]({"run_id": run_id, "on_behalf_of_user": True})

    run = await db.repos.generation_runs.get(run_id)
    assert run is not None and run.moderation_status == "rejected"
    reasons = [d.reason for d in await db.repos.decisions.history("generation_run", run_id)]
    assert "agent requested action on behalf of owner without confirmation" in reasons


async def test_agent_confirmed_owner_request_may_revise_rejection(db):
    run_id = await db.repos.generation_runs.create_run(None, "a")
    await db.repos.generation_runs.set_moderation_status(run_id, "rejected", origin="human")

    handlers = _get_tool_handlers(db)
    await handlers["approve_run"]({"run_id": run_id, "on_behalf_of_user": True, "confirm": True})

    run = await db.repos.generation_runs.get(run_id)
    assert run is not None
    assert (run.moderation_status, run.moderation_origin) == ("approved", "human")


# --- accounts: re-auth through the worker command path ----------------------


async def _disabled_by_operator(db, phone: str) -> int:
    await db.add_account(Account(phone=phone, session_string="old-session", is_primary=True))
    account_id = next(a.id for a in await db.get_account_summaries() if a.phone == phone)
    assert account_id is not None
    await db.set_account_active(account_id, False, origin="human", actor="cli")
    return account_id


async def test_reauth_via_worker_keeps_disabled_account_out_of_pool(db):
    await _disabled_by_operator(db, "+7900")
    auth = MagicMock()
    auth.is_configured = True
    auth.verify_code = AsyncMock(return_value="fresh-session")
    pool = MagicMock()
    pool.add_client = AsyncMock()
    pool.get_client_by_phone = AsyncMock(return_value=None)
    dispatcher = TelegramCommandDispatcher(db, pool, auth=auth)

    result = await dispatcher._handle_auth_verify_code(
        {"phone": "+7900", "code": "12345", "phone_code_hash": "h"}
    )

    assert result["result"]["is_active"] is False
    pool.add_client.assert_not_awaited()
    account = next(a for a in await db.get_accounts() if a.phone == "+7900")
    assert account.session_string == "fresh-session"
    assert account.is_active is False
    # A pool restart loads active accounts only, so the account stays out.
    assert [a.phone for a in await db.get_live_usable_accounts(active_only=True)] == []


async def test_stale_session_override_does_not_lease_disabled_account(real_pool_harness_factory, db):
    harness = real_pool_harness_factory()
    await _disabled_by_operator(harness.db, "+7901")
    # An override left behind by an earlier add_client (auth/connect) must not
    # outrank the DB row that says the operator switched the account off.
    harness.pool._session_overrides["+7901"] = "old-session"

    assert await harness.pool._get_account_for_phone("+7901") is None
    assert await harness.pool._get_account_for_phone("+7901", active_only=False) is not None


# --- scheduler: operator-disabled generation on a fresh DB -------------------


class _Pipeline:
    def __init__(self, pid: int):
        self.id = pid
        self.is_active = True
        self.generate_interval_minutes = 10


class _PipelineBundle:
    def __init__(self, pipelines):
        self._pipelines = pipelines

    async def get_all(self, active_only: bool = False):
        return list(self._pipelines)


class _Scheduler:
    def __init__(self):
        self.added: list[str] = []

    def get_jobs(self):
        return []

    def add_job(self, func, trigger, id, replace_existing=False, args=None):  # noqa: A002
        self.added.append(id)

    def remove_job(self, job_id):
        pass


async def test_operator_disabled_generation_survives_restart_on_fresh_db(db):
    key = "scheduler_job_disabled:content_generate_7"
    # Fresh DB: no scheduler_pipeline_jobs_canonicalized flag yet. The operator
    # disables generation through a journaled toggle (CLI/web/agent path).
    assert await db.get_setting("scheduler_pipeline_jobs_canonicalized") is None
    await db.repos.settings.record_setting_decision(key, "1", origin="human", actor="cli")

    mgr = SchedulerManager()
    mgr._scheduler_bundle = SchedulerBundle.from_database(db)
    mgr._pipeline_bundle = _PipelineBundle([_Pipeline(7)])
    mgr._scheduler = _Scheduler()
    await mgr.sync_pipeline_jobs()

    assert await db.get_setting(key) == "1"
    assert "content_generate_7" not in mgr._scheduler.added


async def test_unjournaled_legacy_split_state_is_still_canonicalized(db):
    key = "scheduler_job_disabled:content_generate_8"
    await db.set_setting(key, "1")  # pre-#835/2 state: no journal entry

    mgr = SchedulerManager()
    mgr._scheduler_bundle = SchedulerBundle.from_database(db)
    mgr._pipeline_bundle = _PipelineBundle([_Pipeline(8)])
    mgr._scheduler = _Scheduler()
    await mgr.sync_pipeline_jobs()

    assert await db.get_setting(key) == "0"
    assert "content_generate_8" in mgr._scheduler.added
    assert await db.get_setting("scheduler_pipeline_jobs_canonicalized") == "1"


async def _drain_reconcile(db, dispatcher: TelegramCommandDispatcher) -> None:
    """Run the queued scheduler.reconcile command through the real worker loop step."""
    commands = await db.repos.telegram_commands.list_commands(command_type="scheduler.reconcile")
    pending = [c for c in commands if c.status.value == "pending"]
    assert len(pending) == 1
    await dispatcher._run_iteration()
    done = await db.repos.telegram_commands.get_command(pending[0].id)
    assert done.status.value == "succeeded", done.error


async def test_agent_toggle_with_live_manager_survives_reconcile_and_restart(db):
    """#1696 review: toggle -> journal/command -> reconcile -> restart -> toggle back,
    on a real DB, a real SchedulerManager and the real worker command handler."""
    key = "scheduler_job_disabled:collect_all"
    await db.set_setting("scheduler_autostart", "1")
    mgr = SchedulerManager(scheduler_bundle=SchedulerBundle.from_database(db))
    restarted: SchedulerManager | None = None
    await mgr.start()
    try:
        assert mgr._scheduler.get_job("collect_all") is not None
        handlers = _get_tool_handlers(db, scheduler_manager=mgr)
        dispatcher = TelegramCommandDispatcher(db, MagicMock(), scheduler=mgr)

        # Agent disables the job: persisted and journaled, applied by the worker.
        assert "выключена" in _text(await handlers["toggle_scheduler_job"]({"job_id": "collect_all"}))
        assert await db.get_setting(key) == "1"
        assert await db.repos.settings.has_setting_decision(key)
        await _drain_reconcile(db, dispatcher)
        assert mgr._scheduler.get_job("collect_all") is None

        # Worker restart: a fresh manager over the same DB keeps the job off.
        await mgr.stop()
        restarted = SchedulerManager(scheduler_bundle=SchedulerBundle.from_database(db))
        await restarted.start()
        assert restarted._scheduler.get_job("collect_all") is None

        # A second toggle reads the persisted key and turns the job back on.
        handlers = _get_tool_handlers(db, scheduler_manager=restarted)
        dispatcher = TelegramCommandDispatcher(db, MagicMock(), scheduler=restarted)
        assert "включена" in _text(await handlers["toggle_scheduler_job"]({"job_id": "collect_all"}))
        assert await db.get_setting(key) == "0"
        await _drain_reconcile(db, dispatcher)
        assert restarted._scheduler.get_job("collect_all") is not None
    finally:
        await mgr.stop()
        if restarted is not None:
            await restarted.stop()
