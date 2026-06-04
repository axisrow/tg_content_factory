import asyncio
from types import SimpleNamespace

from src.config import SchedulerConfig
from src.live_runtime_pause import LiveRuntimePauseGate
from src.scheduler.service import SchedulerManager
from src.services.telegram_command_dispatcher import TelegramCommandDispatcher


def test_live_runtime_pause_gate_waits_until_all_agent_requests_release():
    async def _run() -> None:
        gate = LiveRuntimePauseGate()

        async with gate.agent_request():
            assert gate.is_paused
            assert gate.active_agent_requests == 1

            waiter = asyncio.create_task(gate.wait_if_paused())
            await asyncio.sleep(0)
            assert not waiter.done()

            async with gate.agent_request():
                assert gate.is_paused
                assert gate.active_agent_requests == 2

            assert gate.is_paused
            assert gate.active_agent_requests == 1
            assert not waiter.done()

        assert not gate.is_paused
        assert await asyncio.wait_for(waiter, timeout=0.5)

    asyncio.run(_run())


def test_scheduler_waits_to_run_background_collection_while_agent_uses_live_runtime():
    class FakeEnqueuer:
        def __init__(self) -> None:
            self.calls = 0

        async def enqueue_all_channels(self):
            self.calls += 1
            return SimpleNamespace(queued_count=2, skipped_existing_count=3, total_candidates=5)

    async def _run() -> tuple[dict, dict, int]:
        gate = LiveRuntimePauseGate()
        enqueuer = FakeEnqueuer()
        manager = SchedulerManager(
            SchedulerConfig(),
            task_enqueuer=enqueuer,
            live_runtime_pause_gate=gate,
        )

        async with gate.agent_request():
            background_task = asyncio.create_task(manager._run_collection())
            await asyncio.sleep(0)
            assert enqueuer.calls == 0
            manual_result = await manager.trigger_now()
        background_result = await asyncio.wait_for(background_task, timeout=0.5)
        return background_result, manual_result, enqueuer.calls

    background_result, manual_result, calls = asyncio.run(_run())

    assert background_result == {"enqueued": 2, "skipped": 3, "total": 5, "errors": 0}
    assert manual_result == {"enqueued": 2, "skipped": 3, "total": 5, "errors": 0}
    assert calls == 2


def test_telegram_command_dispatcher_does_not_claim_while_agent_uses_live_runtime():
    class FakeTelegramCommands:
        def __init__(self) -> None:
            self.claims = 0

        async def claim_next_command(self):
            self.claims += 1
            return None

    class FakeDb:
        def __init__(self) -> None:
            self.repos = SimpleNamespace(telegram_commands=FakeTelegramCommands())

    async def _run() -> int:
        gate = LiveRuntimePauseGate()
        db = FakeDb()
        dispatcher = TelegramCommandDispatcher(
            db,
            pool=object(),
            live_runtime_pause_gate=gate,
        )
        async with gate.agent_request():
            dispatcher._stop_event.clear()
            task = asyncio.create_task(dispatcher._run_loop())
            await asyncio.sleep(0)
            assert db.repos.telegram_commands.claims == 0
            dispatcher._stop_event.set()
            await asyncio.wait_for(task, timeout=1.5)
        return db.repos.telegram_commands.claims

    assert asyncio.run(_run()) == 0
