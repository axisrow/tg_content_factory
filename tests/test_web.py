import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest


@pytest.mark.anyio
async def test_startup_continues_after_pool_initialize_timeout(tmp_path, db, caplog, monkeypatch):
    """start_container proceeds when pool.initialize() hangs past the timeout."""

    from tg_agent.runtime.bootstrap import start_container

    # Shrink timeout so test runs fast
    monkeypatch.setattr("tg_agent.runtime.bootstrap._POOL_INIT_TIMEOUT", 0.1)

    async def _hang():
        await asyncio.sleep(9999)

    pool = SimpleNamespace(initialize=_hang)
    auth = SimpleNamespace(is_configured=True)
    channel_bundle = SimpleNamespace(
        fail_running_collection_tasks_on_startup=AsyncMock(return_value=0),
    )
    photo_task_service = SimpleNamespace(recover_running=AsyncMock(return_value=0))
    ai_search = SimpleNamespace(initialize=lambda: None)
    scheduler = SimpleNamespace(load_settings=AsyncMock(), start=AsyncMock())
    gen_runs = SimpleNamespace(reset_running_on_startup=AsyncMock(return_value=0))
    dm_drafts = SimpleNamespace(reset_sending_on_startup=AsyncMock(return_value=0))
    tg_cmds = SimpleNamespace(reset_running_on_startup=AsyncMock(return_value=0))
    # start_container announces a non-empty approval queue (#1314); the stub
    # mirrors the real repos contract with an empty queue.
    channels = SimpleNamespace(list_pending_approval=AsyncMock(return_value=[]))

    container = SimpleNamespace(
        auth=auth,
        pool=pool,
        channel_bundle=channel_bundle,
        photo_task_service=photo_task_service,
        db=SimpleNamespace(
            repos=SimpleNamespace(
                generation_runs=gen_runs,
                dm_reply_drafts=dm_drafts,
                telegram_commands=tg_cmds,
                channels=channels,
            ),
            get_setting=AsyncMock(return_value=None),
        ),
        collection_queue=None,
        unified_dispatcher=None,
        ai_search=ai_search,
        agent_manager=None,
        scheduler=scheduler,
    )

    with caplog.at_level("WARNING"):
        await asyncio.wait_for(start_container(container), timeout=5)

    assert "telegram pool timed out" in caplog.text
