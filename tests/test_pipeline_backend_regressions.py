"""Backend contracts from the audit, using real persistence and fake external IO."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from tests.test_publish_service import FakeClientPool
from tg_agent.database import Database
from tg_agent.models import (
    Channel,
    ContentPipeline,
    Message,
    PipelineEdge,
    PipelineGraph,
    PipelineNode,
    PipelineNodeType,
    PipelineTarget,
)
from tg_agent.search.engine import SearchEngine
from tg_agent.services.content_generation_service import ContentGenerationService
from tg_agent.services.pipeline_nodes.base import NodeContext
from tg_agent.services.pipeline_nodes.handlers import DeleteMessageHandler, FilterHandler, ForwardHandler
from tg_agent.services.pipeline_templates_builtin import get_builtin_templates
from tg_agent.services.provider_service import RuntimeProviderRegistry
from tg_agent.services.publish_service import PublishService
from tg_agent.services.task_enqueuer import TaskEnqueuer
from tg_agent.services.telegram_actions import TelegramActionService

pytestmark = [pytest.mark.anyio, pytest.mark.aiosqlite_serial]


async def _persist_pipeline(db, pipeline, *, sources=(), targets=()):
    pipeline.id = await db.repos.content_pipelines.add(pipeline, list(sources), list(targets))
    return pipeline


@pytest.fixture
async def source_message(db):
    await db.add_channel(Channel(channel_id=100, title="Source"))
    message = Message(
        channel_id=100,
        message_id=42,
        text="Member joined",
        date=datetime.now(timezone.utc),
        message_kind="service",
        service_action_semantic="join",
    )
    assert await db.insert_message(message)
    return message


@pytest.mark.parametrize("dry_run", [True, False], ids=["dry-run", "live"])
async def test_generation_dry_run_does_not_delete_or_generate_images(db, source_message, monkeypatch, dry_run):
    graph = PipelineGraph(
        nodes=[
            PipelineNode(id="source", name="Source", type=PipelineNodeType.SOURCE, config={"channel_ids": [100]}),
            PipelineNode(id="fetch", name="Fetch", type=PipelineNodeType.FETCH_MESSAGES),
            PipelineNode(id="delete", name="Delete", type=PipelineNodeType.DELETE_MESSAGE),
            PipelineNode(
                id="image", name="Image", type=PipelineNodeType.IMAGE_GENERATE, config={"model": "test-image"}
            ),
        ],
        edges=[
            PipelineEdge(from_node=start, to_node=end)
            for start, end in [("source", "fetch"), ("fetch", "delete"), ("delete", "image")]
        ],
    )
    pipeline = await _persist_pipeline(
        db, ContentPipeline(name="Dry-run safety", account_phone="+111", pipeline_json=graph), sources=[100]
    )
    actions = AsyncMock(spec=TelegramActionService)
    monkeypatch.setattr(
        "tg_agent.services.pipeline_nodes.handlers.TelegramActionService",
        lambda pool, db=None: actions,
    )
    images = SimpleNamespace(generate=AsyncMock(return_value="https://image.invalid/generated.png"))
    service = ContentGenerationService(
        db,
        SearchEngine(db),
        client_pool=object(),
        image_service=images,
        provider_service=RuntimeProviderRegistry(env={}),
    )

    run = await service.generate(pipeline, dry_run=dry_run)

    assert run.status == "completed"
    assert bool(run.metadata.get("dry_run")) is dry_run
    assert (actions.delete_messages.await_count, images.generate.await_count) == ((0, 0) if dry_run else (1, 1))
    assert run.image_url == (None if dry_run else "https://image.invalid/generated.png")


@pytest.mark.parametrize("text", ["Please join our meeting at 10:00", "I left my keys at home"])
async def test_join_leave_cleanup_only_deletes_service_messages(text):
    normal = Message(
        channel_id=100,
        message_id=42,
        text=text,
        date=datetime.now(timezone.utc),
        message_kind="text",
        sender_kind="user",
        sender_id=123,
    )
    event = normal.model_copy(update={"message_id": 43, "message_kind": "service", "service_action_semantic": "join"})
    context = NodeContext()
    context.set_global("context_messages", [normal, event])
    actions = AsyncMock(spec=TelegramActionService)

    await FilterHandler().execute(
        {"type": "service_message", "service_types": ["user_joined", "user_left"]}, context, {}
    )
    await DeleteMessageHandler().execute(
        {}, context, {"client_pool": object(), "account_phone": "+111", "telegram_actions": actions}
    )

    assert [call.kwargs["message_ids"] for call in actions.delete_messages.await_args_list] == [[43]]


async def test_forward_retry_delivers_only_to_previously_failed_target(db):
    pipeline = await _persist_pipeline(db, ContentPipeline(name="Forward retry"))
    message = Message(channel_id=100, message_id=42, text="News", date=datetime.now(timezone.utc))
    delivered = []
    fail_second = True

    async def forward(**kwargs):
        if kwargs["to_chat"] == 222 and fail_second:
            raise RuntimeError("Temporary Telegram failure")
        delivered.append(kwargs["to_chat"])

    actions = AsyncMock(spec=TelegramActionService)
    actions.forward_messages.side_effect = forward
    services = {
        "db": db,
        "pipeline_id": pipeline.id,
        "_current_node_id": "forward",
        "client_pool": object(),
        "telegram_actions": actions,
    }
    config = {"targets": [{"phone": "+111", "dialog_id": 111}, {"phone": "+111", "dialog_id": 222}]}
    for attempt in range(2):
        context = NodeContext()
        context.set_global("context_messages", [message])
        await ForwardHandler().execute(config, context, services)
        if attempt == 0:
            assert delivered == [111]
            assert context.get_errors()
            fail_second = False

    # A third run inside the same fetch window: every target is logged per
    # target now, so nothing is re-delivered. This pins the UNIQUE key — if
    # the schema drops target from it, the second attempt's row for 222 is
    # IGNOREd as a duplicate of 111's row and this run forwards 222 again.
    third = NodeContext()
    third.set_global("context_messages", [message])
    await ForwardHandler().execute(config, third, services)
    assert third.get_errors() == []

    assert delivered == [111, 222]
    assert [call.kwargs["to_chat"] for call in actions.forward_messages.await_args_list] == [111, 222, 222]


@pytest.mark.parametrize("stale_snapshot", [True, False], ids=["stale-record", "fresh-record"])
async def test_publish_retry_reads_persisted_delivery_progress(db, stale_snapshot):
    pipeline = await _persist_pipeline(
        db,
        ContentPipeline(name="Publish retry"),
        targets=[
            PipelineTarget(pipeline_id=0, phone="+111", dialog_id=111),
            PipelineTarget(pipeline_id=0, phone="+222", dialog_id=222),
        ],
    )
    runs = db.repos.generation_runs
    run_id = await runs.create_run(pipeline.id, "Prompt")
    await runs.save_result(run_id, "Post")
    await runs.set_moderation_status(run_id, "approved")
    first = await runs.get(run_id)
    second = await runs.get(run_id)
    pool = FakeClientPool(fail_phones={"+222"})
    service = PublishService(db, pool)

    results = await service.publish_run(first, pipeline)
    assert [result.success for result in results] == [True, False]
    persisted = await runs.get(run_id)
    assert persisted.metadata["published_targets"] == ["+111:111"]
    assert persisted.published_at is None
    pool._fail_phones.clear()

    results = await service.publish_run(second if stale_snapshot else persisted, pipeline)

    assert all(result.success for result in results)
    assert [len(pool._clients[phone].sent_messages) for phone in ["+111", "+222"]] == [1, 1]
    published = await runs.get(run_id)
    assert published.moderation_status == "published"
    assert published.published_at is not None


@pytest.fixture
async def queue_databases(tmp_path):
    databases = [Database(str(tmp_path / "queue.db"), session_encryption_secret="test-key") for _ in range(2)]
    try:
        for database in databases:
            await database.initialize()
        yield databases
    finally:
        for database in databases:
            await database.close()


async def test_concurrent_pipeline_enqueue_creates_one_active_task(queue_databases):
    pipeline = await _persist_pipeline(queue_databases[0], ContentPipeline(name="Queue dedup"))
    enqueuers = [TaskEnqueuer(database, None) for database in queue_databases]
    barrier = asyncio.Barrier(12)

    async def enqueue(index):
        await barrier.wait()
        return await enqueuers[index % 2].enqueue_pipeline_run(pipeline.id)

    await asyncio.gather(*(enqueue(index) for index in range(12)))

    tasks = await queue_databases[0].repos.tasks.get_collection_tasks(limit=100)
    assert len(tasks) == 1, f"Concurrent requests created task IDs {[task.id for task in tasks]}"
    assert tasks[0].payload.pipeline_id == pipeline.id
    assert await enqueuers[0].enqueue_pipeline_run(pipeline.id) is None


async def test_builtin_forward_template_forwards_stored_messages(db, source_message, monkeypatch):
    template = next(template for template in get_builtin_templates() if template.name == "Пересылка сообщений")
    graph = template.template_json.model_copy(deep=True)
    for node in graph.nodes:
        if node.type == PipelineNodeType.SOURCE:
            node.config["channel_ids"] = [100]
        elif node.type == PipelineNodeType.FORWARD:
            node.config["targets"] = [{"phone": "+111", "dialog_id": 111}]
    pipeline = await _persist_pipeline(
        db, ContentPipeline(name="Builtin forwarding", pipeline_json=graph), sources=[100]
    )
    actions = AsyncMock(spec=TelegramActionService)
    monkeypatch.setattr(
        "tg_agent.services.pipeline_nodes.handlers.TelegramActionService",
        lambda pool, db=None: actions,
    )
    service = ContentGenerationService(
        db, SearchEngine(db), client_pool=object(), provider_service=RuntimeProviderRegistry(env={})
    )
    assert len(await db.repos.messages.get_recent_for_channels([100], 24)) == 1

    run = await service.generate(pipeline)

    assert run.status == "completed"
    sent_ids = [call.kwargs["message_ids"] for call in actions.forward_messages.await_args_list]
    assert sent_ids == [[source_message.message_id]]
    assert run.result_count == 1
