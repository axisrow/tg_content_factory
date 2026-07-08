import pytest

from src.models import ContentPipeline, GenerationRun, PipelinePublishMode, PipelineTarget
from src.services.publish_service import PublishService


class FakeDB:
    def __init__(self, fail_set_metadata_after=None):
        self.repos = FakeRepos(fail_set_metadata_after=fail_set_metadata_after)


class FakeRepos:
    def __init__(self, fail_set_metadata_after=None):
        self._pipelines = FakePipelinesRepo()
        self._generation_runs = FakeGenerationRunsRepo(
            fail_set_metadata_after=fail_set_metadata_after
        )

    @property
    def content_pipelines(self):
        return self._pipelines

    @property
    def generation_runs(self):
        return self._generation_runs


class FakePipelinesRepo:
    def __init__(self):
        self._targets = []

    def set_targets(self, targets):
        self._targets = targets

    async def list_targets(self, pipeline_id):
        return self._targets


class FakeGenerationRunsRepo:
    def __init__(self, fail_set_metadata_after=None):
        self.published_ids = []
        self.metadata_by_id = {}
        # When set, set_metadata raises after this many successful writes,
        # simulating a DB failure mid-publish (issue #1116). The metadata of the
        # last *successful* write stays persisted — exactly what the next retry
        # would read back.
        self._fail_after = fail_set_metadata_after
        self.set_metadata_calls = 0

    async def set_published_at(self, run_id):
        self.published_ids.append(run_id)

    async def set_metadata(self, run_id, metadata):
        self.set_metadata_calls += 1
        if self._fail_after is not None and self.set_metadata_calls > self._fail_after:
            raise RuntimeError("simulated DB failure during publish progress write")
        # Store a copy: the run's metadata is mutated in place across attempts,
        # so without copying every persisted snapshot would alias the same dict.
        self.metadata_by_id[run_id] = dict(metadata)


class FakeClientPool:
    def __init__(self, should_succeed=True, fail_phones=None):
        self._should_succeed = should_succeed
        self._fail_phones = set(fail_phones or [])
        self._clients = {}
        self.released = []

    async def get_client_by_phone(self, phone, *, wait_for_flood=False):
        if not self._should_succeed or phone in self._fail_phones:
            return None
        client = self._clients.setdefault(phone, FakeClient())
        return (client, phone)

    async def release_client(self, phone):
        self.released.append(phone)

    async def resolve_dialog_entity(self, session, phone, dialog_id, dialog_type):
        return {"phone": phone, "dialog_id": dialog_id, "dialog_type": dialog_type}


class FakeClient:
    def __init__(self):
        self.sent_files = []
        self.sent_messages = []

    @property
    def raw_client(self):
        return FakeRawClient()

    async def get_entity(self, peer):
        return peer

    async def get_input_entity(self, peer):
        return peer

    async def send_file(self, entity, files, caption=None, schedule=None):
        self.sent_files.append(
            {
                "entity": entity,
                "files": files,
                "caption": caption,
                "schedule": schedule,
            }
        )
        return FakeMessage()

    async def send_message(self, entity, text, **kwargs):
        self.sent_messages.append(
            {
                "entity": entity,
                "text": text,
                "kwargs": kwargs,
            }
        )
        return FakeMessage()


class FakeRawClient:
    async def send_message(self, entity, text, file=None):
        return FakeMessage()


class FakeMessage:
    id = 12345


def make_pipeline(**overrides):
    defaults = {
        "id": 1,
        "name": "Test",
        "prompt_template": "test",
        "publish_mode": PipelinePublishMode.MODERATED,
    }
    defaults.update(overrides)
    return ContentPipeline(**defaults)


@pytest.mark.anyio
async def test_publish_service_no_text():
    db = FakeDB()
    pool = FakeClientPool()
    service = PublishService(db, pool)

    run = GenerationRun(
        id=1, pipeline_id=1, generated_text=None,
        moderation_status="approved", status="completed",
    )

    results = await service.publish_run(run, make_pipeline())

    assert len(results) == 1
    assert results[0].success is False
    assert "No generated text" in results[0].error


@pytest.mark.anyio
async def test_publish_service_blocks_unapproved_run_for_moderated_pipeline():
    db = FakeDB()
    pool = FakeClientPool()
    service = PublishService(db, pool)

    run = GenerationRun(
        id=1, pipeline_id=1, generated_text="Test content",
        moderation_status="pending", status="completed",
    )

    results = await service.publish_run(run, make_pipeline())

    assert len(results) == 1
    assert results[0].success is False
    assert "not approved" in results[0].error


@pytest.mark.anyio
async def test_publish_service_blocks_non_completed_run():
    """A run whose generation did not complete must never be delivered, even if
    it carries generated_text and moderation_status='approved' (issue #1036
    review, Codex). This is the service-level guard that protects EVERY publish
    entrypoint — web/CLI moderation, the dispatcher, and the agent tool — not
    just CONTENT_PUBLISH's SQL filter. No target is ever contacted."""
    db = FakeDB()
    db.repos.content_pipelines.set_targets(
        [PipelineTarget(id=1, pipeline_id=1, phone="+1234567890", dialog_id=-1001234567890)]
    )
    pool = FakeClientPool(should_succeed=True)
    service = PublishService(db, pool)

    # A failed generation that nonetheless got generated_text saved (text was
    # persisted before a later step failed) and was somehow left 'approved'.
    run = GenerationRun(
        id=1,
        pipeline_id=1,
        generated_text="Leaked content from a failed run",
        moderation_status="approved",
        status="failed",
    )

    results = await service.publish_run(run, make_pipeline(publish_mode=PipelinePublishMode.AUTO))

    assert len(results) == 1
    assert results[0].success is False
    assert "not completed" in results[0].error
    # The irreversible send was never attempted, and the run was not marked published.
    assert pool.released == []
    assert 1 not in db.repos.generation_runs.published_ids


@pytest.mark.anyio
async def test_publish_service_no_targets():
    db = FakeDB()
    pool = FakeClientPool()
    service = PublishService(db, pool)

    run = GenerationRun(
        id=1, pipeline_id=1, generated_text="Test content",
        moderation_status="approved", status="completed",
    )

    results = await service.publish_run(run, make_pipeline())

    assert len(results) == 1
    assert results[0].success is False
    assert "No targets" in results[0].error


@pytest.mark.anyio
async def test_publish_service_success():
    db = FakeDB()
    db.repos.content_pipelines.set_targets(
        [PipelineTarget(id=1, pipeline_id=1, phone="+1234567890", dialog_id=-1001234567890)]
    )
    pool = FakeClientPool(should_succeed=True)
    service = PublishService(db, pool)

    run = GenerationRun(
        id=1,
        pipeline_id=1,
        generated_text="Test content for publishing",
        moderation_status="approved",
        status="completed",
    )

    results = await service.publish_run(run, make_pipeline())

    assert len(results) == 1
    assert results[0].success is True
    assert results[0].message_id == 12345
    assert 1 in db.repos.generation_runs.published_ids
    assert pool.released == ["+1234567890"]


@pytest.mark.anyio
async def test_publish_service_partial_failure_records_delivered():
    """Partial multi-target failure records delivered targets and does NOT mark published (issue #633)."""
    db = FakeDB()
    db.repos.content_pipelines.set_targets(
        [
            PipelineTarget(id=1, pipeline_id=1, phone="+1111111111", dialog_id=-1001),
            PipelineTarget(id=2, pipeline_id=1, phone="+2222222222", dialog_id=-1002),
        ]
    )
    # Second target has no available client → fails.
    pool = FakeClientPool(fail_phones={"+2222222222"})
    service = PublishService(db, pool)

    run = GenerationRun(
        id=1,
        pipeline_id=1,
        generated_text="Test content",
        moderation_status="approved",
        status="completed",
    )

    results = await service.publish_run(run, make_pipeline())

    assert [r.success for r in results] == [True, False]
    # Not fully published → run stays eligible for retry.
    assert 1 not in db.repos.generation_runs.published_ids
    # The delivered target is persisted so a retry can skip it.
    assert db.repos.generation_runs.metadata_by_id[1]["published_targets"] == ["+1111111111:-1001"]


@pytest.mark.anyio
async def test_publish_service_retry_skips_delivered_targets():
    """A retry skips already-delivered targets (no duplicate) and completes the run (issue #633)."""
    db = FakeDB()
    db.repos.content_pipelines.set_targets(
        [
            PipelineTarget(id=1, pipeline_id=1, phone="+1111111111", dialog_id=-1001),
            PipelineTarget(id=2, pipeline_id=1, phone="+2222222222", dialog_id=-1002),
        ]
    )
    # On retry every client is available again.
    pool = FakeClientPool()
    service = PublishService(db, pool)

    # Simulate a prior attempt that already delivered to the first target.
    run = GenerationRun(
        id=1,
        pipeline_id=1,
        generated_text="Test content",
        moderation_status="approved",
        status="completed",
        metadata={"published_targets": ["+1111111111:-1001"]},
    )

    results = await service.publish_run(run, make_pipeline())

    assert [r.success for r in results] == [True, True]
    # First target was skipped — never acquired a client, so no duplicate send.
    assert "+1111111111" not in pool._clients
    # Second target was actually sent.
    assert "+2222222222" in pool._clients
    assert len(pool._clients["+2222222222"].sent_messages) == 1
    # All targets delivered → run is now marked published.
    assert 1 in db.repos.generation_runs.published_ids
    assert db.repos.generation_runs.metadata_by_id[1]["published_targets"] == [
        "+1111111111:-1001",
        "+2222222222:-1002",
    ]


@pytest.mark.anyio
async def test_publish_service_all_targets_already_delivered():
    """All targets already delivered: no new sends, set_metadata NOT called, run marked published (issue #633)."""
    db = FakeDB()
    db.repos.content_pipelines.set_targets(
        [
            PipelineTarget(id=1, pipeline_id=1, phone="+1111111111", dialog_id=-1001),
            PipelineTarget(id=2, pipeline_id=1, phone="+2222222222", dialog_id=-1002),
        ]
    )
    pool = FakeClientPool()
    service = PublishService(db, pool)

    # A prior attempt already delivered to every target.
    run = GenerationRun(
        id=1,
        pipeline_id=1,
        generated_text="Test content",
        moderation_status="approved",
        status="completed",
        metadata={"published_targets": ["+1111111111:-1001", "+2222222222:-1002"]},
    )

    results = await service.publish_run(run, make_pipeline())

    # Every target was skipped — reported success, no client acquired, no send.
    assert [r.success for r in results] == [True, True]
    assert pool._clients == {}
    assert pool.released == []
    # newly_delivered is empty → set_metadata must NOT be called (no clobber, no redundant write).
    assert 1 not in db.repos.generation_runs.metadata_by_id
    # All results succeeded → idempotent re-publish still closes the run.
    assert 1 in db.repos.generation_runs.published_ids


@pytest.mark.anyio
async def test_publish_service_allows_auto_pipeline_without_approval():
    db = FakeDB()
    db.repos.content_pipelines.set_targets(
        [PipelineTarget(id=1, pipeline_id=1, phone="+1234567890", dialog_id=-1001234567890)]
    )
    pool = FakeClientPool(should_succeed=True)
    service = PublishService(db, pool)

    run = GenerationRun(
        id=1,
        pipeline_id=1,
        generated_text="Auto-publish content",
        moderation_status="pending",
        status="completed",
    )

    results = await service.publish_run(
        run,
        make_pipeline(publish_mode=PipelinePublishMode.AUTO),
    )

    assert len(results) == 1
    assert results[0].success is True
    assert pool.released == ["+1234567890"]


@pytest.mark.anyio
async def test_publish_service_client_unavailable():
    db = FakeDB()
    db.repos.content_pipelines.set_targets(
        [PipelineTarget(id=1, pipeline_id=1, phone="+1234567890", dialog_id=-1001234567890)]
    )
    pool = FakeClientPool(should_succeed=False)
    service = PublishService(db, pool)

    run = GenerationRun(
        id=1, pipeline_id=1, generated_text="Test content",
        moderation_status="approved", status="completed",
    )

    results = await service.publish_run(run, make_pipeline())

    assert len(results) == 1
    assert results[0].success is False
    assert "No client" in results[0].error
    assert pool.released == []


# === Additional tests for image, timeout, edge cases ===


@pytest.mark.anyio
async def test_publish_service_with_image_url():
    """Publishes via send_file when run has image_url."""
    db = FakeDB()
    db.repos.content_pipelines.set_targets(
        [PipelineTarget(id=1, pipeline_id=1, phone="+1234567890", dialog_id=-1001234567890)]
    )
    pool = FakeClientPool(should_succeed=True)
    service = PublishService(db, pool)

    run = GenerationRun(
        id=1,
        pipeline_id=1,
        generated_text="Content with image",
        image_url="https://example.com/image.jpg",
        moderation_status="approved",
        status="completed",
    )

    results = await service.publish_run(run, make_pipeline())

    assert len(results) == 1
    assert results[0].success is True
    client_result = await pool.get_client_by_phone("+1234567890")
    assert client_result is not None
    client, _phone = client_result
    assert client.sent_files == [
        {
            "entity": {"phone": "+1234567890", "dialog_id": -1001234567890, "dialog_type": None},
            "files": run.image_url,
            "caption": run.generated_text,
            "schedule": None,
        }
    ]
    assert client.sent_messages == []


@pytest.mark.anyio
async def test_publish_service_whitespace_text():
    """Empty/whitespace generated_text is skipped."""
    db = FakeDB()
    db.repos.content_pipelines.set_targets(
        [PipelineTarget(id=1, pipeline_id=1, phone="+1234567890", dialog_id=-1001234567890)]
    )
    pool = FakeClientPool(should_succeed=True)
    service = PublishService(db, pool)

    run = GenerationRun(
        id=1,
        pipeline_id=1,
        generated_text="   \n\t  ",  # Whitespace only
        moderation_status="approved",
        status="completed",
    )

    results = await service.publish_run(run, make_pipeline())

    assert len(results) == 1
    assert results[0].success is False
    assert "No generated text" in results[0].error


@pytest.mark.anyio
async def test_publish_service_entity_resolution_fail():
    """Entity resolution failure produces error result."""
    db = FakeDB()
    db.repos.content_pipelines.set_targets(
        [PipelineTarget(id=1, pipeline_id=1, phone="+1234567890", dialog_id=-1001234567890)]
    )

    class FailingEntityPool(FakeClientPool):
        async def resolve_dialog_entity(self, session, phone, dialog_id, dialog_type):
            return None  # Entity resolution fails

    pool = FailingEntityPool(should_succeed=True)
    service = PublishService(db, pool)

    run = GenerationRun(
        id=1,
        pipeline_id=1,
        generated_text="Test content",
        moderation_status="approved",
        status="completed",
    )

    results = await service.publish_run(run, make_pipeline())

    assert len(results) == 1
    assert results[0].success is False
    assert "Could not resolve" in results[0].error


@pytest.mark.anyio
async def test_publish_service_timeout():
    """asyncio.TimeoutError produces 'Timeout' error."""
    db = FakeDB()
    db.repos.content_pipelines.set_targets(
        [PipelineTarget(id=1, pipeline_id=1, phone="+1234567890", dialog_id=-1001234567890)]
    )

    import asyncio

    class TimeoutPool(FakeClientPool):
        async def get_client_by_phone(self, phone, *, wait_for_flood=False):
            raise asyncio.TimeoutError()

    pool = TimeoutPool(should_succeed=True)
    service = PublishService(db, pool)

    run = GenerationRun(
        id=1,
        pipeline_id=1,
        generated_text="Test content",
        moderation_status="approved",
        status="completed",
    )

    results = await service.publish_run(run, make_pipeline())

    assert len(results) == 1
    assert results[0].success is False
    assert "Timeout" in results[0].error


@pytest.mark.anyio
async def test_publish_service_missing_run_id():
    """Missing run id returns early error."""
    db = FakeDB()
    pool = FakeClientPool()
    service = PublishService(db, pool)

    run = GenerationRun(id=None, pipeline_id=1, generated_text="text", moderation_status="approved", status="completed")
    results = await service.publish_run(run, make_pipeline())

    assert len(results) == 1
    assert results[0].success is False
    assert "Missing" in results[0].error


@pytest.mark.anyio
async def test_publish_service_missing_pipeline_id():
    """Missing pipeline id returns early error."""
    db = FakeDB()
    pool = FakeClientPool()
    service = PublishService(db, pool)

    run = GenerationRun(id=1, pipeline_id=1, generated_text="text", moderation_status="approved", status="completed")
    results = await service.publish_run(run, make_pipeline(id=None))

    assert len(results) == 1
    assert results[0].success is False
    assert "Missing" in results[0].error


@pytest.mark.anyio
async def test_publish_service_with_reply_to():
    """Sends message with reply_to when metadata has publish_reply."""
    db = FakeDB()
    db.repos.content_pipelines.set_targets(
        [PipelineTarget(id=1, pipeline_id=1, phone="+1234567890", dialog_id=-1001234567890)]
    )
    pool = FakeClientPool(should_succeed=True)
    service = PublishService(db, pool)

    run = GenerationRun(
        id=1,
        pipeline_id=1,
        generated_text="Reply content",
        moderation_status="approved",
        status="completed",
        metadata={"publish_reply": True, "reply_to_message_id": 42},
    )

    results = await service.publish_run(run, make_pipeline(publish_mode=PipelinePublishMode.AUTO))

    assert len(results) == 1
    assert results[0].success is True


@pytest.mark.anyio
async def test_publish_service_general_exception():
    """General exception during publishing produces error result."""
    db = FakeDB()
    db.repos.content_pipelines.set_targets(
        [PipelineTarget(id=1, pipeline_id=1, phone="+1234567890", dialog_id=-1001234567890)]
    )

    class ExceptionPool(FakeClientPool):
        async def get_client_by_phone(self, phone, *, wait_for_flood=False):
            raise ValueError("unexpected error")

    pool = ExceptionPool(should_succeed=True)
    service = PublishService(db, pool)

    run = GenerationRun(
        id=1,
        pipeline_id=1,
        generated_text="Test content",
        moderation_status="approved",
        status="completed",
    )

    results = await service.publish_run(run, make_pipeline())

    assert len(results) == 1
    assert results[0].success is False
    assert "unexpected error" in results[0].error


@pytest.mark.anyio
async def test_publish_service_preview_targets():
    """preview_targets returns target info dicts."""
    db = FakeDB()
    db.repos.content_pipelines.set_targets(
        [
            PipelineTarget(
                id=1, pipeline_id=1, phone="+1234567890",
                dialog_id=-1001234567890, title="Test Channel", dialog_type="channel",
            )
        ]
    )
    pool = FakeClientPool()
    service = PublishService(db, pool)

    preview = await service.preview_targets(1)

    assert len(preview) == 1
    assert preview[0]["phone"] == "+1234567890"
    assert preview[0]["title"] == "Test Channel"
    assert preview[0]["type"] == "channel"


@pytest.mark.anyio
async def test_publish_service_resolve_entity_fallback():
    """_resolve_entity falls back to resolve_input_entity when pool has no resolver."""
    db = FakeDB()
    db.repos.content_pipelines.set_targets(
        [PipelineTarget(id=1, pipeline_id=1, phone="+1234567890", dialog_id=-1001234567890)]
    )

    class NoResolverPool(FakeClientPool):
        # Remove resolve_dialog_entity so fallback path is taken
        resolve_dialog_entity = None

    pool = NoResolverPool(should_succeed=True)
    service = PublishService(db, pool)

    run = GenerationRun(
        id=1, pipeline_id=1, generated_text="Test",
        moderation_status="approved",
        status="completed",
    )

    results = await service.publish_run(run, make_pipeline(publish_mode=PipelinePublishMode.AUTO))
    # Should succeed via fallback resolve_input_entity
    assert results[0].success is True


@pytest.mark.anyio
async def test_publish_service_resolve_entity_no_wait_for():
    """_resolve_entity must await resolve_dialog_entity directly without
    asyncio.wait_for to avoid orphaned background Telethon requests on
    timeout.  Issue #795."""

    import asyncio
    from unittest.mock import patch

    db = FakeDB()
    db.repos.content_pipelines.set_targets(
        [PipelineTarget(id=1, pipeline_id=1, phone="+1234567890", dialog_id=-1001234567890)]
    )

    resolved_entity = {"phone": "+1234567890", "dialog_id": -1001234567890, "dialog_type": None}

    class DirectAwaitPool(FakeClientPool):
        async def resolve_dialog_entity(self, session, phone, dialog_id, dialog_type):
            return resolved_entity

    pool = DirectAwaitPool(should_succeed=True)
    service = PublishService(db, pool)

    with patch("asyncio.wait_for", wraps=asyncio.wait_for) as mock_wait_for:
        run = GenerationRun(
            id=1, pipeline_id=1, generated_text="Test",
            moderation_status="approved",
            status="completed",
        )
        results = await service.publish_run(run, make_pipeline(publish_mode=PipelinePublishMode.AUTO))

    assert results[0].success is True
    # wait_for must NOT be called — resolve_dialog_entity is awaited directly
    for call in mock_wait_for.call_args_list:
        # The only allowed wait_for calls are for send_message/publish_files in _publish_to_target
        pos_args = call[0]
        if pos_args:
            coro_name = getattr(pos_args[0], "__name__", "") or ""
            assert "resolve" not in str(coro_name).lower(), (
                f"asyncio.wait_for called on resolve operation: {call}"
            )


# === issue #1116: per-target progress must be persisted incrementally so a DB
# failure after a partial delivery cannot lose already-delivered targets and
# cause a duplicate send on retry. ===


@pytest.mark.anyio
async def test_publish_service_persists_progress_after_each_delivery():
    """Per-target delivery is persisted after EACH successful send, not once at
    the end (issue #1116).

    The single end-of-loop set_metadata is the data-loss hole: if that one write
    fails after 2 of 3 targets were already delivered, the run goes FAILED with
    NO published_targets recorded, so the retry re-sends to the 2 already-
    delivered targets → duplicate. Writing progress incrementally bounds the loss
    to at most the single in-flight target.
    """
    db = FakeDB()
    db.repos.content_pipelines.set_targets(
        [
            PipelineTarget(id=1, pipeline_id=1, phone="+1111111111", dialog_id=-1001),
            PipelineTarget(id=2, pipeline_id=1, phone="+2222222222", dialog_id=-1002),
            PipelineTarget(id=3, pipeline_id=1, phone="+3333333333", dialog_id=-1003),
        ]
    )
    pool = FakeClientPool()
    service = PublishService(db, pool)

    run = GenerationRun(
        id=1,
        pipeline_id=1,
        generated_text="Test content",
        moderation_status="approved",
        status="completed",
    )

    results = await service.publish_run(run, make_pipeline())

    assert [r.success for r in results] == [True, True, True]
    # One write per delivered target — progress is incremental, not a single
    # end-of-loop write whose failure would lose every delivered target.
    assert db.repos.generation_runs.set_metadata_calls == 3
    assert db.repos.generation_runs.metadata_by_id[1]["published_targets"] == [
        "+1111111111:-1001",
        "+2222222222:-1002",
        "+3333333333:-1003",
    ]
    assert 1 in db.repos.generation_runs.published_ids


@pytest.mark.anyio
async def test_publish_service_db_failure_on_second_write_bounds_loss_to_one_target():
    """The irreducible 1-target floor (issue #1116): the DB write that fails is
    the one right after the 2nd delivery, so only the 1st target is on record.

    This is the worst case the fix accepts — there is no transaction spanning the
    Telegram send and the DB write, so the one in-flight target whose write did
    not land (here the 2nd) is legitimately re-sent on retry. What the fix
    guarantees is that the 1st target, persisted by its own earlier write, is NOT
    re-sent. The old batched-write code recorded *nothing* and would have
    duplicated both.
    """
    targets = [
        PipelineTarget(id=1, pipeline_id=1, phone="+1111111111", dialog_id=-1001),
        PipelineTarget(id=2, pipeline_id=1, phone="+2222222222", dialog_id=-1002),
        PipelineTarget(id=3, pipeline_id=1, phone="+3333333333", dialog_id=-1003),
    ]

    # Attempt 1: the 1st write lands, the 2nd write (after the 2nd delivery) fails.
    db = FakeDB(fail_set_metadata_after=1)
    db.repos.content_pipelines.set_targets(targets)
    pool1 = FakeClientPool()
    service1 = PublishService(db, pool1)

    run = GenerationRun(
        id=1,
        pipeline_id=1,
        generated_text="Test content",
        moderation_status="approved",
        status="completed",
    )

    with pytest.raises(RuntimeError, match="simulated DB failure"):
        await service1.publish_run(run, make_pipeline())

    # Two targets were physically sent on attempt 1, but only the 1st was persisted
    # (its write landed before the failing 2nd write).
    assert "+1111111111" in pool1._clients
    assert "+2222222222" in pool1._clients
    persisted = db.repos.generation_runs.metadata_by_id[1]["published_targets"]
    assert persisted == ["+1111111111:-1001"]

    # Attempt 2 (retry): a fresh run row built from the persisted metadata, like
    # the dispatcher would reload it. No DB failure this time.
    db.repos.generation_runs._fail_after = None
    pool2 = FakeClientPool()
    service2 = PublishService(db, pool2)
    retry_run = GenerationRun(
        id=1,
        pipeline_id=1,
        generated_text="Test content",
        moderation_status="approved",
        status="completed",
        metadata=dict(db.repos.generation_runs.metadata_by_id[1]),
    )

    retry_results = await service2.publish_run(retry_run, make_pipeline())

    assert all(r.success for r in retry_results)
    # The persisted 1st target was NOT re-sent → no duplicate (issue #1116).
    assert "+1111111111" not in pool2._clients, (
        "already-delivered target was re-sent on retry — duplicate (issue #1116)"
    )
    # The 2nd target's write never landed, so it IS re-sent — the accepted floor.
    assert "+2222222222" in pool2._clients
    # The run is fully delivered and closed after the retry.
    assert 1 in db.repos.generation_runs.published_ids
    assert db.repos.generation_runs.metadata_by_id[1]["published_targets"] == [
        "+1111111111:-1001",
        "+2222222222:-1002",
        "+3333333333:-1003",
    ]


# === issue #1239: a send that outruns the timeout may already have reached
# Telegram. The timeout MUST stay (clients run with connection_retries=None, so
# a send on a dead connection would otherwise hang forever and freeze the
# sequential publish dispatcher), but a timed-out send is UNCONFIRMED, not
# known-failed: it is recorded in metadata.unconfirmed_targets and a retry must
# NOT re-send it blindly (would duplicate) — it surfaces it for a manual check.
#
# The fake send below actually BLOCKS past the (patched-tiny) timeout so the
# real asyncio.wait_for fires — this reproduces the true timeout-vs-delivery
# race, not a `sleep(0)` stand-in. ===


class _HangingSendClient(FakeClient):
    """A client whose send blocks longer than the (patched) send timeout, so the
    real asyncio.wait_for around the send actually fires — modelling a send that
    is in flight to Telegram when the local wait is cancelled. It still records
    the send (the request left the process) to mirror a possibly-delivered post.
    """

    async def send_message(self, entity, text, **kwargs):
        import asyncio

        await super().send_message(entity, text, **kwargs)  # record the attempt
        await asyncio.sleep(1.0)  # outlast the patched tiny timeout → wait_for fires
        return FakeMessage()

    async def send_file(self, entity, files, caption=None, schedule=None):
        import asyncio

        await super().send_file(entity, files, caption=caption, schedule=schedule)
        await asyncio.sleep(1.0)
        return FakeMessage()


class _HangingSendPool(FakeClientPool):
    async def get_client_by_phone(self, phone, *, wait_for_flood=False):
        if not self._should_succeed or phone in self._fail_phones:
            return None
        client = self._clients.setdefault(phone, _HangingSendClient())
        return (client, phone)


@pytest.mark.anyio
async def test_publish_service_send_timeout_marks_unconfirmed_not_failed():
    """A send that outruns SEND_TIMEOUT_SEC returns uncertain=True and is recorded
    in unconfirmed_targets, NOT published_targets and NOT a plain failure (#1239).

    The timeout fires for real (the fake send blocks past it), proving the guard
    against a forever-hung send is intact — no vertical freeze of the dispatcher.
    """
    from unittest.mock import patch

    import src.services.publish_service as ps

    db = FakeDB()
    db.repos.content_pipelines.set_targets(
        [PipelineTarget(id=1, pipeline_id=1, phone="+1234567890", dialog_id=-1001234567890)]
    )
    pool = _HangingSendPool(should_succeed=True)
    service = PublishService(db, pool)

    run = GenerationRun(
        id=1,
        pipeline_id=1,
        generated_text="Test content",
        moderation_status="approved",
        status="completed",
    )

    # Tiny timeout so the blocking send trips it fast; the whole test still ends
    # (the guard works — the send does NOT hang forever).
    with patch.object(ps, "SEND_TIMEOUT_SEC", 0.02):
        results = await service.publish_run(run, make_pipeline())

    assert results[0].success is False
    assert results[0].uncertain is True
    assert "unconfirmed" in results[0].error.lower()
    # Recorded as unconfirmed, NOT delivered → run is not marked published.
    md = db.repos.generation_runs.metadata_by_id[1]
    assert md["unconfirmed_targets"] == ["+1234567890:-1001234567890"]
    assert md.get("published_targets", []) == []
    assert 1 not in db.repos.generation_runs.published_ids


@pytest.mark.anyio
async def test_publish_service_unconfirmed_target_not_resent_on_retry():
    """The core #1239 fix: a target left UNCONFIRMED by a timed-out send is NOT
    re-sent on retry — no duplicate post — and is surfaced for a manual check.

    This is the exact production scenario: attempt 1 times out mid-send (post may
    already be live), the run stays retry-eligible, the operator hits publish
    again; the retry must not blindly re-send that target.
    """
    from unittest.mock import patch

    import src.services.publish_service as ps

    db = FakeDB()
    db.repos.content_pipelines.set_targets(
        [PipelineTarget(id=1, pipeline_id=1, phone="+1234567890", dialog_id=-1001234567890)]
    )
    pool = _HangingSendPool(should_succeed=True)
    service = PublishService(db, pool)

    run = GenerationRun(
        id=1,
        pipeline_id=1,
        generated_text="Test content",
        moderation_status="approved",
        status="completed",
    )

    # Attempt 1: the send times out → target recorded unconfirmed, one send tried.
    with patch.object(ps, "SEND_TIMEOUT_SEC", 0.02):
        await service.publish_run(run, make_pipeline())
    assert len(pool._clients["+1234567890"].sent_messages) == 1
    assert db.repos.generation_runs.metadata_by_id[1]["unconfirmed_targets"] == [
        "+1234567890:-1001234567890"
    ]

    # Attempt 2 (retry): reload from persisted metadata like the dispatcher does.
    # No new timeout patch needed — the target must be skipped WITHOUT sending.
    retry_pool = _HangingSendPool(should_succeed=True)
    retry_service = PublishService(db, retry_pool)
    retry_run = GenerationRun(
        id=1,
        pipeline_id=1,
        generated_text="Test content",
        moderation_status="approved",
        status="completed",
        metadata=dict(db.repos.generation_runs.metadata_by_id[1]),
    )
    retry_results = await retry_service.publish_run(retry_run, make_pipeline())

    # NOT re-sent — no client was even acquired for the unconfirmed target → no
    # duplicate. It is surfaced as an unconfirmed failure for a manual check.
    assert retry_pool._clients == {}
    assert retry_results[0].success is False
    assert retry_results[0].uncertain is True
    assert "manual check" in retry_results[0].error.lower()
    # Still not marked published — a human must confirm/re-drive it.
    assert 1 not in db.repos.generation_runs.published_ids


@pytest.mark.anyio
async def test_publish_service_image_send_timeout_marks_unconfirmed():
    """The image branch (publish_files) gets the same unconfirmed handling as the
    text branch — both irreversible sends are covered (#1239, Codex review)."""
    from unittest.mock import patch

    import src.services.publish_service as ps

    db = FakeDB()
    db.repos.content_pipelines.set_targets(
        [PipelineTarget(id=1, pipeline_id=1, phone="+1234567890", dialog_id=-1001234567890)]
    )
    pool = _HangingSendPool(should_succeed=True)
    service = PublishService(db, pool)

    run = GenerationRun(
        id=1,
        pipeline_id=1,
        generated_text="Content with image",
        image_url="https://example.com/image.jpg",
        moderation_status="approved",
        status="completed",
    )

    with patch.object(ps, "SEND_TIMEOUT_SEC", 0.02):
        results = await service.publish_run(run, make_pipeline())

    assert results[0].success is False
    assert results[0].uncertain is True
    # The image send (send_file) was attempted, then recorded unconfirmed.
    assert len(pool._clients["+1234567890"].sent_files) == 1
    assert db.repos.generation_runs.metadata_by_id[1]["unconfirmed_targets"] == [
        "+1234567890:-1001234567890"
    ]
    assert 1 not in db.repos.generation_runs.published_ids


@pytest.mark.anyio
async def test_publish_service_timeout_before_send_is_plain_retryable_failure():
    """A timeout BEFORE the send (client acquisition / flood wait) is a plain,
    retry-eligible failure — NOT an unconfirmed delivery. Nothing was dispatched,
    so the target must NOT be poisoned into unconfirmed_targets (#1239)."""
    import asyncio

    class ClientAcquireTimeoutPool(FakeClientPool):
        async def get_client_by_phone(self, phone, *, wait_for_flood=False):
            raise asyncio.TimeoutError()

    db = FakeDB()
    db.repos.content_pipelines.set_targets(
        [PipelineTarget(id=1, pipeline_id=1, phone="+1234567890", dialog_id=-1001234567890)]
    )
    pool = ClientAcquireTimeoutPool(should_succeed=True)
    service = PublishService(db, pool)

    run = GenerationRun(
        id=1,
        pipeline_id=1,
        generated_text="Test content",
        moderation_status="approved",
        status="completed",
    )

    results = await service.publish_run(run, make_pipeline())

    assert results[0].success is False
    assert results[0].uncertain is False
    assert results[0].error == "Timeout"
    # No send happened → nothing recorded as unconfirmed; the run can safely retry.
    assert 1 not in db.repos.generation_runs.metadata_by_id
    assert 1 not in db.repos.generation_runs.published_ids


@pytest.mark.anyio
async def test_publish_service_db_failure_after_two_deliveries_no_duplicate_on_retry():
    """The exact #1116 headline: 3 targets, delivered to 2, the DB failure strikes
    on the NEXT write (after the 3rd delivery) → run FAILED → retry must NOT
    re-send to EITHER of the 2 already-delivered targets.

    Both successful deliveries (1 and 2) have their own writes landed before the
    failure, so both are on record and skipped on retry. This is the headline
    duplicate scenario from the issue — distinct from the 1-target-floor case
    above, where the failure lands on one of the two deliveries' own writes.
    """
    targets = [
        PipelineTarget(id=1, pipeline_id=1, phone="+1111111111", dialog_id=-1001),
        PipelineTarget(id=2, pipeline_id=1, phone="+2222222222", dialog_id=-1002),
        PipelineTarget(id=3, pipeline_id=1, phone="+3333333333", dialog_id=-1003),
    ]

    # Attempt 1: the first TWO writes land (targets 1 and 2 persisted), the THIRD
    # write — after the 3rd delivery — fails.
    db = FakeDB(fail_set_metadata_after=2)
    db.repos.content_pipelines.set_targets(targets)
    pool1 = FakeClientPool()
    service1 = PublishService(db, pool1)

    run = GenerationRun(
        id=1,
        pipeline_id=1,
        generated_text="Test content",
        moderation_status="approved",
        status="completed",
    )

    with pytest.raises(RuntimeError, match="simulated DB failure"):
        await service1.publish_run(run, make_pipeline())

    # All three were physically sent, but only the first two writes landed.
    assert {"+1111111111", "+2222222222", "+3333333333"} <= set(pool1._clients)
    assert db.repos.generation_runs.metadata_by_id[1]["published_targets"] == [
        "+1111111111:-1001",
        "+2222222222:-1002",
    ]

    # Attempt 2 (retry): reload from persisted metadata, no DB failure this time.
    db.repos.generation_runs._fail_after = None
    pool2 = FakeClientPool()
    service2 = PublishService(db, pool2)
    retry_run = GenerationRun(
        id=1,
        pipeline_id=1,
        generated_text="Test content",
        moderation_status="approved",
        status="completed",
        metadata=dict(db.repos.generation_runs.metadata_by_id[1]),
    )

    retry_results = await service2.publish_run(retry_run, make_pipeline())

    assert all(r.success for r in retry_results)
    # NEITHER already-delivered target is re-sent — the headline #1116 duplicate
    # is fully prevented for every target whose progress write landed.
    assert "+1111111111" not in pool2._clients
    assert "+2222222222" not in pool2._clients
    # Only the one target whose write never landed (the 3rd) is re-sent.
    assert "+3333333333" in pool2._clients
    assert 1 in db.repos.generation_runs.published_ids
    assert db.repos.generation_runs.metadata_by_id[1]["published_targets"] == [
        "+1111111111:-1001",
        "+2222222222:-1002",
        "+3333333333:-1003",
    ]
