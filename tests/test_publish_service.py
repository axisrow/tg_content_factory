import pytest

from src.models import ContentPipeline, GenerationRun, PipelinePublishMode, PipelineTarget
from src.services.publish_service import PublishService


class FakeDB:
    def __init__(self):
        self.repos = FakeRepos()


class FakeRepos:
    def __init__(self):
        self._pipelines = FakePipelinesRepo()
        self._generation_runs = FakeGenerationRunsRepo()

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
    def __init__(self):
        self.published_ids = []

    async def set_published_at(self, run_id):
        self.published_ids.append(run_id)


class FakeClientPool:
    def __init__(self, should_succeed=True):
        self._should_succeed = should_succeed
        self._clients = {}
        self.released = []

    async def get_client_by_phone(self, phone):
        if not self._should_succeed:
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

    run = GenerationRun(id=1, pipeline_id=1, generated_text=None, moderation_status="approved")

    results = await service.publish_run(run, make_pipeline())

    assert len(results) == 1
    assert results[0].success is False
    assert "No generated text" in results[0].error


@pytest.mark.anyio
async def test_publish_service_blocks_unapproved_run_for_moderated_pipeline():
    db = FakeDB()
    pool = FakeClientPool()
    service = PublishService(db, pool)

    run = GenerationRun(id=1, pipeline_id=1, generated_text="Test content", moderation_status="pending")

    results = await service.publish_run(run, make_pipeline())

    assert len(results) == 1
    assert results[0].success is False
    assert "not approved" in results[0].error


@pytest.mark.anyio
async def test_publish_service_no_targets():
    db = FakeDB()
    pool = FakeClientPool()
    service = PublishService(db, pool)

    run = GenerationRun(id=1, pipeline_id=1, generated_text="Test content", moderation_status="approved")

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
    )

    results = await service.publish_run(run, make_pipeline())

    assert len(results) == 1
    assert results[0].success is True
    assert results[0].message_id == 12345
    assert 1 in db.repos.generation_runs.published_ids
    assert pool.released == ["+1234567890"]


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

    run = GenerationRun(id=1, pipeline_id=1, generated_text="Test content", moderation_status="approved")

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
        async def get_client_by_phone(self, phone):
            raise asyncio.TimeoutError()

    pool = TimeoutPool(should_succeed=True)
    service = PublishService(db, pool)

    run = GenerationRun(
        id=1,
        pipeline_id=1,
        generated_text="Test content",
        moderation_status="approved",
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

    run = GenerationRun(id=None, pipeline_id=1, generated_text="text", moderation_status="approved")
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

    run = GenerationRun(id=1, pipeline_id=1, generated_text="text", moderation_status="approved")
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
        async def get_client_by_phone(self, phone):
            raise ValueError("unexpected error")

    pool = ExceptionPool(should_succeed=True)
    service = PublishService(db, pool)

    run = GenerationRun(
        id=1,
        pipeline_id=1,
        generated_text="Test content",
        moderation_status="approved",
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
    )

    results = await service.publish_run(run, make_pipeline(publish_mode=PipelinePublishMode.AUTO))
    # Should succeed via fallback resolve_input_entity
    assert results[0].success is True
