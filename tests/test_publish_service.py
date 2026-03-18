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
        self.released = []

    async def get_client_by_phone(self, phone):
        if not self._should_succeed:
            return None
        return (FakeClient(), phone)

    async def release_client(self, phone):
        self.released.append(phone)

    async def resolve_dialog_entity(self, session, phone, dialog_id, dialog_type):
        return {"phone": phone, "dialog_id": dialog_id, "dialog_type": dialog_type}


class FakeClient:
    @property
    def raw_client(self):
        return FakeRawClient()

    async def get_entity(self, peer):
        return peer

    async def get_input_entity(self, peer):
        return peer

    async def send_file(self, entity, files, caption=None, schedule=None):
        return FakeMessage()

    async def send_message(self, entity, text, **kwargs):
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


@pytest.mark.asyncio
async def test_publish_service_no_text():
    db = FakeDB()
    pool = FakeClientPool()
    service = PublishService(db, pool)

    run = GenerationRun(id=1, pipeline_id=1, generated_text=None, moderation_status="approved")

    results = await service.publish_run(run, make_pipeline())

    assert len(results) == 1
    assert results[0].success is False
    assert "No generated text" in results[0].error


@pytest.mark.asyncio
async def test_publish_service_blocks_unapproved_run_for_moderated_pipeline():
    db = FakeDB()
    pool = FakeClientPool()
    service = PublishService(db, pool)

    run = GenerationRun(id=1, pipeline_id=1, generated_text="Test content", moderation_status="pending")

    results = await service.publish_run(run, make_pipeline())

    assert len(results) == 1
    assert results[0].success is False
    assert "not approved" in results[0].error


@pytest.mark.asyncio
async def test_publish_service_no_targets():
    db = FakeDB()
    pool = FakeClientPool()
    service = PublishService(db, pool)

    run = GenerationRun(id=1, pipeline_id=1, generated_text="Test content", moderation_status="approved")

    results = await service.publish_run(run, make_pipeline())

    assert len(results) == 1
    assert results[0].success is False
    assert "No targets" in results[0].error


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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
