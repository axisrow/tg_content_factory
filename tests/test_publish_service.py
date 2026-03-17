import pytest

from src.models import ContentPipeline, GenerationRun, PipelinePublishMode, PipelineGenerationBackend
from src.services.publish_service import PublishService, PublishResult


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

    async def get_client_by_phone(self, phone):
        if not self._should_succeed:
            return None
        return (FakeClient(), phone)


class FakeClient:
    @property
    def raw_client(self):
        return FakeRawClient()


class FakeRawClient:
    async def send_message(self, entity, text, file=None):
        return FakeMessage()


class FakeMessage:
    id = 12345


@pytest.mark.asyncio
async def test_publish_service_no_text():
    db = FakeDB()
    pool = FakeClientPool()
    service = PublishService(db, pool)

    run = GenerationRun(
        id=1,
        pipeline_id=1,
        generated_text=None,
        moderation_status="approved",
    )
    pipeline = ContentPipeline(
        id=1,
        name="Test",
        prompt_template="test",
    )

    results = await service.publish_run(run, pipeline)

    assert len(results) == 1
    assert results[0].success is False
    assert "No generated text" in results[0].error


@pytest.mark.asyncio
async def test_publish_service_no_targets():
    db = FakeDB()
    pool = FakeClientPool()
    service = PublishService(db, pool)

    run = GenerationRun(
        id=1,
        pipeline_id=1,
        generated_text="Test content",
        moderation_status="approved",
    )
    pipeline = ContentPipeline(
        id=1,
        name="Test",
        prompt_template="test",
    )

    results = await service.publish_run(run, pipeline)

    assert len(results) == 1
    assert results[0].success is False
    assert "No targets" in results[0].error


@pytest.mark.asyncio
async def test_publish_service_success():
    from src.models import PipelineTarget

    db = FakeDB()
    db.repos.content_pipelines.set_targets([
        PipelineTarget(id=1, pipeline_id=1, phone="+1234567890", dialog_id=-1001234567890)
    ])
    pool = FakeClientPool(should_succeed=True)
    service = PublishService(db, pool)

    run = GenerationRun(
        id=1,
        pipeline_id=1,
        generated_text="Test content for publishing",
        moderation_status="approved",
    )
    pipeline = ContentPipeline(
        id=1,
        name="Test",
        prompt_template="test",
    )

    results = await service.publish_run(run, pipeline)

    assert len(results) == 1
    assert results[0].success is True
    assert results[0].message_id == 12345
    assert 1 in db.repos.generation_runs.published_ids


@pytest.mark.asyncio
async def test_publish_service_client_unavailable():
    from src.models import PipelineTarget

    db = FakeDB()
    db.repos.content_pipelines.set_targets([
        PipelineTarget(id=1, pipeline_id=1, phone="+1234567890", dialog_id=-1001234567890)
    ])
    pool = FakeClientPool(should_succeed=False)
    service = PublishService(db, pool)

    run = GenerationRun(
        id=1,
        pipeline_id=1,
        generated_text="Test content",
        moderation_status="approved",
    )
    pipeline = ContentPipeline(
        id=1,
        name="Test",
        prompt_template="test",
    )

    results = await service.publish_run(run, pipeline)

    assert len(results) == 1
    assert results[0].success is False
    assert "No client" in results[0].error
