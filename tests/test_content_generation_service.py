from datetime import datetime, timezone

from src.models import ContentPipeline, PipelineGenerationBackend, PipelinePublishMode
from src.services.content_generation_service import ContentGenerationService
from src.models import Message, SearchResult
from src.services.generation_service import GenerationService


class DummySearchEngine:
    def __init__(self, messages):
        self._messages = messages

    async def search_hybrid(self, query: str, **kwargs) -> SearchResult:
        return SearchResult(messages=self._messages, total=len(self._messages), query=query)


async def fake_provider(**kwargs):
    return "GENERATED: " + (kwargs.get("prompt") or "")[:40]


class FakeGenerationRunsRepo:
    def __init__(self):
        self._runs = {}
        self._next_id = 1

    async def create_run(self, pipeline_id, prompt):
        from src.models import GenerationRun
        run_id = self._next_id
        self._next_id += 1
        run = GenerationRun(
            id=run_id,
            pipeline_id=pipeline_id,
            prompt=prompt,
            status="pending",
            generated_text=None,
            metadata=None,
            moderation_status="pending",
        )
        self._runs[run_id] = run
        return run_id

    async def set_status(self, run_id, status):
        if run_id in self._runs:
            self._runs[run_id].status = status

    async def save_result(self, run_id, generated_text, metadata=None):
        if run_id in self._runs:
            self._runs[run_id].generated_text = generated_text
            self._runs[run_id].metadata = metadata

    async def get(self, run_id):
        return self._runs.get(run_id)


class FakeRepos:
    def __init__(self):
        self.generation_runs = FakeGenerationRunsRepo()


class FakeDB:
    def __init__(self):
        self.repos = FakeRepos()


async def test_content_generation_service_rag():
    msg = Message(
        id=1,
        channel_id=10,
        message_id=42,
        sender_id=None,
        sender_name="Alice",
        text="Hello world from test",
        date=datetime.now(timezone.utc),
        collected_at=None,
        channel_title="TestChannel",
        channel_username="testchan",
    )

    engine = DummySearchEngine([msg])
    db = FakeDB()

    service = ContentGenerationService(db, engine)

    pipeline = ContentPipeline(
        id=1,
        name="Test Pipeline",
        prompt_template="Use {source_messages}",
        llm_model="test-model",
        generation_backend=PipelineGenerationBackend.CHAIN,
        publish_mode=PipelinePublishMode.MODERATED,
    )

    from src.services import provider_service
    original_get = getattr(provider_service.AgentProviderService, "get_provider_callable", None)
    provider_service.AgentProviderService.get_provider_callable = lambda self, model: fake_provider
    try:
        run = await service.generate(pipeline)
        assert run is not None
        assert "GENERATED:" in (run.generated_text or "")
    finally:
        if original_get:
            provider_service.AgentProviderService.get_provider_callable = original_get


async def test_content_generation_service_deep_agents_stub():
    msg = Message(
        id=1,
        channel_id=10,
        message_id=42,
        sender_id=None,
        sender_name="Alice",
        text="Hello world from test",
        date=datetime.now(timezone.utc),
        collected_at=None,
        channel_title="TestChannel",
        channel_username="testchan",
    )

    engine = DummySearchEngine([msg])
    db = FakeDB()

    service = ContentGenerationService(db, engine)

    pipeline = ContentPipeline(
        id=1,
        name="Test Pipeline",
        prompt_template="Use {source_messages}",
        llm_model="test-model",
        generation_backend=PipelineGenerationBackend.DEEP_AGENTS,
        publish_mode=PipelinePublishMode.MODERATED,
    )

    from src.services import provider_service
    original_get = getattr(provider_service.AgentProviderService, "get_provider_callable", None)
    provider_service.AgentProviderService.get_provider_callable = lambda self, model: fake_provider
    try:
        try:
            await service.generate(pipeline)
            assert False, "Expected RuntimeError"
        except RuntimeError as e:
            assert "AgentManager" in str(e) or "not configured" in str(e)
    finally:
        if original_get:
            provider_service.AgentProviderService.get_provider_callable = original_get
