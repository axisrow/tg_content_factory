"""Tests for ContentGenerationService edge cases and collaborator interactions."""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.models import (
    ContentPipeline,
    GenerationRun,
    Message,
    PipelineGenerationBackend,
    PipelineGraph,
    PipelineNode,
    PipelineNodeType,
    PipelinePublishMode,
    SearchResult,
)
from src.services.content_generation_service import ContentGenerationService

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_msg(text="Hello world"):
    return Message(
        id=1,
        channel_id=10,
        message_id=42,
        sender_id=None,
        sender_name="Alice",
        text=text,
        date=datetime.now(timezone.utc),
        collected_at=None,
        channel_title="TestChannel",
        channel_username="testchan",
    )


class DummySearchEngine:
    def __init__(self, messages=None):
        self._messages = messages or []

    async def search_hybrid(self, query, **kwargs):
        return SearchResult(messages=self._messages, total=len(self._messages), query=query)


class FakeGenerationRunsRepo:
    def __init__(self):
        self._runs = {}
        self._next_id = 1

    async def create_run(self, pipeline_id, prompt):
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

    async def set_status(self, run_id, status, metadata=None):
        if run_id in self._runs:
            self._runs[run_id].status = status

    async def save_result(self, run_id, generated_text, metadata=None):
        if run_id in self._runs:
            self._runs[run_id].generated_text = generated_text
            self._runs[run_id].metadata = metadata

    async def get(self, run_id):
        return self._runs.get(run_id)

    async def set_quality_score(self, run_id, score, issues=None):
        if run_id in self._runs:
            self._runs[run_id].quality_score = score
            self._runs[run_id].quality_issues = issues


class FakeRepos:
    def __init__(self):
        self.generation_runs = FakeGenerationRunsRepo()


class FakeDB:
    def __init__(self):
        self.repos = FakeRepos()
        self._settings = {}

    async def execute(self, sql, params=()):
        return None

    async def get_setting(self, key):
        return self._settings.get(key)


class FakeDraftNotificationService:
    def __init__(self):
        self.calls = []

    async def notify_new_draft(self, run, pipeline):
        self.calls.append((run, pipeline))
        return True


class FakeQualityService:
    def __init__(self, overall=0.81, issues=None):
        self.overall = overall
        self.issues = issues or ["weak hook"]
        self.calls = []

    async def score_content(self, text, model=None):
        from src.services.quality_scoring_service import QualityScore

        self.calls.append((text, model))
        return QualityScore(
            relevance=self.overall,
            language_quality=self.overall,
            informativeness=self.overall,
            overall=self.overall,
            issues=self.issues,
        )


class FakeImageService:
    def __init__(self, url="https://img.example/gen.png"):
        self.url = url
        self.calls = []

    async def generate(self, model, text):
        self.calls.append((model, text))
        return self.url


def _provider(text="GENERATED OUTPUT"):
    async def fn(prompt=None, **kwargs):
        return text
    return fn


def _patch_provider(provider_fn):
    """Patch ProviderConfigService.get_provider_callable to return provider_fn."""
    from src.services import provider_service

    original = getattr(provider_service.RuntimeProviderRegistry, "get_provider_callable", None)

    def replacement(self, model):
        return provider_fn

    provider_service.RuntimeProviderRegistry.get_provider_callable = replacement
    return original


def _restore_provider(original):
    from src.services import provider_service

    if original:
        provider_service.RuntimeProviderRegistry.get_provider_callable = original
    else:
        del provider_service.RuntimeProviderRegistry.get_provider_callable


# ---------------------------------------------------------------------------
# Tests: set_status failure -> run marked failed
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_generate_set_status_failure_marks_run_failed():
    """If set_status('running') raises, run should be marked 'failed'."""
    engine = DummySearchEngine([_make_msg()])
    db = FakeDB()

    original_set_status = db.repos.generation_runs.set_status
    call_count = [0]

    async def flaky_set_status(run_id, status):
        call_count[0] += 1
        if call_count[0] == 1:
            raise RuntimeError("DB locked")
        await original_set_status(run_id, status)

    db.repos.generation_runs.set_status = flaky_set_status

    service = ContentGenerationService(db, engine)
    pipeline = ContentPipeline(
        id=1,
        name="Test",
        prompt_template="prompt",
        llm_model="m",
        generation_backend=PipelineGenerationBackend.CHAIN,
        publish_mode=PipelinePublishMode.MODERATED,
    )

    orig = _patch_provider(_provider())
    try:
        with pytest.raises(RuntimeError, match="DB locked"):
            await service.generate(pipeline)
    finally:
        _restore_provider(orig)


# ---------------------------------------------------------------------------
# Tests: publish_reply metadata + reply_to_message_id
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_generate_publish_reply_and_reply_to_id():
    """publish_reply and reply_to_message_id should be stored in metadata."""
    engine = DummySearchEngine([_make_msg()])
    db = FakeDB()
    service = ContentGenerationService(db, engine)

    pipeline = ContentPipeline(
        id=1,
        name="Test",
        prompt_template="prompt",
        llm_model="m",
        generation_backend=PipelineGenerationBackend.CHAIN,
        publish_mode=PipelinePublishMode.MODERATED,
        pipeline_json=PipelineGraph(nodes=[], edges=[]),
    )

    orig = _patch_provider(_provider())
    from src.services import pipeline_executor

    original_execute = getattr(pipeline_executor.PipelineExecutor, "execute", None)
    pipeline_executor.PipelineExecutor.execute = AsyncMock(
        return_value={
            "generated_text": "text",
            "publish_reply": True,
            "reply_to_message_id": 42,
        }
    )
    try:
        run = await service.generate(pipeline)
        assert run.metadata["publish_reply"] is True
        assert run.metadata["reply_to_message_id"] == 42
    finally:
        _restore_provider(orig)
        if original_execute:
            pipeline_executor.PipelineExecutor.execute = original_execute


# ---------------------------------------------------------------------------
# Tests: image_url from graph executor
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_generate_image_url_from_graph_executor():
    """When graph executor returns image_url, it should be saved to the run."""
    engine = DummySearchEngine([_make_msg()])

    class TrackingDB(FakeDB):
        def __init__(self):
            super().__init__()
            self.executed = []

        async def execute(self, sql, params=()):
            self.executed.append((sql, params))
            return None

    db = TrackingDB()
    service = ContentGenerationService(db, engine)

    pipeline = ContentPipeline(
        id=1,
        name="Test",
        prompt_template="prompt",
        llm_model="m",
        generation_backend=PipelineGenerationBackend.CHAIN,
        publish_mode=PipelinePublishMode.MODERATED,
        pipeline_json=PipelineGraph(nodes=[], edges=[]),
    )

    orig = _patch_provider(_provider())
    from src.services import pipeline_executor

    original_execute = getattr(pipeline_executor.PipelineExecutor, "execute", None)
    pipeline_executor.PipelineExecutor.execute = AsyncMock(
        return_value={
            "generated_text": "text",
            "image_url": "https://img.example/graph.png",
        }
    )
    try:
        run = await service.generate(pipeline)
        assert run is not None
        # Verify the UPDATE for image_url was issued
        img_updates = [(s, p) for s, p in db.executed if "image_url" in s]
        assert len(img_updates) == 1
        assert img_updates[0][1] == ("https://img.example/graph.png", run.id)
    finally:
        _restore_provider(orig)
        if original_execute:
            pipeline_executor.PipelineExecutor.execute = original_execute


# ---------------------------------------------------------------------------
# Tests: image generation with image_model + image_service
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_generate_legacy_image_with_image_service():
    """Legacy pipeline with image_model and image_service generates image."""
    engine = DummySearchEngine([_make_msg()])

    class TrackingDB(FakeDB):
        def __init__(self):
            super().__init__()
            self.executed = []

        async def execute(self, sql, params=()):
            self.executed.append((sql, params))
            return None

    db = TrackingDB()
    image_svc = FakeImageService(url="https://img.example/legacy.png")
    service = ContentGenerationService(db, engine, image_service=image_svc)

    pipeline = ContentPipeline(
        id=1,
        name="Test",
        prompt_template="prompt",
        llm_model="m",
        image_model="together:flux",
        generation_backend=PipelineGenerationBackend.CHAIN,
        publish_mode=PipelinePublishMode.MODERATED,
        # No pipeline_json => legacy path
    )

    orig = _patch_provider(_provider())
    try:
        run = await service.generate(pipeline)
        assert run is not None
        assert len(image_svc.calls) == 1
        assert image_svc.calls[0][0] == "together:flux"
    finally:
        _restore_provider(orig)


@pytest.mark.anyio
async def test_generate_legacy_image_with_default_model():
    """Legacy pipeline uses default_image_model from settings when image_model not set."""
    engine = DummySearchEngine([_make_msg()])
    db = FakeDB()
    db._settings["default_image_model"] = "openai:dall-e"
    image_svc = FakeImageService()
    service = ContentGenerationService(db, engine, image_service=image_svc)

    pipeline = ContentPipeline(
        id=1,
        name="Test",
        prompt_template="prompt",
        llm_model="m",
        image_model=None,  # Use default
        generation_backend=PipelineGenerationBackend.CHAIN,
        publish_mode=PipelinePublishMode.MODERATED,
    )

    orig = _patch_provider(_provider())
    try:
        run = await service.generate(pipeline)
        assert run is not None
        assert len(image_svc.calls) == 1
        assert image_svc.calls[0][0] == "openai:dall-e"
    finally:
        _restore_provider(orig)


@pytest.mark.anyio
async def test_generate_no_image_without_service():
    """No image generation when image_service is None and image_model is set."""
    engine = DummySearchEngine([_make_msg()])
    db = FakeDB()
    service = ContentGenerationService(db, engine, image_service=None)

    pipeline = ContentPipeline(
        id=1,
        name="Test",
        prompt_template="prompt",
        llm_model="m",
        image_model="together:flux",
        generation_backend=PipelineGenerationBackend.CHAIN,
        publish_mode=PipelinePublishMode.MODERATED,
    )

    orig = _patch_provider(_provider())
    try:
        run = await service.generate(pipeline)
        assert run is not None
        assert run.image_url is None
    finally:
        _restore_provider(orig)


# ---------------------------------------------------------------------------
# Tests: refinement steps
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_generate_refinement_steps_applied_metadata():
    """Refinement steps count is stored in metadata."""
    engine = DummySearchEngine([_make_msg()])
    db = FakeDB()
    service = ContentGenerationService(db, engine)

    call_count = [0]

    async def counting_provider(prompt=None, **kwargs):
        call_count[0] += 1
        return f"Output {call_count[0]}"

    pipeline = ContentPipeline(
        id=1,
        name="Test",
        prompt_template="prompt",
        llm_model="m",
        generation_backend=PipelineGenerationBackend.CHAIN,
        publish_mode=PipelinePublishMode.MODERATED,
        refinement_steps=[
            {"name": "step1", "prompt": "Refine: {text}"},
            {"name": "step2", "prompt": "Polish: {text}"},
        ],
    )

    orig = _patch_provider(counting_provider)
    try:
        run = await service.generate(pipeline)
        assert run is not None
        assert run.metadata["refinement_steps_applied"] == 2
    finally:
        _restore_provider(orig)


@pytest.mark.anyio
async def test_generate_refinement_steps_skipped_for_graph_pipeline():
    """Refinement steps are NOT applied when pipeline has pipeline_json (graph-based)."""
    engine = DummySearchEngine([_make_msg()])
    db = FakeDB()
    service = ContentGenerationService(db, engine)

    pipeline = ContentPipeline(
        id=1,
        name="Test",
        prompt_template="prompt",
        llm_model="m",
        generation_backend=PipelineGenerationBackend.CHAIN,
        publish_mode=PipelinePublishMode.MODERATED,
        pipeline_json=PipelineGraph(nodes=[], edges=[]),
        refinement_steps=[{"name": "step1", "prompt": "Refine: {text}"}],
    )

    orig = _patch_provider(_provider())
    from src.services import pipeline_executor

    original_execute = getattr(pipeline_executor.PipelineExecutor, "execute", None)
    pipeline_executor.PipelineExecutor.execute = AsyncMock(
        return_value={"generated_text": "graph output"}
    )
    try:
        run = await service.generate(pipeline)
        assert run is not None
        assert "refinement_steps_applied" not in (run.metadata or {})
    finally:
        _restore_provider(orig)
        if original_execute:
            pipeline_executor.PipelineExecutor.execute = original_execute


@pytest.mark.anyio
async def test_refinement_returns_empty_string_keeps_previous():
    """If refinement step returns empty, previous text is kept."""
    engine = DummySearchEngine([_make_msg()])
    db = FakeDB()
    service = ContentGenerationService(db, engine)

    call_count = [0]

    async def provider_empty_refinement(prompt=None, **kwargs):
        call_count[0] += 1
        if call_count[0] == 1:
            return "Initial text"
        return ""

    pipeline = ContentPipeline(
        id=1,
        name="Test",
        prompt_template="prompt",
        llm_model="m",
        generation_backend=PipelineGenerationBackend.CHAIN,
        publish_mode=PipelinePublishMode.MODERATED,
        refinement_steps=[{"name": "step1", "prompt": "Refine: {text}"}],
    )

    orig = _patch_provider(provider_empty_refinement)
    try:
        run = await service.generate(pipeline)
        assert run is not None
        assert run.generated_text == "Initial text"
    finally:
        _restore_provider(orig)


@pytest.mark.anyio
async def test_refinement_returns_dict():
    """Refinement step that returns dict with 'text' key."""
    engine = DummySearchEngine([_make_msg()])
    db = FakeDB()
    service = ContentGenerationService(db, engine)

    call_count = [0]

    async def provider_dict(prompt=None, **kwargs):
        call_count[0] += 1
        if call_count[0] == 1:
            return "Initial"
        return {"text": "Refined from dict"}

    pipeline = ContentPipeline(
        id=1,
        name="Test",
        prompt_template="prompt",
        llm_model="m",
        generation_backend=PipelineGenerationBackend.CHAIN,
        publish_mode=PipelinePublishMode.MODERATED,
        refinement_steps=[{"name": "step1", "prompt": "Refine: {text}"}],
    )

    orig = _patch_provider(provider_dict)
    try:
        run = await service.generate(pipeline)
        assert run is not None
        assert run.generated_text == "Refined from dict"
    finally:
        _restore_provider(orig)


@pytest.mark.anyio
async def test_refinement_returns_dict_generated_text():
    """Refinement step that returns dict with 'generated_text' key."""
    engine = DummySearchEngine([_make_msg()])
    db = FakeDB()
    service = ContentGenerationService(db, engine)

    call_count = [0]

    async def provider_dict(prompt=None, **kwargs):
        call_count[0] += 1
        if call_count[0] == 1:
            return "Initial"
        return {"generated_text": "Refined from generated_text"}

    pipeline = ContentPipeline(
        id=1,
        name="Test",
        prompt_template="prompt",
        llm_model="m",
        generation_backend=PipelineGenerationBackend.CHAIN,
        publish_mode=PipelinePublishMode.MODERATED,
        refinement_steps=[{"name": "step1", "prompt": "Refine: {text}"}],
    )

    orig = _patch_provider(provider_dict)
    try:
        run = await service.generate(pipeline)
        assert run is not None
        assert run.generated_text == "Refined from generated_text"
    finally:
        _restore_provider(orig)


# ---------------------------------------------------------------------------
# Tests: _run_deep_agents
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_deep_agents_no_manager():
    """DEEP_AGENTS backend without agent_manager raises RuntimeError."""
    engine = DummySearchEngine([_make_msg()])
    db = FakeDB()
    service = ContentGenerationService(db, engine, agent_manager=None)

    pipeline = ContentPipeline(
        id=1,
        name="Test",
        prompt_template="prompt",
        llm_model="m",
        generation_backend=PipelineGenerationBackend.DEEP_AGENTS,
        publish_mode=PipelinePublishMode.MODERATED,
    )

    with pytest.raises(RuntimeError, match="AgentManager"):
        await service._run_deep_agents(pipeline, None, 512, 0.7)


@pytest.mark.anyio
async def test_deep_agents_with_manager():
    """DEEP_AGENTS backend calls agent_manager.chat_stream and returns text."""
    engine = DummySearchEngine([_make_msg()])
    db = FakeDB()

    mock_manager = MagicMock()

    async def fake_stream(*args, **kwargs):
        yield 'data: {"text": "Streamed output"}'
        yield 'data: {"full_text": "Full streamed output"}'

    mock_manager.chat_stream = fake_stream

    service = ContentGenerationService(db, engine, agent_manager=mock_manager)

    pipeline = ContentPipeline(
        id=1,
        name="Test",
        prompt_template="prompt",
        llm_model="m",
        generation_backend=PipelineGenerationBackend.DEEP_AGENTS,
        publish_mode=PipelinePublishMode.MODERATED,
    )

    result = await service._run_deep_agents(pipeline, None, 512, 0.7)
    assert result["generated_text"] == "Full streamed output"
    assert result["citations"] == []


@pytest.mark.anyio
async def test_deep_agents_invalid_json_chunk():
    """Invalid JSON chunks in stream are ignored."""
    engine = DummySearchEngine([_make_msg()])
    db = FakeDB()

    mock_manager = MagicMock()

    async def fake_stream(*args, **kwargs):
        yield 'data: not-json'
        yield 'data: {"text": "Valid text"}'

    mock_manager.chat_stream = fake_stream

    service = ContentGenerationService(db, engine, agent_manager=mock_manager)

    pipeline = ContentPipeline(
        id=1,
        name="Test",
        prompt_template="prompt",
        llm_model="m",
        generation_backend=PipelineGenerationBackend.DEEP_AGENTS,
        publish_mode=PipelinePublishMode.MODERATED,
    )

    result = await service._run_deep_agents(pipeline, None, 512, 0.7)
    assert result["generated_text"] == "Valid text"


# ---------------------------------------------------------------------------
# Tests: _run_graph
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_run_graph_passes_services():
    """_run_graph builds PipelineExecutor with correct service dict."""
    engine = DummySearchEngine([_make_msg()])
    db = FakeDB()
    notification_svc = FakeDraftNotificationService()
    image_svc = FakeImageService()
    client_pool = MagicMock()

    service = ContentGenerationService(
        db, engine,
        image_service=image_svc,
        notification_service=notification_svc,
        client_pool=client_pool,
    )

    graph = PipelineGraph(
        nodes=[PipelineNode(id="n1", type=PipelineNodeType.LLM_GENERATE, name="gen")],
        edges=[],
    )
    pipeline = ContentPipeline(
        id=1,
        name="GraphTest",
        prompt_template="prompt",
        llm_model="gpt-4",
        image_model="openai:dall-e",
        generation_backend=PipelineGenerationBackend.CHAIN,
        publish_mode=PipelinePublishMode.MODERATED,
        pipeline_json=graph,
    )

    orig = _patch_provider(_provider())
    from src.services import pipeline_executor

    original_execute = getattr(pipeline_executor.PipelineExecutor, "execute", None)

    captured_services = {}

    async def capture_execute(self_pipe, pipeline_arg, graph_arg, services):
        captured_services.update(services)
        return {"generated_text": "ok"}

    pipeline_executor.PipelineExecutor.execute = capture_execute

    try:
        result = await service._run_graph(pipeline, "gpt-4", 512, 0.7)
        assert result["generated_text"] == "ok"
        assert captured_services["search_engine"] is engine
        assert captured_services["image_service"] is image_svc
        assert captured_services["notification_service"] is notification_svc
        assert captured_services["client_pool"] is client_pool
        assert captured_services["generation_query"] == "GraphTest"
        assert captured_services["channel_id"] is None
    finally:
        _restore_provider(orig)
        if original_execute:
            pipeline_executor.PipelineExecutor.execute = original_execute


# ---------------------------------------------------------------------------
# Tests: _run_rag
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_run_rag_returns_generated_text():
    """_run_rag calls GenerationService and returns result dict."""
    engine = DummySearchEngine([_make_msg()])
    db = FakeDB()
    service = ContentGenerationService(db, engine)

    pipeline = ContentPipeline(
        id=1,
        name="Test",
        prompt_template="Use {source_messages}",
        llm_model="test-model",
        generation_backend=PipelineGenerationBackend.CHAIN,
        publish_mode=PipelinePublishMode.MODERATED,
    )

    orig = _patch_provider(_provider("RAG output"))
    try:
        result = await service._run_rag(pipeline, "test-model", 512, 0.7)
        assert result["generated_text"] == "RAG output"
    finally:
        _restore_provider(orig)


# ---------------------------------------------------------------------------
# Tests: run not found after save
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_generate_run_not_found_after_save():
    """If generation_runs.get returns None after save, raise RuntimeError."""
    engine = DummySearchEngine([_make_msg()])

    class BugDB(FakeDB):
        def __init__(self):
            super().__init__()
            self._get_call_count = [0]

        @property
        def repos(self):
            return self._repos

    # Create a repo where get() always returns None
    class BugRepo(FakeGenerationRunsRepo):
        async def get(self, run_id):
            return None

    db = FakeDB()
    db.repos.generation_runs = BugRepo()

    service = ContentGenerationService(db, engine)
    pipeline = ContentPipeline(
        id=1,
        name="Test",
        prompt_template="prompt",
        llm_model="m",
        generation_backend=PipelineGenerationBackend.CHAIN,
        publish_mode=PipelinePublishMode.MODERATED,
    )

    orig = _patch_provider(_provider())
    try:
        with pytest.raises(RuntimeError, match="not found after save"):
            await service.generate(pipeline)
    finally:
        _restore_provider(orig)


# ---------------------------------------------------------------------------
# Tests: overall generate failure sets status to failed
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_generate_exception_sets_failed():
    """Any exception during generation should mark run as failed."""
    engine = DummySearchEngine([_make_msg()])
    db = FakeDB()
    service = ContentGenerationService(db, engine)

    pipeline = ContentPipeline(
        id=1,
        name="Test",
        prompt_template="prompt",
        llm_model="m",
        generation_backend=PipelineGenerationBackend.CHAIN,
        publish_mode=PipelinePublishMode.MODERATED,
    )

    async def failing_provider(**kwargs):
        raise RuntimeError("Provider crashed")

    orig = _patch_provider(failing_provider)
    try:
        with pytest.raises(RuntimeError, match="Provider crashed"):
            await service.generate(pipeline)
        # The run should have been marked as failed
        # Since we get a new run_id on each call, check the repo
        for run in db.repos.generation_runs._runs.values():
            if run.status == "failed":
                break
        else:
            pytest.fail("No run was marked as failed")
    finally:
        _restore_provider(orig)


# ---------------------------------------------------------------------------
# Tests: _generate_image edge cases
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_generate_image_no_service():
    """_generate_image returns None when no image_service."""
    engine = DummySearchEngine([_make_msg()])
    db = FakeDB()
    service = ContentGenerationService(db, engine, image_service=None)

    pipeline = ContentPipeline(
        id=1,
        name="Test",
        prompt_template="prompt",
        llm_model="m",
        image_model="test-model",
    )
    result = await service._generate_image(pipeline, "text", model="test-model")
    assert result is None


@pytest.mark.anyio
async def test_generate_image_with_service():
    """_generate_image delegates to image_service.generate."""
    engine = DummySearchEngine([_make_msg()])
    db = FakeDB()
    image_svc = FakeImageService()
    service = ContentGenerationService(db, engine, image_service=image_svc)

    pipeline = ContentPipeline(
        id=1,
        name="Test",
        prompt_template="prompt",
        llm_model="m",
        image_model="test-image-model",
    )
    result = await service._generate_image(pipeline, "test text", model="override-model")
    assert result == "https://img.example/gen.png"
    assert image_svc.calls == [("override-model", "test text")]


@pytest.mark.anyio
async def test_generate_image_uses_pipeline_model_when_no_model_arg():
    """_generate_image uses pipeline.image_model when model arg is None."""
    engine = DummySearchEngine([_make_msg()])
    db = FakeDB()
    image_svc = FakeImageService()
    service = ContentGenerationService(db, engine, image_service=image_svc)

    pipeline = ContentPipeline(
        id=1,
        name="Test",
        prompt_template="prompt",
        llm_model="m",
        image_model="pipeline-image-model",
    )
    result = await service._generate_image(pipeline, "test text", model=None)
    assert result == "https://img.example/gen.png"
    assert image_svc.calls == [("pipeline-image-model", "test text")]


# ---------------------------------------------------------------------------
# Tests: _run_generation backend selection
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_run_generation_selects_graph():
    """_run_generation selects _run_graph when pipeline_json is set."""
    engine = DummySearchEngine([_make_msg()])
    db = FakeDB()
    service = ContentGenerationService(db, engine)

    pipeline = ContentPipeline(
        id=1,
        name="Test",
        prompt_template="prompt",
        llm_model="m",
        generation_backend=PipelineGenerationBackend.CHAIN,
        publish_mode=PipelinePublishMode.MODERATED,
        pipeline_json=PipelineGraph(nodes=[], edges=[]),
    )

    orig = _patch_provider(_provider())
    from src.services import pipeline_executor

    original_execute = getattr(pipeline_executor.PipelineExecutor, "execute", None)
    pipeline_executor.PipelineExecutor.execute = AsyncMock(
        return_value={"generated_text": "graph result"}
    )
    try:
        result = await service._run_generation(pipeline, None, 512, 0.7)
        assert result["generated_text"] == "graph result"
    finally:
        _restore_provider(orig)
        if original_execute:
            pipeline_executor.PipelineExecutor.execute = original_execute


@pytest.mark.anyio
async def test_run_generation_selects_deep_agents():
    """_run_generation selects _run_deep_agents when backend is DEEP_AGENTS."""
    engine = DummySearchEngine([_make_msg()])
    db = FakeDB()

    mock_manager = MagicMock()

    async def fake_stream(*args, **kwargs):
        yield 'data: {"text": "deep output"}'

    mock_manager.chat_stream = fake_stream

    service = ContentGenerationService(db, engine, agent_manager=mock_manager)

    pipeline = ContentPipeline(
        id=1,
        name="Test",
        prompt_template="prompt",
        llm_model="m",
        generation_backend=PipelineGenerationBackend.DEEP_AGENTS,
        publish_mode=PipelinePublishMode.MODERATED,
    )

    result = await service._run_generation(pipeline, None, 512, 0.7)
    assert result["generated_text"] == "deep output"
