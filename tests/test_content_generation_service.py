from datetime import datetime, timezone

from src.models import ContentPipeline, Message, PipelineGenerationBackend, PipelinePublishMode, SearchResult
from src.services.content_generation_service import ContentGenerationService


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

    async def execute(self, sql, params=()):
        return None

    async def get_setting(self, key):
        return None


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


async def test_content_generation_service_skips_image_generation_without_service():
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
        image_model="stub-image-model",
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
            structure=self.overall,
            overall=self.overall,
            issues=self.issues,
        )


async def test_content_generation_service_notifies_for_moderated_drafts():
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
    notifications = FakeDraftNotificationService()
    service = ContentGenerationService(db, engine, notification_service=notifications)

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
        assert len(notifications.calls) == 1
        assert notifications.calls[0][0].id == run.id
    finally:
        if original_get:
            provider_service.AgentProviderService.get_provider_callable = original_get


async def test_content_generation_service_skips_notification_for_auto_publish():
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
    notifications = FakeDraftNotificationService()
    service = ContentGenerationService(db, engine, notification_service=notifications)

    pipeline = ContentPipeline(
        id=1,
        name="Test Pipeline",
        prompt_template="Use {source_messages}",
        llm_model="test-model",
        generation_backend=PipelineGenerationBackend.CHAIN,
        publish_mode=PipelinePublishMode.AUTO,
    )

    from src.services import provider_service

    original_get = getattr(provider_service.AgentProviderService, "get_provider_callable", None)
    provider_service.AgentProviderService.get_provider_callable = lambda self, model: fake_provider
    try:
        await service.generate(pipeline)
        assert notifications.calls == []
    finally:
        if original_get:
            provider_service.AgentProviderService.get_provider_callable = original_get


async def test_content_generation_service_records_quality_score():
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
    quality = FakeQualityService(overall=0.91, issues=["missing CTA"])
    service = ContentGenerationService(db, engine, quality_service=quality)

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
        assert run.quality_score == 0.91
        assert run.quality_issues == ["missing CTA"]
        assert quality.calls == [(run.generated_text, "test-model")]
    finally:
        if original_get:
            provider_service.AgentProviderService.get_provider_callable = original_get


# ── Issue #463: _build_metadata invariants ───────────────────────────────────


class TestBuildMetadataInvariants:
    """Pure unit tests on ContentGenerationService._build_metadata.

    Invariants (issue #463):
      - action_counts ALWAYS survives for mixed / action-only runs.
      - Pure generation runs stay lean (no action_counts key).
      - dry_run / publish_reply / reply_to_message_id are opt-in flags.
    """

    def test_mixed_result_preserves_action_counts(self):
        result = {
            "generated_text": "draft",
            "citations": [{"id": 1}, {"id": 2}],
            "result_kind": "generated_items",
            "result_count": 2,
            "action_counts": {"react": 3},
            "publish_mode": "moderated",
        }
        metadata = ContentGenerationService._build_metadata(result, dry_run=False)
        assert metadata["action_counts"] == {"react": 3}
        assert metadata["result_kind"] == "generated_items"
        assert metadata["result_count"] == 2
        assert metadata["citations"] == [{"id": 1}, {"id": 2}]
        assert metadata["effective_publish_mode"] == "moderated"
        assert "dry_run" not in metadata

    def test_pure_generation_has_no_action_counts_key(self):
        result = {
            "generated_text": "draft",
            "citations": [{"id": 1}],
            "result_kind": "generated_items",
            "result_count": 1,
            "action_counts": {},
            "publish_mode": "auto",
        }
        metadata = ContentGenerationService._build_metadata(result, dry_run=False)
        assert "action_counts" not in metadata
        assert metadata["result_kind"] == "generated_items"
        assert metadata["result_count"] == 1

    def test_action_only_includes_action_counts(self):
        result = {
            "generated_text": "",
            "citations": [],
            "result_kind": "processed_messages",
            "result_count": 5,
            "action_counts": {"react": 5},
            "publish_mode": "moderated",
        }
        metadata = ContentGenerationService._build_metadata(result, dry_run=False)
        assert metadata["action_counts"] == {"react": 5}
        assert metadata["result_kind"] == "processed_messages"
        assert metadata["result_count"] == 5

    def test_dry_run_flag_set(self):
        result = {"result_kind": "generated_items", "result_count": 1, "publish_mode": "auto"}
        metadata = ContentGenerationService._build_metadata(result, dry_run=True)
        assert metadata["dry_run"] is True

    def test_publish_reply_propagates(self):
        result = {
            "result_kind": "generated_items",
            "result_count": 1,
            "publish_mode": "auto",
            "publish_reply": True,
            "reply_to_message_id": 12345,
        }
        metadata = ContentGenerationService._build_metadata(result, dry_run=False)
        assert metadata["publish_reply"] is True
        assert metadata["reply_to_message_id"] == 12345

    def test_missing_result_count_defaults_to_zero(self):
        result = {"result_kind": "processed_messages", "publish_mode": "auto"}
        metadata = ContentGenerationService._build_metadata(result, dry_run=False)
        assert metadata["result_count"] == 0

    def test_node_errors_preserved_in_metadata(self):
        """Issue #463: errors from node execution must survive into run.metadata."""
        result = {
            "result_kind": "processed_messages",
            "result_count": 0,
            "publish_mode": "moderated",
            "node_errors": [
                {
                    "node_id": "react_1",
                    "code": "chat_write_forbidden",
                    "detail": "ChatWriteForbiddenError on message 6239",
                }
            ],
        }
        metadata = ContentGenerationService._build_metadata(result, dry_run=False)
        assert metadata["node_errors"] == result["node_errors"]

    def test_no_node_errors_key_when_empty(self):
        """A clean run should not have an empty node_errors key in metadata."""
        metadata = ContentGenerationService._build_metadata(
            {
                "result_kind": "generated_items",
                "result_count": 1,
                "publish_mode": "auto",
                "node_errors": [],
            },
            dry_run=False,
        )
        assert "node_errors" not in metadata

    def test_no_node_errors_key_when_missing(self):
        metadata = ContentGenerationService._build_metadata(
            {
                "result_kind": "generated_items",
                "result_count": 1,
                "publish_mode": "auto",
            },
            dry_run=False,
        )
        assert "node_errors" not in metadata


# ── Issue #463: _run_graph propagates node_errors from PipelineExecutor ──────


async def test_run_graph_copies_node_errors_from_executor(monkeypatch):
    """_run_graph must forward node_errors from PipelineExecutor.execute() so
    that ContentGenerationService.generate -> _build_metadata sees them.
    Previously the dict passthrough dropped the key entirely.
    """
    from unittest.mock import AsyncMock, MagicMock

    from src.models import ContentPipeline, PipelineEdge, PipelineGraph, PipelineNode, PipelineNodeType
    from src.services.content_generation_service import ContentGenerationService

    pipeline = ContentPipeline(
        id=1,
        name="X",
        prompt_template="",
        pipeline_json=PipelineGraph(
            nodes=[PipelineNode(id="react", type=PipelineNodeType.REACT, name="react", config={})],
            edges=[],
        ),
    )

    executor_result = {
        "generated_text": "",
        "citations": [],
        "publish_mode": None,
        "action_counts": {},
        "result_kind": "processed_messages",
        "result_count": 0,
        "node_errors": [
            {"node_id": "react", "code": "no_client_pool", "detail": "..."}
        ],
    }

    class _FakeExecutor:
        async def execute(self, p, g, services):
            return executor_result

    monkeypatch.setattr(
        "src.services.pipeline_executor.PipelineExecutor",
        _FakeExecutor,
    )
    # resolve_retrieval_scope + list_sources path
    async def _fake_scope(pipeline, list_sources):
        s = MagicMock()
        s.query = ""
        s.channel_id = None
        return s

    monkeypatch.setattr("src.services.pipeline_service.resolve_retrieval_scope", _fake_scope)

    db = MagicMock()
    db.repos = MagicMock()
    db.repos.content_pipelines = MagicMock()
    db.repos.content_pipelines.list_sources = AsyncMock(return_value=[])
    db.get_setting = AsyncMock(return_value="")

    svc = ContentGenerationService(db, MagicMock(), config=MagicMock())
    # Stub _get_provider_callable to avoid hitting provider_service
    async def _noop_provider(model):
        return None
    svc._get_provider_callable = _noop_provider  # type: ignore[attr-defined]

    out = await svc._run_graph(pipeline, None, 100, 0.0)
    assert out["node_errors"] == executor_result["node_errors"]
