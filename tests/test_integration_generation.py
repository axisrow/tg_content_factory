import re

import pytest

from src.models import Account, Channel
from tests.helpers import build_web_app, make_auth_client, make_test_config


@pytest.fixture
async def pipeline_client(tmp_path, real_pool_harness_factory):
    config = make_test_config(tmp_path)
    harness = real_pool_harness_factory()
    app, built_db = await build_web_app(config, harness)
    await built_db.add_account(Account(phone="+100", session_string="sess"))
    await built_db.add_channel(Channel(channel_id=1001, title="Source A"))
    await built_db.repos.dialog_cache.replace_dialogs(
        "+100",
        [
            {
                "channel_id": 77,
                "title": "Target A",
                "username": "targeta",
                "channel_type": "channel",
            }
        ],
    )

    async def _get_dialogs_for_phone(
        self,
        phone,
        include_dm=False,
        mode="full",
        refresh=False,
    ):
        return [
            {
                "channel_id": 77,
                "title": "Target A",
                "username": "targeta",
                "channel_type": "channel",
            }
        ]

    # Patch pool method used by the pipelines page to fetch dialogs
    from types import MethodType

    app.state.pool.get_dialogs_for_phone = MethodType(_get_dialogs_for_phone, app.state.pool)

    async with make_auth_client(app) as client:
        yield client

    await app.state.collection_queue.shutdown()
    await built_db.close()


@pytest.mark.anyio
async def test_pipeline_generate_and_publish(pipeline_client, monkeypatch):
    # Mock LLM provider service so the Generate button is shown in the template
    from unittest.mock import MagicMock

    mock_llm_service = MagicMock()
    mock_llm_service.has_providers = MagicMock(return_value=True)
    mock_llm_service.get_provider_callable = MagicMock(return_value=None)
    app = pipeline_client._transport.app  # type: ignore
    app.state.llm_provider_service = mock_llm_service

    # Create a simple pipeline (reuse existing channels/accounts from fixture)
    resp = await pipeline_client.post(
        "/pipelines/add",
        data={
            "name": "IntegrationDigest",
            "prompt_template": "Summarize {source_messages}",
            "source_channel_ids": ["1001"],
            "target_refs": ["+100|77"],
            "publish_mode": "moderated",
            "generation_backend": "chain",
            "generate_interval_minutes": "60",
            "is_active": "true",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303

    # Find the pipeline id from the pipelines page
    page = await pipeline_client.get("/pipelines/")
    assert page.status_code == 200
    m = re.search(r'href="/pipelines/(\d+)/generate"', page.text)
    assert m, page.text
    pipeline_id = int(m.group(1))

    # Monkeypatch GenerationService.generate to avoid search/provider complexities and return a deterministic result
    async def fake_generate(self, *args, **kwargs):
        return {"prompt": "", "generated_text": "INTEGRATION GENERATED", "citations": []}

    monkeypatch.setattr(
        "src.services.generation_service.GenerationService.generate",
        fake_generate,
    )
    monkeypatch.setattr(
        "src.services.provider_service.AgentProviderService.has_providers",
        lambda self: True,
    )

    # Trigger generation (non-streaming path)
    gen_resp = await pipeline_client.post(
        f"/pipelines/{pipeline_id}/generate",
        data={"model": "", "max_tokens": "256", "temperature": "0.0"},
        follow_redirects=True,
    )
    assert gen_resp.status_code == 200
    # Preview rendering may be async or paginated; assert result persisted in DB below

    # Extract run id from page or fallback to DB lookup
    run_id_match = re.search(r"Preview \(run id=(\d+)\)", gen_resp.text)
    db = pipeline_client._transport.app.state.db
    if run_id_match:
        run_id = int(run_id_match.group(1))
    else:
        runs = await db.repos.generation_runs.list_by_pipeline(pipeline_id)
        assert runs
        run_id = runs[0].id

    run = await db.repos.generation_runs.get(run_id)
    assert run is not None
    assert run.status == "completed", f"generation failed, metadata: {run.metadata}"
    assert "INTEGRATION GENERATED" in (run.generated_text or "")

    # Publish the run
    pub = await pipeline_client.post(
        f"/pipelines/{pipeline_id}/publish", data={"run_id": str(run_id)}, follow_redirects=False
    )
    assert pub.status_code == 303
    assert "pipeline_published" in pub.headers["location"]

    run_after = await db.repos.generation_runs.get(run_id)
    assert run_after is not None
    assert run_after.status == "published"
    assert run_after.metadata and run_after.metadata.get("published") is True


@pytest.mark.anyio
async def test_pipeline_generate_action_only_runs_graph(pipeline_client, monkeypatch):
    """Regression: POST /pipelines/{id}/generate on an action-only pipeline
    (with pipeline_json graph) must exercise PipelineExecutor — not the legacy
    GenerationService — so action_counts accumulate and result_count>0.
    """
    from unittest.mock import AsyncMock, MagicMock

    from src.models import (
        ContentPipeline,
        PipelineEdge,
        PipelineGraph,
        PipelineNode,
        PipelineNodeType,
        PipelinePublishMode,
        PipelineTarget,
    )
    from src.services.pipeline_result import increment_action_count

    app = pipeline_client._transport.app  # type: ignore
    db = app.state.db

    graph = PipelineGraph(
        nodes=[
            PipelineNode(id="src", type=PipelineNodeType.SOURCE, name="src", config={}),
            PipelineNode(id="react", type=PipelineNodeType.REACT, name="react", config={}),
        ],
        edges=[PipelineEdge(from_node="src", to_node="react")],
    )
    pipeline_id = await db.repos.content_pipelines.add(
        pipeline=ContentPipeline(
            name="ActionOnlyWeb",
            prompt_template="",
            publish_mode=PipelinePublishMode.MODERATED,
            pipeline_json=graph,
        ),
        source_channel_ids=[1001],
        targets=[
            PipelineTarget(
                pipeline_id=0,
                phone="+100",
                dialog_id=77,
                title="Target A",
                dialog_type="channel",
            )
        ],
    )

    # Ensure the web route's LLM gate lets this action-only pipeline through.
    mock_llm_service = MagicMock()
    mock_llm_service.has_providers = MagicMock(return_value=True)
    mock_llm_service.get_provider_callable = MagicMock(return_value=None)
    app.state.llm_provider_service = mock_llm_service

    def fake_get_handler(node_type):
        h = AsyncMock()

        async def _execute(config, ctx, services):
            if node_type == PipelineNodeType.REACT:
                increment_action_count(ctx, "react", amount=4)

        h.execute.side_effect = _execute
        return h

    monkeypatch.setattr(
        "src.services.pipeline_executor.get_handler", fake_get_handler
    )

    resp = await pipeline_client.post(
        f"/pipelines/{pipeline_id}/generate",
        data={"model": "", "max_tokens": "256", "temperature": "0.0"},
        follow_redirects=True,
    )
    assert resp.status_code == 200, resp.text

    runs = await db.repos.generation_runs.list_by_pipeline(pipeline_id)
    assert runs, "web generate must create a generation_run"
    run = runs[0]
    assert run.status == "completed", (
        f"run status={run.status}, metadata={run.metadata}"
    )
    assert run.result_kind == "processed_messages"
    assert run.result_count == 4
    assert (run.metadata or {}).get("action_counts") == {"react": 4}
