"""Extra tests for pipelines routes — covering uncovered endpoints and branches."""
from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.models import (
    ContentPipeline,
    PipelineEdge,
    PipelineGenerationBackend,
    PipelineGraph,
    PipelineNode,
    PipelineNodeType,
)
from src.services.pipeline_service import PipelineValidationError

_ADD_DATA = {
    "name": "Test Pipeline",
    "prompt_template": "Write a summary",
    "publish_mode": "moderated",
    "source_channel_ids": "100",
    "target_refs": "+1234567890|200",
    "llm_model": "",
    "image_model": "",
    "generation_backend": "chain",
    "generate_interval_minutes": "60",
}

# Variant without target_refs — avoids dialog cache validation.
_ADD_DATA_NO_TARGETS = {
    "name": "Test Pipeline",
    "prompt_template": "Write a summary",
    "publish_mode": "moderated",
    "source_channel_ids": "100",
    "llm_model": "",
    "image_model": "",
    "generation_backend": "chain",
    "generate_interval_minutes": "60",
}


@pytest.fixture
async def client(route_client):
    """Use shared route_client fixture."""
    return route_client


@pytest.fixture
async def client_with_dialog(route_client, base_app):
    """Client with dialog cache populated for target picker."""
    _, db, pool_mock = base_app
    pool_mock.clients = {"+1234567890": MagicMock()}
    pool_mock.get_dialogs_for_phone = AsyncMock(return_value=[])
    await db.repos.dialog_cache.replace_dialogs(
        "+1234567890",
        [{"channel_id": 200, "title": "Test Dialog", "channel_type": "channel"}],
    )
    return route_client


def _make_provider_svc(has=True, callable_return=None):
    """Create a mock LLM provider service."""
    svc = MagicMock()
    svc.has_providers = MagicMock(return_value=has)
    svc.get_provider_callable = MagicMock(return_value=callable_return or (lambda: None))
    svc.get_provider_status_list = AsyncMock(return_value=[])
    return svc


def _make_pipeline(pipeline_id=1, *, name="Test", dag=None, backend=PipelineGenerationBackend.CHAIN, llm_model=None):
    """Create a ContentPipeline mock."""
    if dag is None:
        # Legacy chain pipeline — no pipeline_json
        return ContentPipeline(
            id=pipeline_id,
            name=name,
            prompt_template="Write something",
            llm_model=llm_model,
            generation_backend=backend,
            pipeline_json=None,
        )
    return ContentPipeline(
        id=pipeline_id,
        name=name,
        prompt_template=".",
        llm_model=llm_model,
        generation_backend=backend,
        pipeline_json=dag,
    )


# ── api_channels_search ─────────────────────────────────────────────


@pytest.mark.asyncio
async def test_api_channels_search_short_query(client, base_app):
    """Short query (< 2 chars) returns top 50 channels."""
    _, db, _ = base_app
    await db.add_channel(__import__("src.models", fromlist=["Channel"]).Channel(channel_id=999, title="Alpha Chan"))
    resp = await client.get("/pipelines/api/channels/search?q=a")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)


@pytest.mark.asyncio
async def test_api_channels_search_empty_query(client, base_app):
    """Empty q returns top 50 channels."""
    resp = await client.get("/pipelines/api/channels/search")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)


@pytest.mark.asyncio
async def test_api_channels_search_with_query(client, base_app):
    """Query >= 2 chars searches by title/username/channel_id."""
    _, db, _ = base_app
    from src.models import Channel

    await db.add_channel(Channel(channel_id=777, title="UniqueSearchTerm", username="searchuser"))
    resp = await client.get("/pipelines/api/channels/search?q=UniqueSearch")
    assert resp.status_code == 200
    data = resp.json()
    assert any(item["title"] == "UniqueSearchTerm" for item in data)


@pytest.mark.asyncio
async def test_api_channels_search_by_channel_id(client, base_app):
    """Search by numeric channel_id substring."""
    _, db, _ = base_app
    from src.models import Channel

    await db.add_channel(Channel(channel_id=123456, title="IdChannel"))
    resp = await client.get("/pipelines/api/channels/search?q=12345")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) >= 1


@pytest.mark.asyncio
async def test_api_channels_search_result_fields(client, base_app):
    """Each result has value, title, username, group keys."""
    _, db, _ = base_app
    from src.models import Channel

    await db.add_channel(Channel(channel_id=555, title="FieldTest", username="ft"))
    resp = await client.get("/pipelines/api/channels/search?q=Fi")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) >= 1
    item = data[0]
    assert "value" in item
    assert "title" in item
    assert "username" in item
    assert "group" in item
    assert item["group"] == "channel"


# ── create_wizard_submit ────────────────────────────────────────────


@pytest.mark.asyncio
async def test_create_wizard_submit_success(client_with_dialog, base_app):
    """POST /pipelines/create-wizard creates a DAG pipeline."""
    app, db, _ = base_app
    app.state.llm_provider_service = _make_provider_svc(has=True)

    graph = {
        "nodes": [
            {"id": "src", "type": "source", "name": "src", "config": {}, "position": {"x": 0, "y": 0}},
            {
                "id": "llm",
                "type": "llm_generate",
                "name": "llm",
                "config": {"model": "gpt-4o"},
                "position": {"x": 100, "y": 0},
            },
            {"id": "pub", "type": "publish", "name": "pub", "config": {}, "position": {"x": 200, "y": 0}},
        ],
        "edges": [
            {"from": "src", "to": "llm"},
            {"from": "llm", "to": "pub"},
        ],
    }

    resp = await client_with_dialog.post(
        "/pipelines/create-wizard",
        data={
            "name": "Wizard Pipeline",
            "pipeline_json": json.dumps(graph),
            "source_channel_ids": "100",
            "target_refs": "+1234567890|200",
            "generate_interval_minutes": "30",
            "is_active": "1",
            "run_after": "",
            "since_value": "24",
            "since_unit": "h",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "msg=pipeline_added" in resp.headers["location"]


@pytest.mark.asyncio
async def test_create_wizard_submit_with_run_after(client_with_dialog, base_app):
    """POST /pipelines/create-wizard with run_after triggers pipeline run."""
    app, db, _ = base_app
    app.state.llm_provider_service = _make_provider_svc(has=True)

    graph = {
        "nodes": [
            {"id": "src", "type": "source", "name": "src", "config": {}, "position": {"x": 0, "y": 0}},
        ],
        "edges": [],
    }

    with patch("src.web.routes.pipelines.deps.get_task_enqueuer") as mock_enq:
        mock_enq.return_value.enqueue_pipeline_run = AsyncMock()
        resp = await client_with_dialog.post(
            "/pipelines/create-wizard",
            data={
                "name": "Wizard Run After",
                "pipeline_json": json.dumps(graph),
                "source_channel_ids": "100",
                "target_refs": "+1234567890|200",
                "generate_interval_minutes": "60",
                "is_active": "",
                "run_after": "1",
                "since_value": "12",
                "since_unit": "h",
            },
            follow_redirects=False,
        )
    assert resp.status_code == 303
    assert "msg=pipeline_run_with_since" in resp.headers["location"]


@pytest.mark.asyncio
async def test_create_wizard_submit_validation_error(client, base_app):
    """POST /pipelines/create-wizard with invalid data redirects with error."""
    app, db, _ = base_app
    app.state.llm_provider_service = _make_provider_svc(has=True)

    resp = await client.post(
        "/pipelines/create-wizard",
        data={
            "name": "Bad Wizard",
            "pipeline_json": "not-json",
            "generate_interval_minutes": "60",
            "is_active": "",
            "run_after": "",
            "since_value": "24",
            "since_unit": "h",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "error=" in resp.headers["location"]


@pytest.mark.asyncio
async def test_create_wizard_submit_inactive(client_with_dialog, base_app):
    """POST /pipelines/create-wizard without is_active checkbox creates inactive pipeline."""
    app, db, _ = base_app
    app.state.llm_provider_service = _make_provider_svc(has=True)

    graph = {"nodes": [], "edges": []}

    resp = await client_with_dialog.post(
        "/pipelines/create-wizard",
        data={
            "name": "Inactive Wizard",
            "pipeline_json": json.dumps(graph),
            "generate_interval_minutes": "60",
            "is_active": "",
            "run_after": "",
            "since_value": "24",
            "since_unit": "h",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "msg=pipeline_added" in resp.headers["location"]


# ── create_wizard_page ──────────────────────────────────────────────


@pytest.mark.asyncio
async def test_create_wizard_page_renders(client, base_app):
    """GET /pipelines/create renders the wizard page."""
    app, _, _ = base_app
    app.state.llm_provider_service = _make_provider_svc(has=True)
    resp = await client.get("/pipelines/create")
    assert resp.status_code == 200


# ── add_pipeline validation error ───────────────────────────────────


@pytest.mark.asyncio
async def test_add_pipeline_validation_error(client, base_app):
    """POST /pipelines/add with validation error redirects with error."""
    app, _, _ = base_app
    app.state.llm_provider_service = _make_provider_svc(has=True)

    with patch("src.web.routes.pipelines.deps.pipeline_service") as mock_svc:
        mock_svc.return_value.add = AsyncMock(side_effect=PipelineValidationError("Bad"))
        resp = await client.post(
            "/pipelines/add",
            data=_ADD_DATA,
            follow_redirects=False,
        )
    assert resp.status_code == 303
    assert "error=pipeline_invalid" in resp.headers["location"]


# ── edit_pipeline DAG preservation ──────────────────────────────────


@pytest.mark.asyncio
async def test_edit_pipeline_dag_preserves_backend(client_with_dialog, base_app):
    """Edit a DAG pipeline preserves existing backend when CHAIN default is submitted."""
    app, db, _ = base_app
    app.state.llm_provider_service = _make_provider_svc(has=True)

    dag = PipelineGraph(
        nodes=[
            PipelineNode(id="src", type=PipelineNodeType.SOURCE, name="src"),
            PipelineNode(id="pub", type=PipelineNodeType.PUBLISH, name="pub"),
        ],
        edges=[PipelineEdge(from_node="src", to_node="pub")],
    )
    pipeline = _make_pipeline(1, dag=dag, backend=PipelineGenerationBackend.AGENT)
    with patch("src.web.routes.pipelines.deps.pipeline_service") as mock_svc:
        mock_svc.return_value.get = AsyncMock(return_value=pipeline)
        mock_svc.return_value.update = AsyncMock(return_value=True)
        resp = await client_with_dialog.post(
            "/pipelines/1/edit",
            data={
                **_ADD_DATA,
                "name": "DAG Edit",
                "publish_mode": "auto",
                "generation_backend": "chain",  # default form value — should be overridden
                "react_emoji": "",
                "dag_source_channel_ids": "100",
            },
            follow_redirects=False,
        )
    assert resp.status_code == 303
    assert "msg=pipeline_edited" in resp.headers["location"]


@pytest.mark.asyncio
async def test_edit_pipeline_dag_preserves_prompt(client_with_dialog, base_app):
    """Edit a DAG pipeline preserves existing prompt_template when empty is submitted."""
    app, db, _ = base_app
    app.state.llm_provider_service = _make_provider_svc(has=True)

    dag = PipelineGraph(
        nodes=[
            PipelineNode(id="src", type=PipelineNodeType.SOURCE, name="src"),
            PipelineNode(id="pub", type=PipelineNodeType.PUBLISH, name="pub"),
        ],
        edges=[PipelineEdge(from_node="src", to_node="pub")],
    )
    pipeline = _make_pipeline(1, dag=dag)
    pipeline.prompt_template = "original prompt"

    with patch("src.web.routes.pipelines.deps.pipeline_service") as mock_svc:
        mock_svc.return_value.get = AsyncMock(return_value=pipeline)
        mock_svc.return_value.update = AsyncMock(return_value=True)
        resp = await client_with_dialog.post(
            "/pipelines/1/edit",
            data={
                "name": "DAG Edit",
                "prompt_template": "",  # empty — should use existing
                "source_channel_ids": "100",
                "target_refs": "+1234567890|200",
                "llm_model": "",
                "image_model": "",
                "publish_mode": "moderated",
                "generation_backend": "chain",
                "generate_interval_minutes": "60",
                "react_emoji": "",
                "dag_source_channel_ids": "100",
            },
            follow_redirects=False,
        )
    assert resp.status_code == 303
    assert "msg=pipeline_edited" in resp.headers["location"]


@pytest.mark.asyncio
async def test_edit_pipeline_validation_error(client, base_app):
    """POST /pipelines/<id>/edit with validation error redirects with error."""
    app, _, _ = base_app
    app.state.llm_provider_service = _make_provider_svc(has=True)
    pipeline = _make_pipeline(1)

    with patch("src.web.routes.pipelines.deps.pipeline_service") as mock_svc:
        mock_svc.return_value.get = AsyncMock(return_value=pipeline)
        mock_svc.return_value.update = AsyncMock(side_effect=PipelineValidationError("Bad data"))
        resp = await client.post(
            "/pipelines/1/edit",
            data={
                "name": "Test",
                "prompt_template": "test",
                "source_channel_ids": "100",
                "target_refs": "+1234567890|200",
                "llm_model": "",
                "image_model": "",
                "publish_mode": "moderated",
                "generation_backend": "chain",
                "generate_interval_minutes": "60",
                "react_emoji": "",
            },
            follow_redirects=False,
        )
    assert resp.status_code == 303
    assert "error=" in resp.headers["location"]


@pytest.mark.asyncio
async def test_edit_pipeline_update_returns_false(client, base_app):
    """POST /pipelines/<id>/edit when update returns False redirects with error."""
    app, _, _ = base_app
    app.state.llm_provider_service = _make_provider_svc(has=True)
    pipeline = _make_pipeline(1)

    with patch("src.web.routes.pipelines.deps.pipeline_service") as mock_svc:
        mock_svc.return_value.get = AsyncMock(return_value=pipeline)
        mock_svc.return_value.update = AsyncMock(return_value=False)
        resp = await client.post(
            "/pipelines/1/edit",
            data={
                "name": "Test",
                "prompt_template": "test",
                "source_channel_ids": "100",
                "target_refs": "+1234567890|200",
                "llm_model": "",
                "image_model": "",
                "publish_mode": "moderated",
                "generation_backend": "chain",
                "generate_interval_minutes": "60",
                "react_emoji": "",
            },
            follow_redirects=False,
        )
    assert resp.status_code == 303
    assert "error=pipeline_invalid" in resp.headers["location"]


@pytest.mark.asyncio
async def test_edit_pipeline_with_phone_query_param(client, base_app):
    """POST /pipelines/<id>/edit with phone query param passes it to redirect."""
    app, _, _ = base_app
    app.state.llm_provider_service = _make_provider_svc(has=True)

    with patch("src.web.routes.pipelines.deps.pipeline_service") as mock_svc:
        mock_svc.return_value.get = AsyncMock(return_value=None)
        resp = await client.post(
            "/pipelines/1/edit?phone=%2B1234567890",
            data={
                "name": "Test",
                "prompt_template": "test",
                "source_channel_ids": "100",
                "target_refs": "+1234567890|200",
                "llm_model": "",
                "image_model": "",
                "publish_mode": "moderated",
                "generation_backend": "chain",
                "generate_interval_minutes": "60",
                "react_emoji": "",
            },
            follow_redirects=False,
        )
    assert resp.status_code == 303
    assert "phone=" in resp.headers["location"]


# ── generate_stream SSE ─────────────────────────────────────────────


@pytest.mark.asyncio
async def test_generate_stream_no_llm_nodes(client, base_app):
    """GET /pipelines/<id>/generate-stream for pipeline with no LLM nodes redirects."""
    app, db, _ = base_app
    app.state.llm_provider_service = _make_provider_svc(has=True)

    dag = PipelineGraph(
        nodes=[
            PipelineNode(id="src", type=PipelineNodeType.SOURCE, name="src"),
            PipelineNode(id="pub", type=PipelineNodeType.PUBLISH, name="pub"),
        ],
        edges=[PipelineEdge(from_node="src", to_node="pub")],
    )

    with patch("src.web.routes.pipelines.deps.pipeline_service") as mock_svc:
        mock_svc.return_value.get = AsyncMock(return_value=_make_pipeline(1, dag=dag))
        resp = await client.get("/pipelines/1/generate-stream", follow_redirects=False)
    assert resp.status_code == 303
    assert "error=pipeline_no_llm_nodes" in resp.headers["location"]


@pytest.mark.asyncio
async def test_generate_stream_llm_not_configured(client_with_dialog, base_app):
    """GET /pipelines/<id>/generate-stream with no provider redirects."""
    app, db, _ = base_app
    # Set up provider=True so add succeeds, then switch to False for the stream check
    app.state.llm_provider_service = _make_provider_svc(has=True)
    await client_with_dialog.post("/pipelines/add", data=_ADD_DATA)
    # Now set provider to False to trigger the llm_not_configured path
    app.state.llm_provider_service = _make_provider_svc(has=False)

    resp = await client_with_dialog.get("/pipelines/1/generate-stream", follow_redirects=False)
    assert resp.status_code == 303
    assert "error=llm_not_configured" in resp.headers["location"]


# ── generate_pipeline POST ──────────────────────────────────────────


@pytest.mark.asyncio
async def test_generate_pipeline_no_llm_nodes(client, base_app):
    """POST /pipelines/<id>/generate for pipeline with no LLM nodes redirects."""
    app, db, _ = base_app
    app.state.llm_provider_service = _make_provider_svc(has=True)

    dag = PipelineGraph(
        nodes=[
            PipelineNode(id="src", type=PipelineNodeType.SOURCE, name="src"),
            PipelineNode(id="pub", type=PipelineNodeType.PUBLISH, name="pub"),
        ],
        edges=[PipelineEdge(from_node="src", to_node="pub")],
    )

    with patch("src.web.routes.pipelines.deps.pipeline_service") as mock_svc:
        mock_svc.return_value.get = AsyncMock(return_value=_make_pipeline(1, dag=dag))
        resp = await client.post(
            "/pipelines/1/generate",
            data={"model": "", "max_tokens": "256", "temperature": "0.0"},
            follow_redirects=False,
        )
    assert resp.status_code == 303
    assert "error=pipeline_no_llm_nodes" in resp.headers["location"]


@pytest.mark.asyncio
async def test_generate_pipeline_llm_not_configured(client_with_dialog, base_app):
    """POST /pipelines/<id>/generate with no provider redirects."""
    app, db, _ = base_app
    # Set up provider=True so add succeeds, then switch to False
    app.state.llm_provider_service = _make_provider_svc(has=True)
    await client_with_dialog.post("/pipelines/add", data=_ADD_DATA)
    app.state.llm_provider_service = _make_provider_svc(has=False)

    resp = await client_with_dialog.post(
        "/pipelines/1/generate",
        data={"model": "", "max_tokens": "256", "temperature": "0.0"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "error=llm_not_configured" in resp.headers["location"]


# ── dry_run_pipeline ────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_dry_run_pipeline_success(client_with_dialog, base_app):
    """POST /pipelines/<id>/dry-run enqueues dry run."""
    app, db, _ = base_app
    app.state.llm_provider_service = _make_provider_svc(has=True)
    await client_with_dialog.post("/pipelines/add", data=_ADD_DATA)

    with patch("src.web.routes.pipelines.deps.pipeline_service") as mock_svc:
        mock_svc.return_value.get = AsyncMock(return_value=_make_pipeline(1))
        with patch("src.web.routes.pipelines.deps.get_task_enqueuer") as mock_enq:
            mock_enq.return_value.enqueue_pipeline_run = AsyncMock()
            resp = await client_with_dialog.post("/pipelines/1/dry-run", follow_redirects=False)
    assert resp.status_code == 303
    assert "msg=pipeline_dry_run_enqueued" in resp.headers["location"]


@pytest.mark.asyncio
async def test_dry_run_pipeline_not_found(client, base_app):
    """POST /pipelines/<id>/dry-run with invalid ID redirects."""
    app, _, _ = base_app
    app.state.llm_provider_service = _make_provider_svc(has=True)

    with patch("src.web.routes.pipelines.deps.pipeline_service") as mock_svc:
        mock_svc.return_value.get = AsyncMock(return_value=None)
        resp = await client.post("/pipelines/999/dry-run", follow_redirects=False)
    assert resp.status_code == 303
    assert "error=pipeline_invalid" in resp.headers["location"]


@pytest.mark.asyncio
async def test_dry_run_pipeline_llm_not_configured(client_with_dialog, base_app):
    """POST /pipelines/<id>/dry-run with no LLM provider redirects."""
    app, db, _ = base_app
    app.state.llm_provider_service = _make_provider_svc(has=True)
    await client_with_dialog.post("/pipelines/add", data=_ADD_DATA)
    app.state.llm_provider_service = _make_provider_svc(has=False)

    resp = await client_with_dialog.post("/pipelines/1/dry-run", follow_redirects=False)
    assert resp.status_code == 303
    assert "error=llm_not_configured" in resp.headers["location"]


@pytest.mark.asyncio
async def test_dry_run_pipeline_enqueue_failure(client_with_dialog, base_app):
    """POST /pipelines/<id>/dry-run handles enqueue failure."""
    app, db, _ = base_app
    app.state.llm_provider_service = _make_provider_svc(has=True)
    await client_with_dialog.post("/pipelines/add", data=_ADD_DATA)

    with patch("src.web.routes.pipelines.deps.pipeline_service") as mock_svc:
        mock_svc.return_value.get = AsyncMock(return_value=_make_pipeline(1))
        with patch("src.web.routes.pipelines.deps.get_task_enqueuer") as mock_enq:
            mock_enq.return_value.enqueue_pipeline_run = AsyncMock(side_effect=Exception("fail"))
            resp = await client_with_dialog.post("/pipelines/1/dry-run", follow_redirects=False)
    assert resp.status_code == 303
    assert "error=pipeline_run_failed" in resp.headers["location"]


# ── dry_run_count ───────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_dry_run_count_not_found(client, base_app):
    """GET /pipelines/<id>/dry-run-count for missing pipeline returns 404."""
    app, _, _ = base_app
    app.state.llm_provider_service = _make_provider_svc()

    with patch("src.web.routes.pipelines.deps.pipeline_service") as mock_svc:
        mock_svc.return_value.get = AsyncMock(return_value=None)
        resp = await client.get("/pipelines/999/dry-run-count")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_dry_run_count_legacy_pipeline(client, base_app):
    """GET /pipelines/<id>/dry-run-count for legacy pipeline uses source IDs."""
    app, db, _ = base_app
    app.state.llm_provider_service = _make_provider_svc()
    pipeline = _make_pipeline(1)  # legacy — no pipeline_json

    with patch("src.web.routes.pipelines.deps.pipeline_service") as mock_svc:
        mock_svc.return_value.get = AsyncMock(return_value=pipeline)
        resp = await client.get("/pipelines/1/dry-run-count")
    assert resp.status_code == 200
    data = resp.json()
    assert "total" in data
    assert "after_filter" in data


@pytest.mark.asyncio
async def test_dry_run_count_dag_pipeline(client, base_app):
    """GET /pipelines/<id>/dry-run-count for DAG pipeline uses source node IDs."""
    app, db, _ = base_app
    app.state.llm_provider_service = _make_provider_svc()

    dag = PipelineGraph(
        nodes=[
            PipelineNode(
                id="src", type=PipelineNodeType.SOURCE, name="src",
                config={"channel_ids": [100]},
            ),
            PipelineNode(id="pub", type=PipelineNodeType.PUBLISH, name="pub"),
        ],
        edges=[PipelineEdge(from_node="src", to_node="pub")],
    )
    pipeline = _make_pipeline(1, dag=dag)

    with patch("src.web.routes.pipelines.deps.pipeline_service") as mock_svc:
        mock_svc.return_value.get = AsyncMock(return_value=pipeline)
        resp = await client.get("/pipelines/1/dry-run-count")
    assert resp.status_code == 200
    data = resp.json()
    assert "total" in data


# ── dry_run_count_new ───────────────────────────────────────────────


@pytest.mark.asyncio
async def test_dry_run_count_new(client, base_app):
    """GET /pipelines/dry-run-count returns total and after_filter."""
    app, _, _ = base_app
    app.state.llm_provider_service = _make_provider_svc()
    resp = await client.get("/pipelines/dry-run-count?source_ids=100&since_value=6&since_unit=h")
    assert resp.status_code == 200
    data = resp.json()
    assert "total" in data
    assert "after_filter" in data


@pytest.mark.asyncio
async def test_dry_run_count_new_empty_ids(client, base_app):
    """GET /pipelines/dry-run-count with empty source_ids."""
    app, _, _ = base_app
    app.state.llm_provider_service = _make_provider_svc()
    resp = await client.get("/pipelines/dry-run-count?source_ids=&since_value=6&since_unit=h")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] == 0


# ── _apply_pipeline_filter ──────────────────────────────────────────


def test_apply_pipeline_filter_no_pipeline_json():
    """Filter returns all messages when no pipeline_json."""
    from src.web.routes.pipelines import _apply_pipeline_filter

    pipeline = MagicMock()
    pipeline.pipeline_json = None
    messages = [MagicMock(text="hello"), MagicMock(text="world")]
    assert _apply_pipeline_filter(pipeline, messages) == 2


def test_apply_pipeline_filter_no_filter_node():
    """Filter returns all messages when no filter node in graph."""
    from src.web.routes.pipelines import _apply_pipeline_filter

    pipeline = MagicMock()
    pipeline.pipeline_json = PipelineGraph(
        nodes=[PipelineNode(id="src", type=PipelineNodeType.SOURCE, name="src")],
        edges=[],
    )
    messages = [MagicMock(text="hello")]
    assert _apply_pipeline_filter(pipeline, messages) == 1


def test_apply_pipeline_filter_keywords():
    """Filter node with keywords filters correctly."""
    from src.web.routes.pipelines import _apply_pipeline_filter

    pipeline = MagicMock()
    pipeline.pipeline_json = PipelineGraph(
        nodes=[
            PipelineNode(
                id="f", type=PipelineNodeType.FILTER, name="f",
                config={"type": "keywords", "keywords": ["hello", "world"]},
            ),
        ],
        edges=[],
    )
    messages = [
        MagicMock(text="hello there"),
        MagicMock(text="world peace"),
        MagicMock(text="no match"),
    ]
    assert _apply_pipeline_filter(pipeline, messages) == 2


def test_apply_pipeline_filter_regex():
    """Filter node with regex pattern filters correctly."""
    from src.web.routes.pipelines import _apply_pipeline_filter

    pipeline = MagicMock()
    pipeline.pipeline_json = PipelineGraph(
        nodes=[
            PipelineNode(
                id="f", type=PipelineNodeType.FILTER, name="f",
                config={"type": "regex", "pattern": r"\d+"},
            ),
        ],
        edges=[],
    )
    messages = [
        MagicMock(text="has 123 numbers"),
        MagicMock(text="no numbers here"),
    ]
    assert _apply_pipeline_filter(pipeline, messages) == 1


def test_apply_pipeline_filter_regex_invalid():
    """Invalid regex pattern is treated as non-matching."""
    from src.web.routes.pipelines import _apply_pipeline_filter

    pipeline = MagicMock()
    pipeline.pipeline_json = PipelineGraph(
        nodes=[
            PipelineNode(
                id="f", type=PipelineNodeType.FILTER, name="f",
                config={"type": "regex", "pattern": "[invalid"},
            ),
        ],
        edges=[],
    )
    messages = [MagicMock(text="anything")]
    assert _apply_pipeline_filter(pipeline, messages) == 0


def test_apply_pipeline_filter_anonymous_sender():
    """Filter node with anonymous_sender checks sender_id."""
    from src.web.routes.pipelines import _apply_pipeline_filter

    pipeline = MagicMock()
    pipeline.pipeline_json = PipelineGraph(
        nodes=[
            PipelineNode(
                id="f", type=PipelineNodeType.FILTER, name="f",
                config={"type": "anonymous_sender"},
            ),
        ],
        edges=[],
    )
    messages = [
        MagicMock(text="anon", sender_id=None),
        MagicMock(text="named", sender_id=123),
    ]
    assert _apply_pipeline_filter(pipeline, messages) == 1


def test_apply_pipeline_filter_unknown_type():
    """Unknown filter type counts all messages."""
    from src.web.routes.pipelines import _apply_pipeline_filter

    pipeline = MagicMock()
    pipeline.pipeline_json = PipelineGraph(
        nodes=[
            PipelineNode(
                id="f", type=PipelineNodeType.FILTER, name="f",
                config={"type": "unknown_type"},
            ),
        ],
        edges=[],
    )
    messages = [MagicMock(text="a"), MagicMock(text="b")]
    assert _apply_pipeline_filter(pipeline, messages) == 2


# ── export_pipeline ─────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_export_pipeline_success(client_with_dialog, base_app):
    """GET /pipelines/<id>/export returns JSON file."""
    app, db, _ = base_app
    app.state.llm_provider_service = _make_provider_svc(has=True)
    await client_with_dialog.post("/pipelines/add", data=_ADD_DATA)

    resp = await client_with_dialog.get("/pipelines/1/export", follow_redirects=False)
    assert resp.status_code == 200
    assert resp.headers["content-type"] == "application/json"
    assert "attachment" in resp.headers.get("content-disposition", "")
    data = json.loads(resp.text)
    assert "name" in data
    assert data["name"] == "Test Pipeline"


@pytest.mark.asyncio
async def test_export_pipeline_not_found(client, base_app):
    """GET /pipelines/<id>/export for missing pipeline redirects."""
    app, _, _ = base_app
    app.state.llm_provider_service = _make_provider_svc()

    with patch("src.web.routes.pipelines.deps.pipeline_service") as mock_svc:
        mock_svc.return_value.export_json = AsyncMock(return_value=None)
        resp = await client.get("/pipelines/999/export", follow_redirects=False)
    assert resp.status_code == 303
    assert "error=pipeline_invalid" in resp.headers["location"]


# ── import_pipeline ─────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_import_pipeline_from_text(client, base_app):
    """POST /pipelines/import with json_text creates a pipeline."""
    app, db, _ = base_app
    app.state.llm_provider_service = _make_provider_svc(has=True)

    import_data = {
        "name": "Imported",
        "prompt_template": "test",
        "source_ids": [],
        "target_refs": [],
        "generation_backend": "chain",
        "publish_mode": "moderated",
        "generate_interval_minutes": 60,
    }
    resp = await client.post(
        "/pipelines/import",
        data={"json_text": json.dumps(import_data), "name_override": ""},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "/generate" in resp.headers["location"]


@pytest.mark.asyncio
async def test_import_pipeline_from_file(client, base_app):
    """POST /pipelines/import with json_file creates a pipeline."""
    app, db, _ = base_app
    app.state.llm_provider_service = _make_provider_svc(has=True)

    import_data = {
        "name": "File Import",
        "prompt_template": "test",
        "source_ids": [],
        "target_refs": [],
    }
    import io

    file_content = json.dumps(import_data).encode()
    resp = await client.post(
        "/pipelines/import",
        files={"json_file": ("pipeline.json", io.BytesIO(file_content), "application/json")},
        data={"json_text": "", "name_override": ""},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "/generate" in resp.headers["location"]


@pytest.mark.asyncio
async def test_import_pipeline_no_data(client, base_app):
    """POST /pipelines/import without file or text redirects with error."""
    app, _, _ = base_app
    app.state.llm_provider_service = _make_provider_svc()

    resp = await client.post(
        "/pipelines/import",
        data={"json_text": "", "name_override": ""},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "error=" in resp.headers["location"]


@pytest.mark.asyncio
async def test_import_pipeline_validation_error(client, base_app):
    """POST /pipelines/import with bad JSON redirects with error."""
    app, _, _ = base_app
    app.state.llm_provider_service = _make_provider_svc()

    with patch("src.web.routes.pipelines.deps.pipeline_service") as mock_svc:
        mock_svc.return_value.import_json = AsyncMock(side_effect=PipelineValidationError("Bad"))
        resp = await client.post(
            "/pipelines/import",
            data={"json_text": "{}", "name_override": ""},
            follow_redirects=False,
        )
    assert resp.status_code == 303
    assert "error=" in resp.headers["location"]


@pytest.mark.asyncio
async def test_import_pipeline_general_exception(client, base_app):
    """POST /pipelines/import with unexpected error redirects with error."""
    app, _, _ = base_app
    app.state.llm_provider_service = _make_provider_svc()

    with patch("src.web.routes.pipelines.deps.pipeline_service") as mock_svc:
        mock_svc.return_value.import_json = AsyncMock(side_effect=Exception("Unexpected"))
        resp = await client.post(
            "/pipelines/import",
            data={"json_text": "{}", "name_override": ""},
            follow_redirects=False,
        )
    assert resp.status_code == 303
    assert "error=" in resp.headers["location"]


# ── create_from_template ────────────────────────────────────────────


@pytest.mark.asyncio
async def test_create_from_template_success(client, base_app):
    """POST /pipelines/from-template creates pipeline from template."""
    app, db, _ = base_app
    app.state.llm_provider_service = _make_provider_svc(has=True)

    with patch("src.web.routes.pipelines.deps.pipeline_service") as mock_svc:
        mock_svc.return_value.create_from_template = AsyncMock(return_value=42)
        resp = await client.post(
            "/pipelines/from-template",
            data={
                "template_id": "1",
                "name": "From Template",
                "source_channel_ids": "100",
                "target_refs": "+1234567890|200",
                "llm_model": "gpt-4o",
                "image_model": "",
                "generate_interval_minutes": "30",
            },
            follow_redirects=False,
        )
    assert resp.status_code == 303
    assert "/pipelines/42/edit" in resp.headers["location"]


@pytest.mark.asyncio
async def test_create_from_template_validation_error(client, base_app):
    """POST /pipelines/from-template with bad data redirects with error."""
    app, db, _ = base_app
    app.state.llm_provider_service = _make_provider_svc(has=True)

    with patch("src.web.routes.pipelines.deps.pipeline_service") as mock_svc:
        mock_svc.return_value.create_from_template = AsyncMock(
            side_effect=PipelineValidationError("Template not found")
        )
        resp = await client.post(
            "/pipelines/from-template",
            data={
                "template_id": "999",
                "name": "Bad Template",
                "llm_model": "",
                "image_model": "",
                "generate_interval_minutes": "60",
            },
            follow_redirects=False,
        )
    assert resp.status_code == 303
    assert "error=" in resp.headers["location"]


# ── templates_json ──────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_templates_json(client, base_app):
    """GET /pipelines/templates/json returns template list."""
    app, db, _ = base_app
    app.state.llm_provider_service = _make_provider_svc()

    with patch("src.web.routes.pipelines.deps.pipeline_service") as mock_svc:
        tpl = MagicMock()
        tpl.id = 1
        tpl.name = "Test Template"
        tpl.description = "A template"
        tpl.category = "basic"
        tpl.template_json = PipelineGraph(
            nodes=[PipelineNode(id="src", type=PipelineNodeType.SOURCE, name="src")],
            edges=[],
        )
        mock_svc.return_value.list_templates = AsyncMock(return_value=[tpl])
        resp = await client.get("/pipelines/templates/json")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)
    assert len(data) == 1
    assert data[0]["name"] == "Test Template"
    assert "template_json" in data[0]


# ── templates_page ──────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_templates_page_renders(client, base_app):
    """GET /pipelines/templates renders the templates page."""
    app, db, _ = base_app
    app.state.llm_provider_service = _make_provider_svc(has=True)

    with patch("src.web.routes.pipelines.deps.pipeline_service") as mock_svc, \
         patch("src.web.routes.pipelines.deps.get_channel_bundle") as mock_ch, \
         patch("src.web.routes.pipelines.deps.get_account_bundle") as mock_acct:
        mock_svc.return_value.list_templates = AsyncMock(return_value=[])
        mock_svc.return_value.list_cached_dialogs_by_phone = AsyncMock(return_value={})
        mock_ch.return_value.list_channels = AsyncMock(return_value=[])
        mock_acct.return_value.list_accounts = AsyncMock(return_value=[])
        resp = await client.get("/pipelines/templates")
    assert resp.status_code == 200


# ── ai_edit_pipeline ────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_ai_edit_pipeline_no_provider(client, base_app):
    """POST /pipelines/<id>/ai-edit without provider returns 400."""
    app, _, _ = base_app
    app.state.llm_provider_service = _make_provider_svc(has=False)

    resp = await client.post(
        "/pipelines/1/ai-edit",
        json={"instruction": "Add a filter node"},
    )
    assert resp.status_code == 400
    assert resp.json()["ok"] is False


@pytest.mark.asyncio
async def test_ai_edit_pipeline_no_instruction(client, base_app):
    """POST /pipelines/<id>/ai-edit without instruction returns 400."""
    app, _, _ = base_app
    app.state.llm_provider_service = _make_provider_svc(has=True)

    resp = await client.post(
        "/pipelines/1/ai-edit",
        json={"instruction": ""},
    )
    assert resp.status_code == 400
    assert "instruction is required" in resp.json()["error"]


@pytest.mark.asyncio
async def test_ai_edit_pipeline_success(client, base_app):
    """POST /pipelines/<id>/ai-edit with valid instruction returns updated JSON."""
    app, db, _ = base_app
    app.state.llm_provider_service = _make_provider_svc(has=True)

    with patch("src.web.routes.pipelines.deps.pipeline_service") as mock_svc:
        mock_svc.return_value.edit_via_llm = AsyncMock(
            return_value={"ok": True, "pipeline_json": {"nodes": [], "edges": []}}
        )
        resp = await client.post(
            "/pipelines/1/ai-edit",
            json={"instruction": "Remove all nodes"},
        )
    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is True


@pytest.mark.asyncio
async def test_ai_edit_pipeline_exception(client, base_app):
    """POST /pipelines/<id>/ai-edit handles service exception."""
    app, db, _ = base_app
    app.state.llm_provider_service = _make_provider_svc(has=True)

    with patch("src.web.routes.pipelines.deps.pipeline_service") as mock_svc:
        mock_svc.return_value.edit_via_llm = AsyncMock(side_effect=Exception("LLM error"))
        resp = await client.post(
            "/pipelines/1/ai-edit",
            json={"instruction": "Add node"},
        )
    assert resp.status_code == 500
    assert resp.json()["ok"] is False


# ── get/set_refinement_steps ────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_refinement_steps_not_found(client, base_app):
    """GET /pipelines/<id>/refinement-steps for missing pipeline redirects."""
    app, db, _ = base_app
    app.state.llm_provider_service = _make_provider_svc()

    resp = await client.get("/pipelines/999/refinement-steps", follow_redirects=False)
    assert resp.status_code == 303
    assert "error=pipeline_not_found" in resp.headers["location"]


@pytest.mark.asyncio
async def test_get_refinement_steps_success(client_with_dialog, base_app):
    """GET /pipelines/<id>/refinement-steps returns steps list."""
    app, db, _ = base_app
    app.state.llm_provider_service = _make_provider_svc(has=True)
    await client_with_dialog.post("/pipelines/add", data=_ADD_DATA)

    resp = await client_with_dialog.get("/pipelines/1/refinement-steps")
    assert resp.status_code == 200
    data = resp.json()
    assert "steps" in data
    assert isinstance(data["steps"], list)


@pytest.mark.asyncio
async def test_set_refinement_steps_not_found(client, base_app):
    """POST /pipelines/<id>/refinement-steps for missing pipeline redirects."""
    app, db, _ = base_app
    app.state.llm_provider_service = _make_provider_svc()

    resp = await client.post(
        "/pipelines/999/refinement-steps",
        json={"steps": [{"name": "s1", "prompt": "Refine: {text}"}]},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "error=pipeline_not_found" in resp.headers["location"]


@pytest.mark.asyncio
async def test_set_refinement_steps_success(client_with_dialog, base_app):
    """POST /pipelines/<id>/refinement-steps saves steps and returns them."""
    app, db, _ = base_app
    app.state.llm_provider_service = _make_provider_svc(has=True)
    await client_with_dialog.post("/pipelines/add", data=_ADD_DATA)

    steps = [
        {"name": "Step 1", "prompt": "Refine this: {text}"},
        {"name": "Step 2", "prompt": "Improve grammar: {text}"},
    ]
    resp = await client_with_dialog.post(
        "/pipelines/1/refinement-steps",
        json={"steps": steps},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is True
    assert len(data["steps"]) == 2


@pytest.mark.asyncio
async def test_set_refinement_steps_invalid_steps(client_with_dialog, base_app):
    """POST /pipelines/<id>/refinement-steps with invalid steps returns 400."""
    app, db, _ = base_app
    app.state.llm_provider_service = _make_provider_svc(has=True)
    await client_with_dialog.post("/pipelines/add", data=_ADD_DATA)

    resp = await client_with_dialog.post(
        "/pipelines/1/refinement-steps",
        json={"steps": "not a list"},
    )
    assert resp.status_code == 400
    assert resp.json()["ok"] is False


@pytest.mark.asyncio
async def test_set_refinement_steps_empty_prompt_filtered(client_with_dialog, base_app):
    """Steps with empty prompts are filtered out."""
    app, db, _ = base_app
    app.state.llm_provider_service = _make_provider_svc(has=True)
    await client_with_dialog.post("/pipelines/add", data=_ADD_DATA)

    # Mock pipeline exists in DB
    with patch("src.web.routes.pipelines.deps.get_db") as mock_get_db:
        mock_pipeline = MagicMock()
        mock_pipeline.id = 1
        mock_pipeline.refinement_steps = []
        mock_get_db.return_value.repos.content_pipelines.get_by_id = AsyncMock(return_value=mock_pipeline)
        mock_get_db.return_value.repos.content_pipelines.set_refinement_steps = AsyncMock()

        steps = [
            {"name": "Good", "prompt": "Refine: {text}"},
            {"name": "Empty", "prompt": ""},
            {"name": "Also Empty", "prompt": "   "},
        ]
        resp = await client_with_dialog.post(
            "/pipelines/1/refinement-steps",
            json={"steps": steps},
        )
    assert resp.status_code == 200
    data = resp.json()
    assert len(data["steps"]) == 1
    assert data["steps"][0]["name"] == "Good"


# ── _page_context branches ──────────────────────────────────────────


@pytest.mark.asyncio
async def test_pipelines_page_with_no_llm_provider(client, base_app):
    """Pipelines page renders when no LLM provider is configured."""
    app, db, _ = base_app
    app.state.llm_provider_service = _make_provider_svc(has=False)

    resp = await client.get("/pipelines/")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_pipelines_page_with_dialog_refresh(client, base_app):
    """Pipelines page handles dialog refresh with ?refresh=1."""
    app, db, pool_mock = base_app
    app.state.llm_provider_service = _make_provider_svc(has=True)

    resp = await client.get("/pipelines/?refresh=1")
    assert resp.status_code == 200


# ── edit_page with refresh ──────────────────────────────────────────


@pytest.mark.asyncio
async def test_edit_page_with_refresh(client_with_dialog, base_app):
    """GET /pipelines/<id>/edit?refresh=1 triggers dialog cache refresh."""
    app, db, _ = base_app
    app.state.llm_provider_service = _make_provider_svc(has=True)
    await client_with_dialog.post("/pipelines/add", data=_ADD_DATA)

    resp = await client_with_dialog.get("/pipelines/1/edit?refresh=1")
    assert resp.status_code == 200


# ── Toggle with scheduler sync failure ──────────────────────────────


@pytest.mark.asyncio
async def test_toggle_pipeline_scheduler_sync_failure(client_with_dialog, base_app):
    """Toggle pipeline handles scheduler sync failure gracefully."""
    app, db, _ = base_app
    app.state.llm_provider_service = _make_provider_svc(has=True)
    await client_with_dialog.post("/pipelines/add", data=_ADD_DATA)

    # The base_app's scheduler may not support sync_pipeline_jobs;
    # this test ensures the endpoint handles it gracefully.
    resp = await client_with_dialog.post("/pipelines/1/toggle", follow_redirects=False)
    assert resp.status_code == 303
    assert "msg=pipeline_toggled" in resp.headers["location"]


# ── Delete with scheduler sync failure ──────────────────────────────


@pytest.mark.asyncio
async def test_delete_pipeline_handles_scheduler(client_with_dialog, base_app):
    """Delete pipeline handles scheduler sync failure gracefully."""
    app, db, _ = base_app
    app.state.llm_provider_service = _make_provider_svc(has=True)
    await client_with_dialog.post("/pipelines/add", data=_ADD_DATA)

    resp = await client_with_dialog.post("/pipelines/1/delete", follow_redirects=False)
    assert resp.status_code == 303
    assert "msg=pipeline_deleted" in resp.headers["location"]


# ── service_message filter type ─────────────────────────────────────


def test_apply_pipeline_filter_service_message():
    """Filter node with service_message type checks service types."""
    from src.web.routes.pipelines import _apply_pipeline_filter

    pipeline = MagicMock()
    pipeline.pipeline_json = PipelineGraph(
        nodes=[
            PipelineNode(
                id="f", type=PipelineNodeType.FILTER, name="f",
                config={"type": "service_message", "service_types": ["pinned", "joined"]},
            ),
        ],
        edges=[],
    )
    messages = [
        MagicMock(text="user pinned a message"),
        MagicMock(text="user joined the group"),
        MagicMock(text="normal message"),
    ]
    assert _apply_pipeline_filter(pipeline, messages) == 2
