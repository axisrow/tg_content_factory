"""Tests for pipelines routes."""
from __future__ import annotations

import base64
import re
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from src.models import Account

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


@pytest.fixture
async def client(base_app):
    app, db, pool_mock = base_app

    pool_mock.clients = {"+1234567890": MagicMock()}
    pool_mock.get_dialogs_for_phone = AsyncMock(return_value=[])
    await db.repos.dialog_cache.replace_dialogs(
        "+1234567890",
        [{"channel_id": 200, "title": "Test Dialog", "channel_type": "channel"}],
    )

    transport = ASGITransport(app=app)
    auth_header = base64.b64encode(b":testpass").decode()
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        follow_redirects=True,
        headers={"Authorization": f"Basic {auth_header}", "Origin": "http://test"},
    ) as c:
        yield c



@pytest.mark.anyio
async def test_pipelines_page_renders(client):
    resp = await client.get("/pipelines/")
    assert resp.status_code == 200
    assert "Пайплайны" in resp.text
    # Lazyload (#947): the skeleton defers the heavy list to the fragment.
    assert "/pipelines/fragments/list" in resp.text
    assert 'hx-trigger="load"' in resp.text


@pytest.mark.anyio
async def test_add_pipeline_warns_when_no_llm_provider(client):
    """LLM-requiring pipeline + no provider ⇒ pipeline_added_no_llm warning."""
    resp = await client.post(
        "/pipelines/add",
        data=_ADD_DATA,
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "msg=pipeline_added_no_llm" in resp.headers["location"]


@pytest.mark.anyio
async def test_add_pipeline_with_provider(client):
    """With provider registered, redirect shows plain pipeline_added."""
    mock_provider_instance = MagicMock()
    mock_provider_instance.has_providers = MagicMock(return_value=True)
    app = client._transport.app  # type: ignore
    app.state.llm_provider_service = mock_provider_instance

    resp = await client.post(
        "/pipelines/add",
        data=_ADD_DATA,
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "msg=pipeline_added" in resp.headers["location"]
    assert "pipeline_added_no_llm" not in resp.headers["location"]


@pytest.mark.anyio
async def test_pipelines_page_lists_pipeline(client):
    await client.post(
        "/pipelines/add",
        data={**_ADD_DATA, "name": "Listed Pipeline"},
    )
    resp = await client.get("/pipelines/fragments/list")
    assert resp.status_code == 200
    assert "Listed Pipeline" in resp.text


@pytest.mark.anyio
async def test_pipelines_page_shows_pipeline_id(client):
    await client.post(
        "/pipelines/add",
        data={**_ADD_DATA, "name": "Pipeline With Id"},
    )
    resp = await client.get("/pipelines/fragments/list")
    assert resp.status_code == 200
    assert "ID: 1" in resp.text


@pytest.mark.anyio
async def test_toggle_pipeline(client):
    await client.post("/pipelines/add", data=_ADD_DATA)
    resp = await client.post("/pipelines/1/toggle", follow_redirects=False)
    assert resp.status_code == 303
    assert "msg=pipeline_toggled" in resp.headers["location"]


@pytest.mark.anyio
async def test_delete_pipeline(client):
    await client.post("/pipelines/add", data=_ADD_DATA)
    resp = await client.post("/pipelines/1/delete", follow_redirects=False)
    assert resp.status_code == 303
    assert "msg=pipeline_deleted" in resp.headers["location"]


@pytest.mark.anyio
async def test_edit_pipeline(client):
    await client.post("/pipelines/add", data=_ADD_DATA)
    resp = await client.post(
        "/pipelines/1/edit",
        data={**_ADD_DATA, "name": "Edited", "publish_mode": "auto", "llm_model": "gpt-4o"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "msg=pipeline_edited" in resp.headers["location"]


@pytest.mark.anyio
async def test_pipeline_edit_page_loads(client):
    """GET /pipelines/<id>/edit returns 200 with edit form."""
    await client.post("/pipelines/add", data=_ADD_DATA)
    resp = await client.get("/pipelines/1/edit")
    assert resp.status_code == 200
    assert "Редактировать" in resp.text


@pytest.mark.anyio
async def test_pipeline_pages_render_with_encrypted_account_missing_key(client):
    """Read-only pipeline pages must not decrypt account sessions to render."""
    await client.post("/pipelines/add", data=_ADD_DATA)

    app = client._transport.app  # type: ignore[attr-defined]
    await app.state.db.add_account(
        Account(
            phone="+19999999999",
            session_string="enc:v2:not-a-valid-token",
            is_active=True,
        )
    )

    for path in ("/pipelines/", "/pipelines/create", "/pipelines/templates", "/pipelines/1/edit"):
        resp = await client.get(path)
        assert resp.status_code == 200, path
        assert "Ошибка сервера" not in resp.text


@pytest.mark.anyio
async def test_pipeline_edit_page_not_found(client):
    """GET /pipelines/<id>/edit redirects for invalid ID."""
    resp = await client.get("/pipelines/999999/edit", follow_redirects=False)
    assert resp.status_code == 303
    assert "error=pipeline_invalid" in resp.headers["location"]


# === New tests ===


@pytest.mark.anyio
async def test_run_pipeline_not_found(client):
    """Test run pipeline with invalid ID."""
    from unittest.mock import patch

    with patch("src.services.provider_service.RuntimeProviderRegistry.has_providers", return_value=True):
        resp = await client.post("/pipelines/999999/run", follow_redirects=False)
    assert resp.status_code == 303
    assert "error=pipeline_invalid" in resp.headers["location"]


@pytest.mark.anyio
async def test_run_pipeline_enqueues(client):
    """Test run pipeline enqueues generation."""
    from unittest.mock import patch

    await client.post("/pipelines/add", data=_ADD_DATA)

    with patch("src.services.provider_service.RuntimeProviderRegistry.has_providers", return_value=True):
        with patch("src.web.pipelines.handlers.deps.pipeline_service") as mock_svc:
            mock_svc.return_value.get = AsyncMock(
                return_value=MagicMock(id=1, is_active=True)
            )
            with patch("src.web.pipelines.handlers.deps.get_task_enqueuer") as mock_enq:
                mock_enq.return_value.enqueue_pipeline_run = AsyncMock()
                resp = await client.post("/pipelines/1/run", follow_redirects=False)
                assert resp.status_code == 303
                assert "msg=pipeline_run_enqueued" in resp.headers["location"]


@pytest.mark.anyio
async def test_run_pipeline_blocked_when_needs_llm_and_no_provider(client):
    """Default chain pipeline needs LLM: without provider, run is blocked."""
    await client.post("/pipelines/add", data=_ADD_DATA)
    # base_app fixture sets up ProviderConfigService() with no providers.
    resp = await client.post("/pipelines/1/run", follow_redirects=False)
    assert resp.status_code == 303
    assert "error=llm_not_configured" in resp.headers["location"]


@pytest.mark.anyio
async def test_run_pipeline_allowed_for_non_llm_dag_without_provider(client):
    """A DAG with only SOURCE→PUBLISH nodes runs without an LLM provider."""
    from unittest.mock import patch

    from src.models import (
        PipelineEdge,
        PipelineGraph,
        PipelineNode,
        PipelineNodeType,
    )

    await client.post("/pipelines/add", data=_ADD_DATA)

    non_llm_pipeline = MagicMock(
        id=1,
        is_active=True,
        pipeline_json=PipelineGraph(
            nodes=[
                PipelineNode(id="src", type=PipelineNodeType.SOURCE, name="src"),
                PipelineNode(id="pub", type=PipelineNodeType.PUBLISH, name="pub"),
            ],
            edges=[PipelineEdge(from_node="src", to_node="pub")],
        ),
    )
    # MagicMock's default mocks generation_backend; force it to the real CHAIN value
    # so pipeline_needs_llm short-circuits on pipeline_json inspection instead.
    from src.models import PipelineGenerationBackend

    non_llm_pipeline.generation_backend = PipelineGenerationBackend.CHAIN

    with patch("src.web.pipelines.handlers.deps.pipeline_service") as mock_svc:
        mock_svc.return_value.get = AsyncMock(return_value=non_llm_pipeline)
        with patch("src.web.pipelines.handlers.deps.get_task_enqueuer") as mock_enq:
            mock_enq.return_value.enqueue_pipeline_run = AsyncMock()
            resp = await client.post("/pipelines/1/run", follow_redirects=False)
            assert resp.status_code == 303
            # Not blocked despite no provider registered in base_app.
            assert "msg=pipeline_run_enqueued" in resp.headers["location"]


@pytest.mark.anyio
async def test_run_pipeline_failure(client):
    """Test run pipeline handles failure."""
    from unittest.mock import patch

    await client.post("/pipelines/add", data=_ADD_DATA)

    with patch("src.services.provider_service.RuntimeProviderRegistry.has_providers", return_value=True):
        with patch("src.web.pipelines.handlers.deps.pipeline_service") as mock_svc:
            mock_svc.return_value.get = AsyncMock(
                return_value=MagicMock(id=1, is_active=True)
            )
            with patch("src.web.pipelines.handlers.deps.get_task_enqueuer") as mock_enq:
                mock_enq.return_value.enqueue_pipeline_run = AsyncMock(
                    side_effect=Exception("Queue error")
                )
                resp = await client.post("/pipelines/1/run", follow_redirects=False)
                assert resp.status_code == 303
                assert "error=pipeline_run_failed" in resp.headers["location"]


@pytest.mark.anyio
async def test_generate_page_renders(client):
    """Test generate page renders."""
    await client.post("/pipelines/add", data=_ADD_DATA)

    resp = await client.get("/pipelines/1/generate")
    assert resp.status_code == 200


@pytest.mark.anyio
async def test_generate_page_warns_when_llm_pipeline_has_no_provider(client):
    """#386: an LLM-requiring pipeline with no provider configured must show a
    warning banner and disable the preview controls (instead of failing only on
    click). The _ADD_DATA pipeline uses the legacy chain backend ⇒ needs_llm, and
    the test fixture has no LLM provider registered."""
    await client.post("/pipelines/add", data=_ADD_DATA)

    resp = await client.get("/pipelines/1/generate")
    assert resp.status_code == 200
    # LLM fields are still rendered (it IS an LLM pipeline) but gated:
    assert 'name="model"' in resp.text
    assert "ни один провайдер не настроен" in resp.text
    assert "disabled" in resp.text
    # The warning says "Превью и запуск недоступны" — the "Run now" button must
    # actually be disabled to match it, not just Preview/Stream (#735 review).
    assert re.search(r'btn btn-success[^>]*disabled[^>]*>\s*<i class="bi bi-play-fill"', resp.text)


@pytest.mark.anyio
async def test_generate_page_not_found(client):
    """Test generate page with invalid pipeline."""
    resp = await client.get("/pipelines/999999/generate", follow_redirects=False)
    assert resp.status_code == 303
    assert "error=pipeline_invalid" in resp.headers["location"]


# === SSE Streaming tests ===


@pytest.mark.anyio
async def test_generate_stream_success(client):
    """Test SSE streaming generation."""
    from unittest.mock import patch

    await client.post("/pipelines/add", data=_ADD_DATA)

    async def fake_stream(*args, **kwargs):
        yield {"delta": "Hello", "generated_text": None, "citations": []}
        yield {"delta": " world", "generated_text": "Hello world", "citations": []}

    mock_provider_instance = MagicMock()
    mock_provider_instance.has_providers = MagicMock(return_value=True)
    mock_provider_instance.get_provider_callable = MagicMock(return_value=lambda: None)

    app = client._transport.app  # type: ignore
    app.state.llm_provider_service = mock_provider_instance

    with patch("src.services.generation_service.GenerationService") as mock_gen:
        mock_instance = MagicMock()
        mock_instance.generate_stream = fake_stream
        mock_gen.return_value = mock_instance

        resp = await client.get("/pipelines/1/generate-stream")
        assert resp.status_code == 200
        assert "text/event-stream" in resp.headers["content-type"]


@pytest.mark.anyio
async def test_generate_stream_midstream_break_marks_run_failed(client):
    """A provider that drops mid-stream must NOT persist a 'completed' run.

    Regression for issue #1034 (cycle-review): generate_stream now returns partial
    text gracefully (partial/stream_error flags) instead of raising. The SSE handler
    must honor those flags and persist the run as 'failed' with the error recorded —
    otherwise a truncated generation is silently saved as a successful completed run.
    Exercises the REAL GenerationService (provider fails after one chunk), not a mock.
    """
    await client.post("/pipelines/add", data=_ADD_DATA)

    async def failing_provider(prompt="", **kwargs):
        async def _gen():
            yield "partial text "
            raise ConnectionError("provider dropped mid-stream")

        return _gen()

    mock_provider_instance = MagicMock()
    mock_provider_instance.has_providers = MagicMock(return_value=True)
    mock_provider_instance.get_provider_callable = MagicMock(return_value=failing_provider)

    app = client._transport.app  # type: ignore
    app.state.llm_provider_service = mock_provider_instance
    db = app.state.db

    resp = await client.get("/pipelines/1/generate-stream")
    assert resp.status_code == 200
    # The interrupted stream surfaces an error event, not a done event.
    assert "event: error" in resp.text
    assert "event: done" not in resp.text

    # The run must be persisted as failed, not completed (the core regression).
    runs = await db.repos.generation_runs.list_by_pipeline(1)
    assert runs, "a generation run should have been created"
    latest = runs[0]
    assert latest.status == "failed"
    assert latest.status != "completed"


@pytest.mark.anyio
async def test_generate_stream_pipeline_not_found(client):
    """Test SSE streaming with invalid pipeline."""
    resp = await client.get("/pipelines/999999/generate-stream", follow_redirects=False)
    assert resp.status_code == 303
    assert "error=pipeline_invalid" in resp.headers["location"]


@pytest.mark.anyio
async def test_generate_stream_scope_error_fails_closed(client):
    """Fail-closed (#1077): a source-scope resolution failure must abort the run
    with a clean error redirect — NOT a 500 and NOT a stream that silently runs
    unscoped (cross-channel) retrieval. A transient source-lookup blip must never
    widen a single-source pipeline to all channels."""
    from unittest.mock import patch

    from src.services.pipeline_service import PipelineScopeError

    await client.post("/pipelines/add", data=_ADD_DATA)

    mock_provider_instance = MagicMock()
    mock_provider_instance.has_providers = MagicMock(return_value=True)
    mock_provider_instance.get_provider_callable = MagicMock(return_value=lambda: None)
    app = client._transport.app  # type: ignore
    app.state.llm_provider_service = mock_provider_instance
    db = app.state.db

    async def boom(_pipeline):
        raise PipelineScopeError("source lookup failed")

    with patch(
        "src.services.pipeline_service.PipelineService.get_retrieval_scope",
        side_effect=boom,
    ):
        resp = await client.get("/pipelines/1/generate-stream", follow_redirects=False)

    # Clean fail-closed redirect, not a 200 stream and not a 500.
    assert resp.status_code == 303
    assert "error=" in resp.headers["location"]
    # No run should have been created — the abort happens before persistence.
    runs = await db.repos.generation_runs.list_by_pipeline(1)
    assert runs == []


@pytest.mark.anyio
async def test_generate_pipeline_success(client):
    """Test non-streaming generation success."""
    from unittest.mock import patch

    await client.post("/pipelines/add", data=_ADD_DATA)

    mock_provider_instance = MagicMock()
    mock_provider_instance.has_providers = MagicMock(return_value=True)
    mock_provider_instance.get_provider_callable = MagicMock(return_value=lambda: None)

    app = client._transport.app  # type: ignore
    app.state.llm_provider_service = mock_provider_instance

    with patch("src.services.generation_service.GenerationService") as mock_gen:
        mock_instance = MagicMock()
        mock_instance.generate = AsyncMock(
            return_value={"generated_text": "Test output", "citations": []}
        )
        mock_gen.return_value = mock_instance

        resp = await client.post(
            "/pipelines/1/generate",
            data={"model": "", "max_tokens": "256", "temperature": "0.0"},
        )
        assert resp.status_code == 200


@pytest.mark.anyio
async def test_generate_pipeline_failure(client):
    """Test non-streaming generation failure."""
    from unittest.mock import patch

    await client.post("/pipelines/add", data=_ADD_DATA)

    mock_provider_instance = MagicMock()
    mock_provider_instance.has_providers = MagicMock(return_value=True)
    mock_provider_instance.get_provider_callable = MagicMock(return_value=lambda: None)

    app = client._transport.app  # type: ignore
    app.state.llm_provider_service = mock_provider_instance

    with patch("src.services.generation_service.GenerationService") as mock_gen:
        mock_instance = MagicMock()
        mock_instance.generate = AsyncMock(side_effect=Exception("Generation error"))
        mock_gen.return_value = mock_instance

        resp = await client.post(
            "/pipelines/1/generate",
            data={"model": "", "max_tokens": "256", "temperature": "0.0"},
        )
        assert resp.status_code == 200
        assert "Generation failed" in resp.text


@pytest.mark.anyio
async def test_generate_pipeline_error_render_hides_llm_fields_for_non_llm(client):
    """#735 review (Bug 2): on a POST that re-renders generate.html, the handler must
    propagate needs_llm/llm_configured. For a non-LLM pipeline the LLM model field must
    stay hidden instead of defaulting to shown."""
    from unittest.mock import patch

    from src.models import (
        PipelineEdge,
        PipelineGenerationBackend,
        PipelineGraph,
        PipelineNode,
        PipelineNodeType,
    )

    await client.post("/pipelines/add", data=_ADD_DATA)

    non_llm_pipeline = MagicMock(
        id=1,
        name="non-llm",
        is_active=True,
        pipeline_json=PipelineGraph(
            nodes=[
                PipelineNode(id="src", type=PipelineNodeType.SOURCE, name="src"),
                PipelineNode(id="pub", type=PipelineNodeType.PUBLISH, name="pub"),
            ],
            edges=[PipelineEdge(from_node="src", to_node="pub")],
        ),
    )
    non_llm_pipeline.generation_backend = PipelineGenerationBackend.CHAIN

    with patch("src.web.pipelines.handlers.deps.pipeline_service") as mock_svc:
        mock_svc.return_value.get = AsyncMock(return_value=non_llm_pipeline)
        with patch(
            "src.services.content_generation_service.ContentGenerationService"
        ) as mock_gen:
            mock_gen.return_value.generate = AsyncMock(side_effect=Exception("boom"))
            resp = await client.post(
                "/pipelines/1/generate",
                data={"model": "", "max_tokens": "256", "temperature": "0.0"},
            )
            assert resp.status_code == 200
            assert "Generation failed" in resp.text
            # non-LLM pipeline ⇒ the LLM parameter block (model/max_tokens/temperature
            # inputs + Preview button) must not be rendered. The "model" string still
            # appears in the always-present <script>, so match the unique LLM markers.
            assert ">Max tokens<" not in resp.text
            assert ">Temperature<" not in resp.text
            assert "не использует LLM" in resp.text


@pytest.mark.anyio
async def test_generate_pipeline_not_found(client):
    """Test non-streaming generation with invalid pipeline."""
    resp = await client.post(
        "/pipelines/999999/generate",
        data={"model": "", "max_tokens": "256", "temperature": "0.0"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "error=pipeline_invalid" in resp.headers["location"]


@pytest.mark.anyio
async def test_publish_pipeline_success(client):
    """Test publishing a generation run."""
    await client.post("/pipelines/add", data=_ADD_DATA)

    # Get db from app state through transport
    app = client._transport.app  # type: ignore
    db = app.state.db

    # Create a generation run
    run_id = await db.repos.generation_runs.create_run(1, "prompt")
    await db.repos.generation_runs.save_result(run_id, "Generated text", {})

    resp = await client.post(
        "/pipelines/1/publish",
        data={"run_id": str(run_id)},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "msg=pipeline_published" in resp.headers["location"]


@pytest.mark.anyio
async def test_publish_pipeline_invalid_run(client):
    """Test publishing with wrong pipeline_id."""
    await client.post("/pipelines/add", data=_ADD_DATA)

    # Get db from app state through transport
    app = client._transport.app  # type: ignore
    db = app.state.db

    # Create a generation run
    run_id = await db.repos.generation_runs.create_run(1, "prompt")
    await db.repos.generation_runs.save_result(run_id, "Generated text", {})

    # Try to publish with wrong pipeline_id
    resp = await client.post(
        "/pipelines/999999/publish",
        data={"run_id": str(run_id)},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "error=pipeline_invalid" in resp.headers["location"]


@pytest.mark.anyio
async def test_publish_pipeline_run_not_found(client):
    """Test publishing a non-existent run."""
    await client.post("/pipelines/add", data=_ADD_DATA)

    resp = await client.post(
        "/pipelines/1/publish",
        data={"run_id": "999999"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "error=pipeline_invalid" in resp.headers["location"]


@pytest.mark.anyio
async def test_edit_pipeline_not_found(client):
    """Test edit with invalid pipeline_id."""
    resp = await client.post(
        "/pipelines/999999/edit",
        data=_ADD_DATA,
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "error=pipeline_invalid" in resp.headers["location"]


@pytest.mark.anyio
async def test_toggle_pipeline_not_found(client):
    """Test toggle with invalid pipeline_id."""
    resp = await client.post("/pipelines/999999/toggle", follow_redirects=False)
    assert resp.status_code == 303
    assert "error=pipeline_invalid" in resp.headers["location"]


# === read-only JSON GET routes (parity: pipeline show/runs/run-show/queue) ===


@pytest.mark.anyio
async def test_show_pipeline_json(client):
    await client.post("/pipelines/add", data=_ADD_DATA)
    resp = await client.get("/pipelines/1/show")
    assert resp.status_code == 200
    data = resp.json()
    assert data["id"] == 1
    assert data["name"] == "Test Pipeline"
    assert "source_ids" in data


@pytest.mark.anyio
async def test_show_pipeline_not_found(client):
    resp = await client.get("/pipelines/999/show")
    assert resp.status_code == 404


@pytest.mark.anyio
async def test_list_pipeline_runs_json(client):
    await client.post("/pipelines/add", data=_ADD_DATA)
    app = client._transport.app  # type: ignore
    db = app.state.db
    run_id = await db.repos.generation_runs.create_run(1, "prompt")
    await db.repos.generation_runs.save_result(run_id, "txt", {})

    resp = await client.get("/pipelines/1/runs")
    assert resp.status_code == 200
    data = resp.json()
    assert data["pipeline_id"] == 1
    assert any(r["id"] == run_id for r in data["runs"])


@pytest.mark.anyio
async def test_pipeline_run_and_queue_reads_do_not_hydrate_pipeline_service(client):
    await client.post("/pipelines/add", data=_ADD_DATA)

    with patch("src.web.pipelines.handlers.deps.pipeline_service", side_effect=AssertionError("hydrated")):
        runs_resp = await client.get("/pipelines/1/runs")
        queue_resp = await client.get("/pipelines/1/queue")

    assert runs_resp.status_code == 200
    assert queue_resp.status_code == 200


@pytest.mark.anyio
async def test_list_pipeline_runs_filters_by_status(client):
    await client.post("/pipelines/add", data=_ADD_DATA)
    app = client._transport.app  # type: ignore
    db = app.state.db
    completed_id = await db.repos.generation_runs.create_run(1, "prompt completed")
    await db.repos.generation_runs.save_result(completed_id, "done", {})
    failed_id = await db.repos.generation_runs.create_run(1, "prompt failed")
    await db.repos.generation_runs.set_status(failed_id, "failed")

    resp = await client.get("/pipelines/1/runs?status=failed")

    assert resp.status_code == 200
    run_ids = [r["id"] for r in resp.json()["runs"]]
    assert run_ids == [failed_id]


@pytest.mark.anyio
async def test_list_pipeline_runs_filters_by_moderation_status(client):
    await client.post("/pipelines/add", data=_ADD_DATA)
    app = client._transport.app  # type: ignore
    db = app.state.db
    approved_id = await db.repos.generation_runs.create_run(1, "prompt approved")
    await db.repos.generation_runs.set_moderation_status(approved_id, "approved")
    rejected_id = await db.repos.generation_runs.create_run(1, "prompt rejected")
    await db.repos.generation_runs.set_moderation_status(rejected_id, "rejected")

    resp = await client.get("/pipelines/1/runs?moderation_status=approved")

    assert resp.status_code == 200
    run_ids = [r["id"] for r in resp.json()["runs"]]
    assert run_ids == [approved_id]


@pytest.mark.anyio
async def test_show_pipeline_run_json(client):
    await client.post("/pipelines/add", data=_ADD_DATA)
    app = client._transport.app  # type: ignore
    db = app.state.db
    run_id = await db.repos.generation_runs.create_run(1, "prompt")
    await db.repos.generation_runs.save_result(run_id, "hello run", {})

    resp = await client.get(f"/pipelines/1/runs/{run_id}")
    assert resp.status_code == 200
    data = resp.json()
    assert data["id"] == run_id
    assert data["generated_text"] == "hello run"


@pytest.mark.anyio
async def test_show_pipeline_run_wrong_pipeline(client):
    await client.post("/pipelines/add", data=_ADD_DATA)
    app = client._transport.app  # type: ignore
    db = app.state.db
    run_id = await db.repos.generation_runs.create_run(1, "prompt")

    resp = await client.get(f"/pipelines/2/runs/{run_id}")
    assert resp.status_code == 404


@pytest.mark.anyio
async def test_pipeline_queue_json(client):
    await client.post("/pipelines/add", data=_ADD_DATA)
    resp = await client.get("/pipelines/1/queue")
    assert resp.status_code == 200
    data = resp.json()
    assert data["pipeline_id"] == 1
    assert isinstance(data["queue"], list)


# === _target_refs error paths ===


def test_target_refs_missing_separator():
    """Test _target_refs with missing separator."""
    from src.services.pipeline_service import PipelineValidationError
    from src.web.pipelines.forms import parse_target_refs as _target_refs

    try:
        _target_refs(["invalid_format"])
        assert False, "Should have raised PipelineValidationError"
    except PipelineValidationError as e:
        assert "Некорректный формат цели" in str(e)


def test_target_refs_invalid_dialog_id():
    """Test _target_refs with invalid dialog_id."""
    from src.services.pipeline_service import PipelineValidationError
    from src.web.pipelines.forms import parse_target_refs as _target_refs

    try:
        _target_refs(["+1234567890|not_a_number"])
        assert False, "Should have raised PipelineValidationError"
    except PipelineValidationError as e:
        assert "Некорректный dialog id" in str(e)


def test_target_refs_success():
    """Test _target_refs with valid input."""
    from src.web.pipelines.forms import parse_target_refs as _target_refs

    refs = _target_refs(["+1234567890|100", "+0987654321|200"])
    assert len(refs) == 2
    assert refs[0].phone == "+1234567890"
    assert refs[0].dialog_id == 100
    assert refs[1].phone == "+0987654321"
    assert refs[1].dialog_id == 200
