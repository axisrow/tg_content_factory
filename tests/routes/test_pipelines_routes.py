"""Tests for pipelines routes."""
from __future__ import annotations

import base64
from unittest.mock import AsyncMock, MagicMock

import pytest
from httpx import ASGITransport, AsyncClient

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



@pytest.mark.asyncio
async def test_pipelines_page_renders(client):
    resp = await client.get("/pipelines/")
    assert resp.status_code == 200
    assert "Пайплайны" in resp.text


@pytest.mark.asyncio
async def test_add_pipeline(client):
    resp = await client.post(
        "/pipelines/add",
        data=_ADD_DATA,
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "msg=pipeline_added" in resp.headers["location"]


@pytest.mark.asyncio
async def test_pipelines_page_lists_pipeline(client):
    await client.post(
        "/pipelines/add",
        data={**_ADD_DATA, "name": "Listed Pipeline"},
    )
    resp = await client.get("/pipelines/")
    assert resp.status_code == 200
    assert "Listed Pipeline" in resp.text


@pytest.mark.asyncio
async def test_toggle_pipeline(client):
    await client.post("/pipelines/add", data=_ADD_DATA)
    resp = await client.post("/pipelines/1/toggle", follow_redirects=False)
    assert resp.status_code == 303
    assert "msg=pipeline_toggled" in resp.headers["location"]


@pytest.mark.asyncio
async def test_delete_pipeline(client):
    await client.post("/pipelines/add", data=_ADD_DATA)
    resp = await client.post("/pipelines/1/delete", follow_redirects=False)
    assert resp.status_code == 303
    assert "msg=pipeline_deleted" in resp.headers["location"]


@pytest.mark.asyncio
async def test_edit_pipeline(client):
    await client.post("/pipelines/add", data=_ADD_DATA)
    resp = await client.post(
        "/pipelines/1/edit",
        data={**_ADD_DATA, "name": "Edited", "publish_mode": "auto", "llm_model": "gpt-4o"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "msg=pipeline_edited" in resp.headers["location"]


# === New tests ===


@pytest.mark.asyncio
async def test_run_pipeline_not_found(client):
    """Test run pipeline with invalid ID."""
    resp = await client.post("/pipelines/999999/run", follow_redirects=False)
    assert resp.status_code == 303
    assert "error=pipeline_invalid" in resp.headers["location"]


@pytest.mark.asyncio
async def test_run_pipeline_enqueues(client):
    """Test run pipeline enqueues generation."""
    from unittest.mock import patch

    await client.post("/pipelines/add", data=_ADD_DATA)

    with patch("src.web.routes.pipelines.deps.pipeline_service") as mock_svc:
        mock_svc.return_value.get = AsyncMock(
            return_value=MagicMock(id=1, is_active=True)
        )
        with patch("src.web.routes.pipelines.deps.get_task_enqueuer") as mock_enq:
            mock_enq.return_value.enqueue_pipeline_run = AsyncMock()
            resp = await client.post("/pipelines/1/run", follow_redirects=False)
            assert resp.status_code == 303
            assert "msg=pipeline_run_enqueued" in resp.headers["location"]


@pytest.mark.asyncio
async def test_run_pipeline_failure(client):
    """Test run pipeline handles failure."""
    from unittest.mock import patch

    await client.post("/pipelines/add", data=_ADD_DATA)

    with patch("src.web.routes.pipelines.deps.pipeline_service") as mock_svc:
        mock_svc.return_value.get = AsyncMock(
            return_value=MagicMock(id=1, is_active=True)
        )
        with patch("src.web.routes.pipelines.deps.get_task_enqueuer") as mock_enq:
            mock_enq.return_value.enqueue_pipeline_run = AsyncMock(
                side_effect=Exception("Queue error")
            )
            resp = await client.post("/pipelines/1/run", follow_redirects=False)
            assert resp.status_code == 303
            assert "error=pipeline_run_failed" in resp.headers["location"]


@pytest.mark.asyncio
async def test_generate_page_renders(client):
    """Test generate page renders."""
    await client.post("/pipelines/add", data=_ADD_DATA)

    resp = await client.get("/pipelines/1/generate")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_generate_page_not_found(client):
    """Test generate page with invalid pipeline."""
    resp = await client.get("/pipelines/999999/generate", follow_redirects=False)
    assert resp.status_code == 303
    assert "error=pipeline_invalid" in resp.headers["location"]


# === SSE Streaming tests ===


@pytest.mark.asyncio
async def test_generate_stream_success(client):
    """Test SSE streaming generation."""
    from unittest.mock import patch

    await client.post("/pipelines/add", data=_ADD_DATA)

    async def fake_stream(*args, **kwargs):
        yield {"delta": "Hello", "generated_text": None, "citations": []}
        yield {"delta": " world", "generated_text": "Hello world", "citations": []}

    with patch("src.services.provider_service.AgentProviderService") as mock_provider_service:
        mock_provider_instance = MagicMock()
        mock_provider_instance.get_provider_callable = MagicMock(return_value=lambda: None)
        mock_provider_service.return_value = mock_provider_instance

        with patch("src.services.generation_service.GenerationService") as mock_gen:
            mock_instance = MagicMock()
            mock_instance.generate_stream = fake_stream
            mock_gen.return_value = mock_instance

            resp = await client.get("/pipelines/1/generate-stream")
            assert resp.status_code == 200
            assert "text/event-stream" in resp.headers["content-type"]


@pytest.mark.asyncio
async def test_generate_stream_pipeline_not_found(client):
    """Test SSE streaming with invalid pipeline."""
    resp = await client.get("/pipelines/999999/generate-stream", follow_redirects=False)
    assert resp.status_code == 303
    assert "error=pipeline_invalid" in resp.headers["location"]


@pytest.mark.asyncio
async def test_generate_pipeline_success(client):
    """Test non-streaming generation success."""
    from unittest.mock import patch

    await client.post("/pipelines/add", data=_ADD_DATA)

    with patch("src.services.provider_service.AgentProviderService") as mock_provider_service:
        mock_provider_instance = MagicMock()
        mock_provider_instance.get_provider_callable = MagicMock(return_value=lambda: None)
        mock_provider_service.return_value = mock_provider_instance

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


@pytest.mark.asyncio
async def test_generate_pipeline_failure(client):
    """Test non-streaming generation failure."""
    from unittest.mock import patch

    await client.post("/pipelines/add", data=_ADD_DATA)

    with patch("src.services.provider_service.AgentProviderService") as mock_provider_service:
        mock_provider_instance = MagicMock()
        mock_provider_instance.get_provider_callable = MagicMock(return_value=lambda: None)
        mock_provider_service.return_value = mock_provider_instance

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


@pytest.mark.asyncio
async def test_generate_pipeline_not_found(client):
    """Test non-streaming generation with invalid pipeline."""
    resp = await client.post(
        "/pipelines/999999/generate",
        data={"model": "", "max_tokens": "256", "temperature": "0.0"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "error=pipeline_invalid" in resp.headers["location"]


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
async def test_edit_pipeline_not_found(client):
    """Test edit with invalid pipeline_id."""
    resp = await client.post(
        "/pipelines/999999/edit",
        data=_ADD_DATA,
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "error=pipeline_invalid" in resp.headers["location"]


@pytest.mark.asyncio
async def test_toggle_pipeline_not_found(client):
    """Test toggle with invalid pipeline_id."""
    resp = await client.post("/pipelines/999999/toggle", follow_redirects=False)
    assert resp.status_code == 303
    assert "error=pipeline_invalid" in resp.headers["location"]


# === _target_refs error paths ===


def test_target_refs_missing_separator():
    """Test _target_refs with missing separator."""
    from src.services.pipeline_service import PipelineValidationError
    from src.web.routes.pipelines import _target_refs

    try:
        _target_refs(["invalid_format"])
        assert False, "Should have raised PipelineValidationError"
    except PipelineValidationError as e:
        assert "Некорректный формат цели" in str(e)


def test_target_refs_invalid_dialog_id():
    """Test _target_refs with invalid dialog_id."""
    from src.services.pipeline_service import PipelineValidationError
    from src.web.routes.pipelines import _target_refs

    try:
        _target_refs(["+1234567890|not_a_number"])
        assert False, "Should have raised PipelineValidationError"
    except PipelineValidationError as e:
        assert "Некорректный dialog id" in str(e)


def test_target_refs_success():
    """Test _target_refs with valid input."""
    from src.web.routes.pipelines import _target_refs

    refs = _target_refs(["+1234567890|100", "+0987654321|200"])
    assert len(refs) == 2
    assert refs[0].phone == "+1234567890"
    assert refs[0].dialog_id == 100
    assert refs[1].phone == "+0987654321"
    assert refs[1].dialog_id == 200
