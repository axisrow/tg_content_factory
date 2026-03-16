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


@pytest.mark.asyncio
async def test_pipeline_generate_and_publish(pipeline_client, monkeypatch):
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

    # Trigger generation (non-streaming path)
    gen_resp = await pipeline_client.post(f"/pipelines/{pipeline_id}/generate", data={"model": "", "max_tokens": "256", "temperature": "0.0"}, follow_redirects=True)
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
    pub = await pipeline_client.post(f"/pipelines/{pipeline_id}/publish", data={"run_id": str(run_id)}, follow_redirects=False)
    assert pub.status_code == 303
    assert "pipeline_published" in pub.headers["location"]

    run_after = await db.repos.generation_runs.get(run_id)
    assert run_after is not None
    assert run_after.status == "published"
    assert run_after.metadata and run_after.metadata.get("published") is True
