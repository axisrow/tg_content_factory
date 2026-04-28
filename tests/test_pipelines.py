from __future__ import annotations

from types import MethodType

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

    app.state.pool.get_dialogs_for_phone = MethodType(_get_dialogs_for_phone, app.state.pool)

    async with make_auth_client(app) as client:
        yield client

    await app.state.collection_queue.shutdown()
    await built_db.close()


_LLM_ENV_VARS = [
    "OPENAI_API_KEY", "COHERE_API_KEY", "CONTEXT7_API_KEY", "CTX7_API_KEY",
    "OLLAMA_BASE", "OLLAMA_URL", "HUGGINGFACE_API_KEY", "HUGGINGFACE_TOKEN",
    "FIREWORKS_BASE", "FIREWORKS_API_BASE", "FIREWORKS_API_KEY",
    "DEEPSEEK_BASE", "DEEPSEEK_API_BASE", "DEEPSEEK_API_KEY",
    "TOGETHER_BASE", "TOGETHER_API_BASE", "TOGETHER_API_KEY",
]


@pytest.mark.anyio
async def test_pipelines_page_renders(pipeline_client, monkeypatch):
    for var in _LLM_ENV_VARS:
        monkeypatch.delenv(var, raising=False)
    resp = await pipeline_client.get("/pipelines/")
    assert resp.status_code == 200
    assert "Пайплайны контента" in resp.text
    # When no LLM provider is configured, a warning banner is shown and the
    # "New pipeline" form is hidden (so channel names like "Source A" are not in the DOM).
    assert "LLM-провайдер не настроен" in resp.text


@pytest.mark.anyio
async def test_add_pipeline_route(pipeline_client):
    resp = await pipeline_client.post(
        "/pipelines/add",
        data={
            "name": "Digest",
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
    assert "msg=pipeline_added" in resp.headers["location"]

    follow = await pipeline_client.get("/pipelines/")
    assert "Digest" in follow.text
