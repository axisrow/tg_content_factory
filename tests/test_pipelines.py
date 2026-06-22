from __future__ import annotations

import pytest

# pipeline_client fixture is shared from tests/conftest.py (deduped).

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
    resp = await pipeline_client.get("/pipelines/fragments/list")
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

    follow = await pipeline_client.get("/pipelines/fragments/list")
    assert "Digest" in follow.text
