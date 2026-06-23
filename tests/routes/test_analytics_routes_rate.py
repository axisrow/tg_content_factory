"""Web parity for the LLM-judge write path (#999).

POST /analytics/channels/rate runs ``ChannelAnalysisService.classify_channel``
— a provider spend + ``channel_ratings`` write — and swaps an HTMX fragment
with the fresh verdict. These tests mirror the CLI guards from #994 with a fake
provider callable: no live provider calls, no real spend.
"""

from __future__ import annotations

import pytest

from src.services.provider_service import RuntimeProviderRegistry


async def _seed_posts(db, channel_id: int, n: int = 3) -> None:
    """Insert text messages so the empty-channel guard does not trip."""
    for i in range(n):
        await db.execute_write(
            "INSERT OR IGNORE INTO messages (channel_id, message_id, text, message_kind, date) "
            "VALUES (?, ?, ?, 'regular', ?)",
            (channel_id, i + 1, f"post {i}", "2026-01-01T00:00:00"),
        )


def _fake_registry(verdict_json: str = '{"useful": "useless", "genre": "ad", '
                   '"confidence": 0.9, "reason": "реклама"}') -> RuntimeProviderRegistry:
    """A registry with one fake provider that returns a canned judge verdict."""
    svc = RuntimeProviderRegistry()

    async def _fake_provider(prompt: str = "", **kwargs) -> str:
        return verdict_json

    svc.register_provider("fake", _fake_provider)
    return svc


@pytest.mark.aiosqlite_serial
async def test_rate_no_provider_returns_fragment(route_client):
    """Without a configured provider: no spend, no write, warning fragment."""
    app = route_client._transport_app
    app.state.llm_provider_service = RuntimeProviderRegistry()  # has_providers() == False

    resp = await route_client.post("/analytics/channels/rate", data={"channel_id": 100})
    assert resp.status_code == 200
    assert "LLM-провайдер не настроен" in resp.text
    # Nothing persisted.
    rating = await app.state.db.repos.channel_ratings.get(100)
    assert rating is None


@pytest.mark.aiosqlite_serial
async def test_rate_unknown_model_aborts(route_client):
    """A mistyped model must surface an error, not persist a stub verdict."""
    app = route_client._transport_app
    app.state.llm_provider_service = _fake_registry()

    resp = await route_client.post(
        "/analytics/channels/rate", data={"channel_id": 100, "model": "gpt-nope"}
    )
    assert resp.status_code == 200
    assert "not registered" in resp.text
    assert await app.state.db.repos.channel_ratings.get(100) is None


@pytest.mark.aiosqlite_serial
async def test_rate_empty_channel_skips(route_client):
    """A channel with no text posts skips the provider call and the upsert."""
    app = route_client._transport_app
    app.state.llm_provider_service = _fake_registry()

    resp = await route_client.post("/analytics/channels/rate", data={"channel_id": 100})
    assert resp.status_code == 200
    assert "нет текстовых постов" in resp.text
    assert await app.state.db.repos.channel_ratings.get(100) is None


@pytest.mark.aiosqlite_serial
async def test_rate_runs_judge_and_persists(route_client):
    """Happy path: fake judge verdict is rendered and persisted to channel_ratings."""
    app = route_client._transport_app
    db = app.state.db
    await _seed_posts(db, 100)
    app.state.llm_provider_service = _fake_registry()

    resp = await route_client.post("/analytics/channels/rate", data={"channel_id": 100})
    assert resp.status_code == 200
    # Verdict fragment rendered.
    assert "useless" in resp.text
    assert "ad" in resp.text
    assert "0.90" in resp.text
    # Persisted.
    rating = await db.repos.channel_ratings.get(100)
    assert rating is not None
    assert rating.useful == "useless"
    assert rating.genre == "ad"


@pytest.mark.aiosqlite_serial
async def test_rate_sample_size_clamped(route_client):
    """sample_size is clamped to [1, 200] before reaching the judge."""
    app = route_client._transport_app
    db = app.state.db
    await _seed_posts(db, 100)
    app.state.llm_provider_service = _fake_registry()

    resp = await route_client.post(
        "/analytics/channels/rate", data={"channel_id": 100, "sample_size": 99999}
    )
    assert resp.status_code == 200
    # Did not error out; verdict persisted (clamp accepted the huge value).
    assert await db.repos.channel_ratings.get(100) is not None
