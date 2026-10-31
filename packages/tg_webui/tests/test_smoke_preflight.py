"""Fast offline smoke preflight over real project functions.

This file is the curated ``-m smoke`` set: a handful of fast, offline checks
that each exercise a *real* critical path (no mocks of the thing under test,
no network, no opt-in gates). The goal is a sub-second "does the app even
stand up" gate to run before the full suite — if any of these fail, something
fundamental is broken and the rest of the run is not worth starting.

Rules for this file:
- offline only — never touch the network or a live provider/Telegram;
- fast — in-memory DB or pure construction, no file-backed DB on disk;
- real — call the actual project entrypoint, do not assert on a mock.

Most tests here are ``integration`` by level (they stand up the DB / web app);
``smoke`` is an orthogonal axis layered on top, so they carry both markers.
"""
# Web-panel tests split out of tests/test_smoke_preflight.py (#1720 wave 3).


from __future__ import annotations

import httpx
import pytest
from webui_helpers import build_web_app

from tests.helpers import make_test_config

pytestmark = pytest.mark.smoke


async def test_web_app_health_endpoint(tmp_path, real_pool_harness_factory) -> None:
    """The web app serves /health over a real (offline) ASGI request."""
    config = make_test_config(tmp_path)
    harness = real_pool_harness_factory()
    app, db = await build_web_app(config, harness)
    try:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/health")
        assert resp.status_code == 200
        assert resp.json()["status"] in ("healthy", "degraded")
    finally:
        await db.close()
