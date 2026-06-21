"""Navigation progress bar is wired into the base layout (issue #756)."""

from __future__ import annotations

import pytest


@pytest.mark.anyio
async def test_base_layout_includes_progress_bar(route_client):
    resp = await route_client.get("/channels/")
    assert resp.status_code == 200
    html = resp.text
    # The bar element and its driver script must be present on every page.
    assert 'id="app-progress"' in html
    assert "/static/js/nav-progress.js" in html
