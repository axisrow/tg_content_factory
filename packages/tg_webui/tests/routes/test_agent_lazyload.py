"""Lazyload split for the Agent page (#949, part of #756).

The skeleton route (`GET /agent`) must render instantly without probing the
agent backend runtime status; the thread list + runtime chips + composer footer
load lazily from `GET /agent/fragments/threads`.
"""

from __future__ import annotations

import pytest


@pytest.fixture
async def client(route_client, agent_manager_mock):
    """Client with the agent_manager_mock wired into app.state."""
    client = route_client
    client._transport_app.state.agent_manager = agent_manager_mock
    yield client


@pytest.fixture
async def db(base_app):
    _, db, _ = base_app
    return db


@pytest.mark.anyio
async def test_skeleton_does_not_probe_runtime_status(client, db, agent_manager_mock):
    """The skeleton route must NOT call get_runtime_status (it's the heavy bit)."""
    thread_id = await db.create_agent_thread("Skeleton Thread")
    agent_manager_mock.get_runtime_status.reset_mock()

    resp = await client.get(f"/agent?thread_id={thread_id}")

    assert resp.status_code == 200
    agent_manager_mock.get_runtime_status.assert_not_called()


@pytest.mark.anyio
async def test_skeleton_has_lazy_fragment_trigger(client, db):
    """Skeleton wires the lazy fragment with hx-trigger=load + active thread id."""
    thread_id = await db.create_agent_thread("Lazy Thread")

    resp = await client.get(f"/agent?thread_id={thread_id}")

    assert resp.status_code == 200
    assert f"/agent/fragments/threads?thread_id={thread_id}" in resp.text
    assert 'hx-trigger="load"' in resp.text
    # The active thread title still renders synchronously (chat header) so the
    # page is recognisable before the fragment lands.
    assert "Lazy Thread" in resp.text


@pytest.mark.anyio
async def test_skeleton_defers_runtime_chips(client, db):
    """Runtime chips/model-select are NOT in the skeleton — they OOB-load later."""
    thread_id = await db.create_agent_thread("Chips Thread")

    resp = await client.get(f"/agent?thread_id={thread_id}")

    assert resp.status_code == 200
    # The OOB target slots exist (empty) but their runtime-dependent content does not.
    assert 'id="chat-runtime-slot"' in resp.text
    assert 'id="composer-actions-slot"' in resp.text
    # No rendered chip spans and no model select — only the CSS rules for the
    # chip classes live in <head>, never the `<span class="runtime-chip ...">`
    # markup or the OOB swaps (those are fragment-only).
    assert '<span class="runtime-chip' not in resp.text
    assert "hx-swap-oob" not in resp.text
    assert 'id="model-select"' not in resp.text


@pytest.mark.anyio
async def test_skeleton_send_reads_saved_model_before_fragment_loads(client, db):
    """A message sent before the lazy composer fragment lands keeps the saved model.

    Regression guard (#949): the composer's <select> now loads lazily, so the
    send-time read MUST fall back to localStorage (via selectedModel()) instead of
    reading the not-yet-present #model-select directly — otherwise the user's saved
    model choice is silently dropped (sent as ''/Авто) during the load window.
    """
    thread_id = await db.create_agent_thread("Race Thread")

    resp = await client.get(f"/agent?thread_id={thread_id}")

    assert resp.status_code == 200
    # The chat POST body uses the localStorage-backed helper, never the raw element.
    assert "model: selectedModel()" in resp.text
    assert "localStorage.getItem('agent_model')" in resp.text
    # The old direct-read pattern (drops the saved model pre-fragment) must be gone.
    assert "(document.getElementById('model-select') || {}).value" not in resp.text


@pytest.mark.anyio
async def test_fragment_probes_runtime_and_renders_threads(
    client, db, agent_manager_mock
):
    """The fragment probes runtime status and renders the thread list + chips."""
    thread_id = await db.create_agent_thread("Fragment Thread")
    agent_manager_mock.get_runtime_status.reset_mock()

    resp = await client.get(f"/agent/fragments/threads?thread_id={thread_id}")

    assert resp.status_code == 200
    agent_manager_mock.get_runtime_status.assert_awaited_once()
    # Thread list (primary swap) + runtime chips (OOB) + composer (OOB).
    assert "Fragment Thread" in resp.text
    assert '<span class="runtime-chip' in resp.text
    assert f'data-thread-id="{thread_id}"' in resp.text


@pytest.mark.anyio
async def test_fragment_oob_targets_match_skeleton_slots(client, db):
    """Fragment OOB swaps must target the slot ids the skeleton renders."""
    thread_id = await db.create_agent_thread("OOB Thread")

    resp = await client.get(f"/agent/fragments/threads?thread_id={thread_id}")

    assert resp.status_code == 200
    for slot in ("offcanvas-threads", "chat-runtime-slot", "composer-actions-slot"):
        assert f'id="{slot}"' in resp.text
        assert 'hx-swap-oob="true"' in resp.text


@pytest.mark.anyio
async def test_fragment_marks_active_thread(client, db):
    """The open thread is highlighted via the active class in the fragment."""
    other = await db.create_agent_thread("Other")
    active = await db.create_agent_thread("Active")

    resp = await client.get(f"/agent/fragments/threads?thread_id={active}")

    assert resp.status_code == 200
    # The active thread row carries the `active` class; the other one does not.
    assert 'class="thread-item active"' in resp.text
    assert f'href="/agent?thread_id={active}"' in resp.text
    assert f'data-thread-id="{other}"' in resp.text
    # Exactly one row is marked active (the open thread), not the other.
    assert resp.text.count('class="thread-item active"') == 2  # sidebar + offcanvas


@pytest.mark.anyio
async def test_fragment_without_agent_manager(client, db):
    """Fragment degrades gracefully when no agent manager is configured."""
    client._transport_app.state.agent_manager = None
    thread_id = await db.create_agent_thread("No Manager")

    resp = await client.get(f"/agent/fragments/threads?thread_id={thread_id}")

    assert resp.status_code == 200
    # Threads still render; runtime chips are absent (no status to show).
    assert "No Manager" in resp.text
    assert '<span class="runtime-chip' not in resp.text


@pytest.mark.anyio
async def test_fragment_deepagents_hides_model_select(client, db, agent_manager_mock):
    """For the deepagents backend the composer shows the hint, not the model select."""
    thread_id = await db.create_agent_thread("Deepagents")
    runtime = agent_manager_mock.get_runtime_status.return_value
    runtime.selected_backend = "deepagents"

    resp = await client.get(f"/agent/fragments/threads?thread_id={thread_id}")

    assert resp.status_code == 200
    assert 'id="model-select"' not in resp.text
    assert "Deepagents runtime" in resp.text


@pytest.mark.anyio
async def test_fragment_claude_shows_model_select(client, db, agent_manager_mock):
    """The Claude backend is the only one that renders the model select."""
    thread_id = await db.create_agent_thread("Claude")
    runtime = agent_manager_mock.get_runtime_status.return_value
    runtime.selected_backend = "claude"

    resp = await client.get(f"/agent/fragments/threads?thread_id={thread_id}")

    assert resp.status_code == 200
    assert 'id="model-select"' in resp.text


@pytest.mark.anyio
@pytest.mark.parametrize("backend", ["codex", "adk"])
async def test_fragment_codex_adk_hide_dead_model_select(client, db, agent_manager_mock, backend):
    """codex/adk must NOT render the Claude-labelled model select (#1002).

    These backends validate the submitted model against their own ID sets
    (CODEX_MODEL_IDS / ADK_MODEL_IDS); a Claude-model dropdown there is a dead
    control whose value is always discarded server-side. The fragment shows a
    backend hint instead of the picker.
    """
    thread_id = await db.create_agent_thread(backend)
    runtime = agent_manager_mock.get_runtime_status.return_value
    runtime.selected_backend = backend

    resp = await client.get(f"/agent/fragments/threads?thread_id={thread_id}")

    assert resp.status_code == 200
    # No dead Claude dropdown and no "Модель Claude" caption for these backends.
    assert 'id="model-select"' not in resp.text
    assert "Модель Claude" not in resp.text
    # A backend-named hint replaces the picker so the slot is not empty.
    assert f"{backend} runtime" in resp.text
