"""Cross-layer regression for pipeline result semantics (issue #463).

Chains per scenario: real metadata → DB → task row → /scheduler renders
a semantic cell. This guards against drift between layers — if any single
layer starts interpreting the result differently, the test fails in that
layer rather than silently passing.
"""

from __future__ import annotations

import pytest
from bs4 import BeautifulSoup

from src.models import (
    CollectionTaskStatus,
    CollectionTaskType,
    PipelineRunTaskPayload,
)

pytestmark = pytest.mark.anyio


async def _seed_run_and_task(
    db,
    *,
    pipeline_id: int = 1,
    generated_text: str,
    metadata: dict,
    messages_collected: int,
) -> tuple[int, int]:
    """Insert a generation_run + completed pipeline_run task, return (task_id, run_id)."""
    run_id = await db.repos.generation_runs.create_run(pipeline_id, "prompt")
    await db.repos.generation_runs.save_result(run_id, generated_text, metadata)

    task_id = await db.repos.tasks.create_generic_task(
        CollectionTaskType.PIPELINE_RUN,
        payload=PipelineRunTaskPayload(pipeline_id=pipeline_id),
    )
    await db.repos.tasks.update_collection_task(
        task_id,
        CollectionTaskStatus.COMPLETED,
        messages_collected=messages_collected,
        note=f"Pipeline run id={run_id}",
    )
    return task_id, run_id


def _first_pipeline_row(soup):
    """Return the first tbody <tr> whose type-cell badge indicates a pipeline run."""
    for table in soup.select("table.tga-table-striped"):
        tbody = table.find("tbody")
        if tbody is None:
            continue
        for tr in tbody.find_all("tr"):
            tds = tr.find_all("td")
            if not tds:
                continue
            first = tds[0].get_text(strip=True, separator=" ").lower()
            if "pipeline" in first or "пайплайн" in first:
                return tr
    return None


async def test_generation_run_end_to_end(base_app, route_client):
    """Generation run: DB → task → /scheduler renders 'Сгенерировано N'."""
    _, db, _ = base_app
    task_id, run_id = await _seed_run_and_task(
        db,
        pipeline_id=1,
        generated_text="hello",
        metadata={
            "citations": [{"id": 1}, {"id": 2}, {"id": 3}, {"id": 4}],
            "result_kind": "generated_items",
            "result_count": 4,
        },
        messages_collected=4,
    )

    # Verify DB contract
    stored_task = await db.repos.tasks.get_collection_task(task_id)
    assert stored_task is not None
    assert stored_task.messages_collected == 4

    stored_run = await db.repos.generation_runs.get(run_id)
    assert stored_run is not None
    assert stored_run.result_kind == "generated_items"
    assert stored_run.result_count == 4

    resp = await route_client.get("/scheduler/?status=all")
    assert resp.status_code == 200
    soup = BeautifulSoup(resp.text, "html.parser")
    row = _first_pipeline_row(soup)
    assert row is not None
    cell = row.find_all("td")[3].get_text(strip=True, separator=" ")
    assert "Сгенерировано" in cell
    assert "4" in cell


async def test_action_only_run_end_to_end_empty_text_no_zero(base_app, route_client):
    """The #463 headline regression: empty text must NOT manifest as '0 messages'."""
    _, db, _ = base_app
    task_id, run_id = await _seed_run_and_task(
        db,
        pipeline_id=1,
        generated_text="",
        metadata={
            "citations": [],
            "action_counts": {"react": 9},
            "result_kind": "processed_messages",
            "result_count": 9,
        },
        messages_collected=9,
    )

    # Verify the DB chain preserved the count.
    stored_run = await db.repos.generation_runs.get(run_id)
    assert stored_run is not None
    assert (stored_run.generated_text or "") == ""
    assert stored_run.result_kind == "processed_messages"
    assert stored_run.result_count == 9

    resp = await route_client.get("/scheduler/?status=all")
    assert resp.status_code == 200
    soup = BeautifulSoup(resp.text, "html.parser")
    row = _first_pipeline_row(soup)
    assert row is not None
    cell = row.find_all("td")[3].get_text(strip=True, separator=" ")
    assert "Обработано" in cell, f"expected 'Обработано' in {cell!r}"
    assert "9" in cell


async def test_action_run_with_node_errors_shows_warning_badge_end_to_end(base_app, route_client):
    """Issue #463 observability: when action handler records node_errors
    (e.g. all accounts flood-waited), scheduler UI must show a warning badge.
    """
    _, db, _ = base_app
    task_id, run_id = await _seed_run_and_task(
        db,
        pipeline_id=1,
        generated_text="",
        metadata={
            "citations": [],
            "result_kind": "processed_messages",
            "result_count": 0,
            "node_errors": [
                {
                    "node_id": "react_1",
                    "code": "no_available_client",
                    "detail": "all accounts are flood-waited",
                }
            ],
        },
        messages_collected=0,
    )

    # DB round-trip preserves node_errors.
    stored = await db.repos.generation_runs.get(run_id)
    assert stored is not None
    assert (stored.metadata or {}).get("node_errors")

    resp = await route_client.get("/scheduler/?status=all")
    soup = BeautifulSoup(resp.text, "html.parser")
    row = _first_pipeline_row(soup)
    assert row is not None
    cell_html = str(row.find_all("td")[3])
    assert "⚠" in cell_html or "pipe-run-warning" in cell_html


async def test_mixed_run_end_to_end_shows_generation_count(base_app, route_client):
    """Mixed run — generation semantics wins, UI shows 'Сгенерировано N_citations'."""
    _, db, _ = base_app
    task_id, run_id = await _seed_run_and_task(
        db,
        pipeline_id=1,
        generated_text="draft",
        metadata={
            "citations": [{"id": 1}, {"id": 2}],
            "action_counts": {"react": 7},
            "result_kind": "generated_items",
            "result_count": 2,
        },
        messages_collected=2,
    )

    # Verify metadata survived the DB round-trip.
    stored_run = await db.repos.generation_runs.get(run_id)
    assert stored_run is not None
    assert stored_run.metadata["action_counts"] == {"react": 7}

    resp = await route_client.get("/scheduler/?status=all")
    soup = BeautifulSoup(resp.text, "html.parser")
    row = _first_pipeline_row(soup)
    assert row is not None
    cell = row.find_all("td")[3].get_text(strip=True, separator=" ")
    assert "Сгенерировано" in cell
    assert "2" in cell
