"""Agent-tool A/B variant parity tests (issue #1068).

Exercise get_ab_variants / select_variant / auto_select_best against the real
in-memory ``db`` fixture so the real ABTestingService + generation_runs paths
run. No real provider — auto_select_best falls back to length-based selection
when the quality service degrades.
"""

from __future__ import annotations

import pytest

from tests.agent_tools_helpers import _get_tool_handlers, _text


async def _seed_run(db, variants: list[str]) -> int:
    run_id = await db.repos.generation_runs.create_run(1, "prompt")
    await db.repos.generation_runs.save_result(run_id, variants[0])
    await db.repos.generation_runs.set_variants(run_id, variants)
    return run_id


@pytest.mark.anyio
async def test_get_ab_variants_lists_entries(db):
    run_id = await _seed_run(db, ["base", "alt one", "alt two"])
    handlers = _get_tool_handlers(db)

    result = await handlers["get_ab_variants"]({"run_id": run_id})

    text = _text(result)
    assert "3 шт." in text
    assert "[0]" in text and "[2]" in text


@pytest.mark.anyio
async def test_get_ab_variants_missing_run(db):
    handlers = _get_tool_handlers(db)
    result = await handlers["get_ab_variants"]({"run_id": 99999})
    assert "не найден" in _text(result)


@pytest.mark.anyio
async def test_select_variant_requires_confirm(db):
    run_id = await _seed_run(db, ["base", "winner"])
    handlers = _get_tool_handlers(db)

    result = await handlers["select_variant"]({"run_id": run_id, "variant_index": 1})

    # Without confirm=true the gate fires and nothing is mutated.
    assert "Подтвердите" in _text(result)
    run = await db.repos.generation_runs.get(run_id)
    assert run.selected_variant is None
    assert run.generated_text == "base"


@pytest.mark.anyio
async def test_select_variant_updates_generated_text(db):
    run_id = await _seed_run(db, ["base", "winner"])
    handlers = _get_tool_handlers(db)

    result = await handlers["select_variant"](
        {"run_id": run_id, "variant_index": 1, "confirm": "true"}
    )

    assert "выбран" in _text(result)
    run = await db.repos.generation_runs.get(run_id)
    assert run.generated_text == "winner"
    assert run.selected_variant == 1


@pytest.mark.anyio
async def test_select_variant_invalid_index_reports_error(db):
    run_id = await _seed_run(db, ["base", "alt"])
    handlers = _get_tool_handlers(db)

    result = await handlers["select_variant"](
        {"run_id": run_id, "variant_index": 9, "confirm": "true"}
    )

    assert "Invalid variant index" in _text(result)
    run = await db.repos.generation_runs.get(run_id)
    assert run.generated_text == "base"


@pytest.mark.anyio
async def test_auto_select_best_requires_confirm(db):
    run_id = await _seed_run(db, ["base", "longer variant text"])
    handlers = _get_tool_handlers(db)

    result = await handlers["auto_select_best"]({"run_id": run_id})

    assert "Подтвердите" in _text(result)
    run = await db.repos.generation_runs.get(run_id)
    assert run.selected_variant is None
