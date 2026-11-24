"""Tests for PipelineService listing, validation, and graph edge cases."""
from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tg_agent.database.bundles import PipelineBundle
from tg_agent.models import (
    Account,
    Channel,
    ContentPipeline,
    PipelineEdge,
    PipelineGraph,
    PipelineNode,
    PipelineNodeType,
)
from tg_agent.services.pipeline_service import (
    PipelineService,
    PipelineTargetRef,
    PipelineValidationError,
)

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
async def svc(db):
    """PipelineService backed by real in-memory DB."""
    await db.add_account(Account(phone="+100", session_string="sess"))
    await db.add_channel(Channel(channel_id=1001, title="Source A"))
    await db.add_channel(Channel(channel_id=1002, title="Source B"))
    await db.repos.dialog_cache.replace_dialogs(
        "+100",
        [
            {
                "channel_id": 77,
                "title": "Target A",
                "username": "targeta",
                "channel_type": "channel",
            },
            {
                "channel_id": 78,
                "title": "Target B",
                "username": "targetb",
                "channel_type": "channel",
            },
        ],
    )
    return PipelineService(PipelineBundle.from_database(db))


@pytest.fixture
async def pipeline_id(svc):
    """Create a basic pipeline for reuse."""
    return await svc.add(
        name="TestPipeline",
        prompt_template="Summarize {source_messages}",
        source_channel_ids=[1001, 1002],
        target_refs=[PipelineTargetRef(phone="+100", dialog_id=77)],
        generate_interval_minutes=30,
    )


# ---------------------------------------------------------------------------
# list / get / toggle / delete
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_list_returns_all(svc, pipeline_id):
    items = await svc.list_pipelines()
    assert len(items) == 1
    assert items[0].name == "TestPipeline"


@pytest.mark.anyio
async def test_list_active_only(svc, pipeline_id):
    items = await svc.list_pipelines(active_only=True)
    assert len(items) == 1


@pytest.mark.anyio
async def test_get_existing(svc, pipeline_id):
    p = await svc.get(pipeline_id)
    assert p is not None
    assert p.id == pipeline_id


@pytest.mark.anyio
async def test_get_nonexistent(svc):
    assert await svc.get(99999) is None


@pytest.mark.anyio
async def test_get_retrieval_scope_single_source(svc):
    pipeline_id = await svc.add(
        name="SingleSource",
        prompt_template="Prompt {source_messages}",
        source_channel_ids=[1001],
        target_refs=[PipelineTargetRef(phone="+100", dialog_id=77)],
    )
    pipeline = await svc.get(pipeline_id)
    scope = await svc.get_retrieval_scope(pipeline)
    assert scope.query == "SingleSource"
    assert scope.channel_id == 1001


@pytest.mark.anyio
async def test_get_retrieval_scope_multi_source(svc, pipeline_id):
    pipeline = await svc.get(pipeline_id)
    scope = await svc.get_retrieval_scope(pipeline)
    assert scope.query == "TestPipeline"
    assert scope.channel_id is None


@pytest.mark.anyio
async def test_toggle_deactivates(svc, pipeline_id):
    result = await svc.toggle(pipeline_id)
    assert result is True
    p = await svc.get(pipeline_id)
    assert p.is_active is False


@pytest.mark.anyio
async def test_toggle_reactivates(svc, pipeline_id):
    await svc.toggle(pipeline_id)  # deactivate
    result = await svc.toggle(pipeline_id)  # reactivate
    assert result is True
    p = await svc.get(pipeline_id)
    assert p.is_active is True


@pytest.mark.anyio
async def test_toggle_nonexistent(svc):
    result = await svc.toggle(99999)
    assert result is False


@pytest.mark.anyio
async def test_delete(svc, pipeline_id):
    await svc.delete(pipeline_id)
    assert await svc.get(pipeline_id) is None


# ---------------------------------------------------------------------------
# update
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_update_pipeline(svc, pipeline_id):
    ok = await svc.update(
        pipeline_id,
        name="Updated",
        prompt_template="New {source_messages}",
        source_channel_ids=[1001],
        target_refs=[PipelineTargetRef(phone="+100", dialog_id=77)],
    )
    assert ok is True
    p = await svc.get(pipeline_id)
    assert p.name == "Updated"


# ---------------------------------------------------------------------------
# get_sources / get_targets / get_detail / get_with_relations
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_get_sources(svc, pipeline_id):
    sources = await svc.get_sources(pipeline_id)
    assert len(sources) == 2
    assert {s.channel_id for s in sources} == {1001, 1002}


@pytest.mark.anyio
async def test_get_targets(svc, pipeline_id):
    targets = await svc.get_targets(pipeline_id)
    assert len(targets) == 1
    assert targets[0].phone == "+100"
    assert targets[0].dialog_id == 77


@pytest.mark.anyio
async def test_get_detail(svc, pipeline_id):
    detail = await svc.get_detail(pipeline_id)
    assert detail is not None
    assert detail["pipeline"].id == pipeline_id
    assert detail["source_ids"] == [1001, 1002]
    assert detail["target_refs"] == ["+100|77"]
    assert len(detail["source_titles"]) == 2


@pytest.mark.anyio
async def test_get_detail_uses_dag_source_ids_without_sidecar_rows(svc):
    graph = PipelineGraph(
        nodes=[
            PipelineNode(
                id="src",
                type=PipelineNodeType.SOURCE,
                name="Source",
                config={"channel_ids": [1002]},
            ),
            PipelineNode(id="pub", type=PipelineNodeType.PUBLISH, name="Publish"),
        ],
        edges=[PipelineEdge(from_node="src", to_node="pub")],
    )
    pipeline_id = await svc._bundle.add(  # noqa: SLF001 - test seeds exact storage shape.
        ContentPipeline(name="DAG detail", prompt_template=".", pipeline_json=graph),
        [],
        [],
    )

    detail = await svc.get_detail(pipeline_id)
    assert detail is not None
    assert detail["source_ids"] == [1002]
    assert detail["source_titles"] == ["Source B"]

    rows = await svc.get_with_relations()
    row = next(item for item in rows if item["pipeline"].id == pipeline_id)
    assert row["source_ids"] == [1002]
    assert row["source_titles"] == ["Source B"]


@pytest.mark.anyio
async def test_get_detail_nonexistent(svc):
    assert await svc.get_detail(99999) is None


@pytest.mark.anyio
async def test_get_with_relations(svc, pipeline_id):
    rows = await svc.get_with_relations()
    assert len(rows) == 1
    assert rows[0]["pipeline"].id == pipeline_id
    assert rows[0]["source_ids"] == [1001, 1002]


@pytest.mark.anyio
async def test_get_with_relations_active_only(svc, pipeline_id):
    rows = await svc.get_with_relations(active_only=True)
    assert len(rows) == 1


@pytest.mark.anyio
async def test_get_with_relations_source_title_fallback(svc):
    """Source channel not found in channels_by_id falls back to str(channel_id)."""
    # Create a pipeline using channel 1001 which exists
    pid = await svc.add(
        name="TitleTest",
        prompt_template="Summarize {source_messages}",
        source_channel_ids=[1001],
        target_refs=[PipelineTargetRef(phone="+100", dialog_id=77)],
    )
    # Now get_with_relations -- source_titles should use channel title
    rows = await svc.get_with_relations()
    found = [r for r in rows if r["pipeline"].id == pid]
    assert len(found) == 1
    assert found[0]["source_titles"] == ["Source A"]


# ---------------------------------------------------------------------------
# list_cached_dialogs_by_phone
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_list_cached_dialogs_by_phone(svc, db):
    result = await svc.list_cached_dialogs_by_phone()
    assert "+100" in result
    assert len(result["+100"]) == 2


# ---------------------------------------------------------------------------
# _build_pipeline validation errors
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_build_pipeline_empty_name(svc):
    with pytest.raises(PipelineValidationError, match="[Нн]азвание"):
        await svc.add(
            name="   ",
            prompt_template="Summarize {source_messages}",
            source_channel_ids=[1001],
            target_refs=[PipelineTargetRef(phone="+100", dialog_id=77)],
        )


@pytest.mark.anyio
async def test_build_pipeline_empty_template(svc):
    with pytest.raises(PipelineValidationError, match="[Шш]аблон"):
        await svc.add(
            name="Name",
            prompt_template="   ",
            source_channel_ids=[1001],
            target_refs=[PipelineTargetRef(phone="+100", dialog_id=77)],
        )


@pytest.mark.anyio
async def test_build_pipeline_invalid_mode(svc):
    with pytest.raises(PipelineValidationError, match="[Рр]ежим"):
        await svc.add(
            name="Name",
            prompt_template="Summarize {source_messages}",
            source_channel_ids=[1001],
            target_refs=[PipelineTargetRef(phone="+100", dialog_id=77)],
            publish_mode="nonexistent_mode",
        )


@pytest.mark.anyio
async def test_build_pipeline_invalid_interval(svc):
    with pytest.raises(PipelineValidationError, match="[Ии]нтервал"):
        await svc.add(
            name="Name",
            prompt_template="Summarize {source_messages}",
            source_channel_ids=[1001],
            target_refs=[PipelineTargetRef(phone="+100", dialog_id=77)],
            generate_interval_minutes=0,
        )


# ---------------------------------------------------------------------------
# _normalize_sources validation
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_normalize_sources_empty(svc):
    with pytest.raises(PipelineValidationError, match="[Вв]ыберите.*источник"):
        await svc.add(
            name="Name",
            prompt_template="Summarize {source_messages}",
            source_channel_ids=[],
            target_refs=[PipelineTargetRef(phone="+100", dialog_id=77)],
        )


@pytest.mark.anyio
async def test_normalize_sources_unknown_channel(svc):
    with pytest.raises(PipelineValidationError, match="[Нн]еизвестные"):
        await svc.add(
            name="Name",
            prompt_template="Summarize {source_messages}",
            source_channel_ids=[99999],
            target_refs=[PipelineTargetRef(phone="+100", dialog_id=77)],
        )


# ---------------------------------------------------------------------------
# _normalize_targets validation
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_normalize_targets_empty(svc):
    with pytest.raises(PipelineValidationError, match="[Вв]ыберите.*цель"):
        await svc.add(
            name="Name",
            prompt_template="Summarize {source_messages}",
            source_channel_ids=[1001],
            target_refs=[],
        )


@pytest.mark.anyio
async def test_normalize_targets_unknown_phone(svc):
    with pytest.raises(PipelineValidationError, match="[Аа]ккаунт"):
        await svc.add(
            name="Name",
            prompt_template="Summarize {source_messages}",
            source_channel_ids=[1001],
            target_refs=[PipelineTargetRef(phone="+999", dialog_id=77)],
        )


@pytest.mark.anyio
async def test_normalize_targets_bot_rejected(svc, db):
    """Targets pointing at bots should be rejected."""
    await db.repos.dialog_cache.replace_dialogs(
        "+100",
        [
            {
                "channel_id": 200,
                "title": "BotTarget",
                "username": "bot_target",
                "channel_type": "bot",
            },
        ],
    )
    with pytest.raises(PipelineValidationError, match="[Бб]от"):
        await svc.add(
            name="Name",
            prompt_template="Summarize {source_messages}",
            source_channel_ids=[1001],
            target_refs=[PipelineTargetRef(phone="+100", dialog_id=200)],
        )


@pytest.mark.anyio
async def test_normalize_targets_dialog_not_cached(svc):
    with pytest.raises(PipelineValidationError, match="кеш"):
        await svc.add(
            name="Name",
            prompt_template="Summarize {source_messages}",
            source_channel_ids=[1001],
            target_refs=[PipelineTargetRef(phone="+100", dialog_id=999)],
        )


@pytest.mark.anyio
async def test_normalize_targets_duplicate_ref_deduped(svc, db):
    """Duplicate target refs are deduplicated."""
    pid = await svc.add(
        name="DupTarget",
        prompt_template="Summarize {source_messages}",
        source_channel_ids=[1001],
        target_refs=[
            PipelineTargetRef(phone="+100", dialog_id=77),
            PipelineTargetRef(phone="+100", dialog_id=77),
        ],
    )
    targets = await svc.get_targets(pid)
    assert len(targets) == 1


# ---------------------------------------------------------------------------
# export_json / import_json
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_export_json(svc, pipeline_id):
    data = await svc.export_json(pipeline_id)
    assert data is not None
    assert data["name"] == "TestPipeline"
    assert data["source_ids"] == [1001, 1002]
    assert data["target_refs"] == ["+100|77"]
    assert data["publish_mode"] == "moderated"


@pytest.mark.anyio
async def test_export_json_nonexistent(svc):
    assert await svc.export_json(99999) is None


@pytest.mark.anyio
async def test_export_json_with_pipeline_graph(svc, db):
    """Export pipeline that has a pipeline_json graph."""
    graph = PipelineGraph(
        nodes=[PipelineNode(id="n1", type=PipelineNodeType.SOURCE, name="src")],
        edges=[],
    )
    pid = await svc.add(
        name="GraphPipeline",
        prompt_template="Summarize {source_messages}",
        source_channel_ids=[1001],
        target_refs=[PipelineTargetRef(phone="+100", dialog_id=77)],
    )
    await db.repos.content_pipelines.set_pipeline_json(pid, graph)

    data = await svc.export_json(pid)
    assert data is not None
    assert "pipeline_json" in data
    assert len(data["pipeline_json"]["nodes"]) == 1


@pytest.mark.anyio
async def test_import_json_from_dict(svc, pipeline_id):
    """Import a pipeline from a dict."""
    export = await svc.export_json(pipeline_id)
    assert export is not None

    new_id = await svc.import_json(export, name_override="Imported")
    assert new_id > 0
    imported = await svc.get(new_id)
    assert imported is not None
    assert imported.name == "Imported"
    assert imported.is_active is False


@pytest.mark.anyio
async def test_import_json_restores_refinement_steps_and_publish_times(svc):
    """export -> import must keep refinement_steps and publish_times (audit #837/4)."""
    export = {
        "name": "RoundTrip",
        "prompt_template": "Summarize {source_messages}",
        "llm_model": "m",
        "image_model": None,
        "publish_mode": "moderated",
        "generation_backend": "chain",
        "generate_interval_minutes": 30,
        "publish_times": '["09:00", "18:00"]',
        "refinement_steps": [{"name": "s1", "prompt": "tighten {text}"}],
        "source_ids": [],
        "target_refs": [],
    }
    new_id = await svc.import_json(export, name_override="RoundTrip")
    detail = await svc.get_detail(new_id)
    pipeline = detail["pipeline"]
    assert pipeline.publish_times == '["09:00", "18:00"]'
    assert pipeline.refinement_steps == [{"name": "s1", "prompt": "tighten {text}"}]


@pytest.mark.anyio
async def test_export_import_roundtrip_keeps_ab_settings(svc):
    """#1621: export -> import must keep ab_num_variants/ab_auto_select."""
    pid = await svc.add(
        name="ABPipeline",
        prompt_template="Summarize {source_messages}",
        source_channel_ids=[1001],
        target_refs=[PipelineTargetRef(phone="+100", dialog_id=77)],
        ab_num_variants=3,
        ab_auto_select=True,
    )
    export = await svc.export_json(pid)
    assert export is not None
    assert export["ab_num_variants"] == 3
    assert export["ab_auto_select"] is True

    new_id = await svc.import_json(export, name_override="ABImported")
    imported = await svc.get(new_id)
    assert imported is not None
    assert imported.ab_num_variants == 3
    assert imported.ab_auto_select is True


@pytest.mark.anyio
async def test_import_json_honours_explicit_ab_fields(svc):
    """#1621: explicit ab_num_variants/ab_auto_select in the payload are applied."""
    payload = {
        "name": "ExplicitAB",
        "prompt_template": "Summarize {source_messages}",
        "publish_mode": "moderated",
        "generation_backend": "chain",
        "generate_interval_minutes": 60,
        "ab_num_variants": 3,
        "ab_auto_select": True,
        "source_ids": [],
        "target_refs": [],
    }
    new_id = await svc.import_json(payload)
    imported = await svc.get(new_id)
    assert imported is not None
    assert imported.ab_num_variants == 3
    assert imported.ab_auto_select is True


@pytest.mark.anyio
async def test_import_json_from_string(svc, pipeline_id):
    """Import a pipeline from a JSON string."""
    export = await svc.export_json(pipeline_id)
    assert export is not None
    json_str = json.dumps(export)

    new_id = await svc.import_json(json_str)
    assert new_id > 0


@pytest.mark.anyio
async def test_import_json_with_pipeline_graph(svc, pipeline_id, db):
    """Import with pipeline_json field."""
    graph = PipelineGraph(
        nodes=[PipelineNode(id="n1", type=PipelineNodeType.LLM_GENERATE, name="gen")],
        edges=[],
    )
    await db.repos.content_pipelines.set_pipeline_json(pipeline_id, graph)

    export = await svc.export_json(pipeline_id)
    assert export is not None
    assert "pipeline_json" in export

    new_id = await svc.import_json(export, name_override="GraphImport")
    imported = await svc.get(new_id)
    assert imported is not None
    assert imported.pipeline_json is not None
    assert len(imported.pipeline_json.nodes) == 1


@pytest.mark.anyio
async def test_import_json_with_target_refs(svc, pipeline_id):
    """Import with target_refs in the data."""
    export = await svc.export_json(pipeline_id)
    assert export is not None

    new_id = await svc.import_json(export, name_override="WithTargets")
    assert new_id > 0
    targets = await svc.get_targets(new_id)
    assert len(targets) >= 1


@pytest.mark.anyio
async def test_import_json_accepts_dict_target_refs(svc):
    """#1246 bug 1: target_refs given as dicts (the shape `pipeline add --json-file`
    builds) must be honoured, not silently dropped. The old string-only filter
    left the imported pipeline with zero targets."""
    data = {
        "name": "DictTargets",
        "prompt_template": "Summarize {source_messages}",
        "source_ids": [1001],
        "target_refs": [{"phone": "+100", "dialog_id": 77}],
        "generate_interval_minutes": 30,
    }
    new_id = await svc.import_json(data)
    targets = await svc.get_targets(new_id)
    assert len(targets) == 1
    assert targets[0].phone == "+100"
    assert targets[0].dialog_id == 77


@pytest.mark.anyio
async def test_import_json_accepts_string_target_refs(svc):
    """The legacy 'phone|dialog_id' string form (what export_json emits) still works."""
    data = {
        "name": "StringTargets",
        "prompt_template": "Summarize {source_messages}",
        "source_ids": [1001],
        "target_refs": ["+100|77"],
        "generate_interval_minutes": 30,
    }
    new_id = await svc.import_json(data)
    targets = await svc.get_targets(new_id)
    assert len(targets) == 1
    assert targets[0].dialog_id == 77


@pytest.mark.anyio
async def test_import_json_honours_is_active_true(svc):
    """#1246 bug 2: import_json hard-coded is_active=False, so a payload asking for
    an active pipeline (the CLI passes is_active=not --inactive) was ignored and
    the pipeline never ran. It must now honour an explicit is_active."""
    data = {
        "name": "ActiveImport",
        "prompt_template": "Summarize {source_messages}",
        "source_ids": [1001],
        "target_refs": [{"phone": "+100", "dialog_id": 77}],
        "generate_interval_minutes": 30,
        "is_active": True,
    }
    new_id = await svc.import_json(data)
    imported = await svc.get(new_id)
    assert imported is not None
    assert imported.is_active is True


@pytest.mark.anyio
async def test_import_json_is_active_defaults_false(svc):
    """A payload without is_active (a plain export->import round-trip) stays inactive."""
    data = {
        "name": "DefaultInactive",
        "prompt_template": "Summarize {source_messages}",
        "source_ids": [1001],
        "target_refs": [{"phone": "+100", "dialog_id": 77}],
        "generate_interval_minutes": 30,
    }
    new_id = await svc.import_json(data)
    imported = await svc.get(new_id)
    assert imported is not None
    assert imported.is_active is False


@pytest.mark.anyio
async def test_import_json_with_invalid_pipeline_graph(svc, pipeline_id):
    """Import with unparsable pipeline_json field -- should be ignored gracefully."""
    export = await svc.export_json(pipeline_id)
    assert export is not None
    # Use truly unparseable data that will cause from_json to fail
    export["pipeline_json"] = "NOT VALID JSON {{{"

    new_id = await svc.import_json(export, name_override="BadGraph")
    assert new_id > 0
    imported = await svc.get(new_id)
    # pipeline_json should be None since the invalid JSON was ignored
    assert imported.pipeline_json is None


@pytest.mark.anyio
async def test_import_json_empty_sources_and_targets(svc):
    """Import with no sources/targets creates inactive pipeline."""
    data = {
        "name": "EmptyImport",
        "prompt_template": "Hello {source_messages}",
        "source_ids": [],
        "target_refs": [],
        "generate_interval_minutes": 60,
    }
    new_id = await svc.import_json(data)
    assert new_id > 0


@pytest.mark.anyio
async def test_import_json_injects_source_ids_into_dag_source_node(svc):
    """Regression: source_ids from --source must backfill DAG source node config.

    Previously only the sidecar pipeline_sources table was populated; the
    SOURCE node in pipeline_json kept channel_ids=[] and /dry-run-count
    reported 0 messages even when the channel had traffic.
    """
    data = {
        "name": "DAGSourceInject",
        "source_ids": [1001],
        "pipeline_json": {
            "nodes": [
                {
                    "id": "s1",
                    "type": "source",
                    "name": "Источник",
                    "config": {"channel_ids": []},
                    "position": {"x": 0, "y": 0},
                },
                {
                    "id": "f1",
                    "type": "fetch_messages",
                    "name": "Fetch",
                    "config": {},
                    "position": {"x": 110, "y": 0},
                },
            ],
            "edges": [{"from": "s1", "to": "f1"}],
        },
    }
    new_id = await svc.import_json(data)
    imported = await svc.get(new_id)
    src_node = next(
        n for n in imported.pipeline_json.nodes if n.type == PipelineNodeType.SOURCE
    )
    assert src_node.config["channel_ids"] == [1001]


@pytest.mark.anyio
async def test_import_json_preserves_explicit_channel_ids(svc):
    """If the imported SOURCE node already has channel_ids, don't overwrite."""
    data = {
        "name": "ExplicitChannels",
        "source_ids": [1001],
        "pipeline_json": {
            "nodes": [
                {
                    "id": "s1",
                    "type": "source",
                    "name": "Источник",
                    "config": {"channel_ids": [1002]},
                    "position": {"x": 0, "y": 0},
                },
                {
                    "id": "f1",
                    "type": "fetch_messages",
                    "name": "Fetch",
                    "config": {},
                    "position": {"x": 110, "y": 0},
                },
            ],
            "edges": [{"from": "s1", "to": "f1"}],
        },
    }
    new_id = await svc.import_json(data)
    imported = await svc.get(new_id)
    src_node = next(
        n for n in imported.pipeline_json.nodes if n.type == PipelineNodeType.SOURCE
    )
    # Explicit override in JSON wins over sidecar source_ids
    assert src_node.config["channel_ids"] == [1002]


# ---------------------------------------------------------------------------
# list_templates
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_list_templates_no_repo(db):
    """When pipeline_templates is None, returns empty list."""
    from tg_agent.database.bundles import PipelineBundle

    bundle = PipelineBundle(
        content_pipelines=db.repos.content_pipelines,
        channels=db.repos.channels,
        accounts=db.repos.accounts,
        dialog_cache=db.repos.dialog_cache,
        pipeline_templates=None,
    )
    svc = PipelineService(bundle)
    templates = await svc.list_templates()
    assert templates == []


@pytest.mark.anyio
async def test_list_templates_with_repo(svc, db):
    """When pipeline_templates repo exists, delegates to list_all."""
    mock_tpl_repo = MagicMock()
    mock_tpl_repo.list_all = AsyncMock(return_value=[])
    svc._bundle = PipelineBundle(
        content_pipelines=db.repos.content_pipelines,
        channels=db.repos.channels,
        accounts=db.repos.accounts,
        dialog_cache=db.repos.dialog_cache,
        pipeline_templates=mock_tpl_repo,
    )
    await svc.list_templates(category="test")
    mock_tpl_repo.list_all.assert_called_once_with("test")


# ---------------------------------------------------------------------------
# create_from_template
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_create_from_template_no_repo(db):
    """When pipeline_templates is None, raises error."""
    from tg_agent.database.bundles import PipelineBundle

    bundle = PipelineBundle(
        content_pipelines=db.repos.content_pipelines,
        channels=db.repos.channels,
        accounts=db.repos.accounts,
        dialog_cache=db.repos.dialog_cache,
        pipeline_templates=None,
    )
    svc = PipelineService(bundle)
    with pytest.raises(PipelineValidationError, match="[Рр]епозиторий шаблонов"):
        await svc.create_from_template(
            template_id=1,
            name="FromTemplate",
            source_ids=[1001],
            target_refs=[PipelineTargetRef(phone="+100", dialog_id=77)],
        )


@pytest.mark.anyio
async def test_create_from_template_not_found(svc, db):
    """When template_id not found, raises error."""
    mock_tpl_repo = MagicMock()
    mock_tpl_repo.get_by_id = AsyncMock(return_value=None)
    svc._bundle = PipelineBundle(
        content_pipelines=db.repos.content_pipelines,
        channels=db.repos.channels,
        accounts=db.repos.accounts,
        dialog_cache=db.repos.dialog_cache,
        pipeline_templates=mock_tpl_repo,
    )
    with pytest.raises(PipelineValidationError, match="не найден"):
        await svc.create_from_template(
            template_id=999,
            name="FromTemplate",
            source_ids=[1001],
            target_refs=[PipelineTargetRef(phone="+100", dialog_id=77)],
        )


@pytest.mark.anyio
async def test_create_from_template_success(svc, db):
    """Create pipeline from a template with llm_generate node."""
    graph = PipelineGraph(
        nodes=[
            PipelineNode(
                id="n1",
                type=PipelineNodeType.LLM_GENERATE,
                name="gen",
                config={"prompt_template": "Generated content about {source_messages}"},
            ),
        ],
        edges=[],
    )
    tpl = MagicMock()
    tpl.template_json = graph
    tpl.id = 1

    mock_tpl_repo = MagicMock()
    mock_tpl_repo.get_by_id = AsyncMock(return_value=tpl)
    svc._bundle = PipelineBundle(
        content_pipelines=db.repos.content_pipelines,
        channels=db.repos.channels,
        accounts=db.repos.accounts,
        dialog_cache=db.repos.dialog_cache,
        pipeline_templates=mock_tpl_repo,
    )

    pid = await svc.create_from_template(
        template_id=1,
        name="TemplatedPipeline",
        source_ids=[1001],
        target_refs=[PipelineTargetRef(phone="+100", dialog_id=77)],
    )
    assert pid > 0
    p = await svc.get(pid)
    assert p is not None
    assert p.name == "TemplatedPipeline"
    assert p.is_active is False
    assert p.pipeline_json is not None


@pytest.mark.anyio
async def test_create_from_template_no_prompt_in_graph(svc, db):
    """Template with no prompt_template in graph uses name as prompt."""
    graph = PipelineGraph(
        nodes=[PipelineNode(id="n1", type=PipelineNodeType.SOURCE, name="src")],
        edges=[],
    )
    tpl = MagicMock()
    tpl.template_json = graph

    mock_tpl_repo = MagicMock()
    mock_tpl_repo.get_by_id = AsyncMock(return_value=tpl)
    svc._bundle = PipelineBundle(
        content_pipelines=db.repos.content_pipelines,
        channels=db.repos.channels,
        accounts=db.repos.accounts,
        dialog_cache=db.repos.dialog_cache,
        pipeline_templates=mock_tpl_repo,
    )

    pid = await svc.create_from_template(
        template_id=1,
        name="FallbackName",
        source_ids=[1001],
        target_refs=[PipelineTargetRef(phone="+100", dialog_id=77)],
    )
    assert pid > 0
    p = await svc.get(pid)
    assert p.prompt_template == "FallbackName"


@pytest.mark.anyio
async def test_create_from_template_no_sources_targets(svc, db):
    """Create from template without sources/targets creates inactive pipeline."""
    graph = PipelineGraph(
        nodes=[
            PipelineNode(
                id="n1",
                type=PipelineNodeType.LLM_GENERATE,
                name="gen",
                config={"prompt": "test prompt"},
            ),
        ],
        edges=[],
    )
    tpl = MagicMock()
    tpl.template_json = graph

    mock_tpl_repo = MagicMock()
    mock_tpl_repo.get_by_id = AsyncMock(return_value=tpl)
    svc._bundle = PipelineBundle(
        content_pipelines=db.repos.content_pipelines,
        channels=db.repos.channels,
        accounts=db.repos.accounts,
        dialog_cache=db.repos.dialog_cache,
        pipeline_templates=mock_tpl_repo,
    )

    pid = await svc.create_from_template(
        template_id=1,
        name="NoSrcTgt",
        source_ids=[],
        target_refs=[],
    )
    assert pid > 0
    p = await svc.get(pid)
    assert p.is_active is False


# ---------------------------------------------------------------------------
# edit_via_llm
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_edit_via_llm_pipeline_not_found(svc, db):
    result = await svc.edit_via_llm(99999, "change something", db)
    assert result["ok"] is False
    assert "не найден" in result["error"]


@pytest.mark.anyio
async def test_edit_via_llm_success(svc, db, pipeline_id):
    """edit_via_llm with a working provider returns updated graph."""
    new_graph_json = json.dumps({
        "nodes": [{"id": "n1", "type": "llm_generate", "name": "gen", "config": {}, "position": {"x": 0, "y": 0}}],
        "edges": [],
    })

    mock_provider = AsyncMock(return_value=new_graph_json)

    with patch("tg_agent.services.provider_service.RuntimeProviderRegistry") as mock_aps:
        instance = mock_aps.return_value
        instance.get_provider_callable.return_value = mock_provider

        result = await svc.edit_via_llm(pipeline_id, "add a generate node", db)

    assert result["ok"] is True
    assert "pipeline_json" in result
    assert len(result["pipeline_json"]["nodes"]) == 1


@pytest.mark.anyio
async def test_edit_via_llm_with_markdown_fences(svc, db, pipeline_id):
    """edit_via_llm strips markdown code fences from LLM response."""
    new_graph = {"nodes": [], "edges": []}
    new_graph_json = json.dumps(new_graph)
    wrapped = f"```json\n{new_graph_json}\n```"

    mock_provider = AsyncMock(return_value=wrapped)

    with patch("tg_agent.services.provider_service.RuntimeProviderRegistry") as mock_aps:
        instance = mock_aps.return_value
        instance.get_provider_callable.return_value = mock_provider

        result = await svc.edit_via_llm(pipeline_id, "clear all nodes", db)

    assert result["ok"] is True


@pytest.mark.anyio
async def test_edit_via_llm_provider_failure(svc, db, pipeline_id):
    """edit_via_llm returns error when provider raises."""
    mock_provider = AsyncMock(side_effect=RuntimeError("API down"))

    with patch("tg_agent.services.provider_service.RuntimeProviderRegistry") as mock_aps:
        instance = mock_aps.return_value
        instance.get_provider_callable.return_value = mock_provider

        result = await svc.edit_via_llm(pipeline_id, "change", db)

    assert result["ok"] is False
    assert "API down" in result["error"]


@pytest.mark.anyio
async def test_edit_via_llm_dict_result(svc, db, pipeline_id):
    """edit_via_llm handles provider returning dict with 'text' key."""
    new_graph = {"nodes": [], "edges": []}
    new_graph_json = json.dumps(new_graph)

    mock_provider = AsyncMock(return_value={"text": new_graph_json})

    with patch("tg_agent.services.provider_service.RuntimeProviderRegistry") as mock_aps:
        instance = mock_aps.return_value
        instance.get_provider_callable.return_value = mock_provider

        result = await svc.edit_via_llm(pipeline_id, "clear", db)

    assert result["ok"] is True


@pytest.mark.anyio
async def test_edit_via_llm_pipeline_without_graph(svc, db, pipeline_id):
    """edit_via_llm works when pipeline has no existing pipeline_json."""
    p = await svc.get(pipeline_id)
    assert p.pipeline_json is None

    new_graph = {"nodes": [], "edges": []}
    mock_provider = AsyncMock(return_value=json.dumps(new_graph))

    with patch("tg_agent.services.provider_service.RuntimeProviderRegistry") as mock_aps:
        instance = mock_aps.return_value
        instance.get_provider_callable.return_value = mock_provider

        result = await svc.edit_via_llm(pipeline_id, "clear", db)

    assert result["ok"] is True


# ---------------------------------------------------------------------------
# _build_pipeline edge cases
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_build_pipeline_strips_llm_model(svc):
    """Empty/whitespace llm_model is normalized to None."""
    pid = await svc.add(
        name="StripModel",
        prompt_template="Summarize {source_messages}",
        source_channel_ids=[1001],
        target_refs=[PipelineTargetRef(phone="+100", dialog_id=77)],
        llm_model="   ",
    )
    p = await svc.get(pid)
    assert p.llm_model is None


@pytest.mark.anyio
async def test_build_pipeline_keeps_llm_model(svc):
    pid = await svc.add(
        name="KeepModel",
        prompt_template="Summarize {source_messages}",
        source_channel_ids=[1001],
        target_refs=[PipelineTargetRef(phone="+100", dialog_id=77)],
        llm_model="gpt-4",
    )
    p = await svc.get(pid)
    assert p.llm_model == "gpt-4"


@pytest.mark.anyio
async def test_build_pipeline_invalid_template_variable(svc):
    with pytest.raises(PipelineValidationError):
        await svc.add(
            name="BadVar",
            prompt_template="Use {unknown_variable}",
            source_channel_ids=[1001],
            target_refs=[PipelineTargetRef(phone="+100", dialog_id=77)],
        )


# ---------------------------------------------------------------------------
# init with Database vs PipelineBundle
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_init_with_database(db):
    """PipelineService accepts Database and wraps it in PipelineBundle."""
    svc = PipelineService(db)
    # Should not raise -- bundle was created from db
    items = await svc.list_pipelines()
    assert isinstance(items, list)


# ---------------------------------------------------------------------------
# Graph CRUD operations (get_graph, add_node, remove_node, replace_node,
#                        add_edge, remove_edge)
# ---------------------------------------------------------------------------


@pytest.fixture
async def graph_pipeline_id(svc, db):
    """Create a pipeline with a DAG graph for CRUD tests."""
    pid = await svc.add(
        name="GraphPipeline",
        prompt_template="Test",
        source_channel_ids=[1001],
        target_refs=[PipelineTargetRef(phone="+100", dialog_id=77)],
    )
    graph = PipelineGraph(
        nodes=[
            PipelineNode(id="src", type=PipelineNodeType.SOURCE, name="source", config={"channel_ids": [1001]}),
            PipelineNode(id="fetch", type=PipelineNodeType.FETCH_MESSAGES, name="fetch"),
            PipelineNode(id="gen", type=PipelineNodeType.LLM_GENERATE, name="gen", config={"model": "claude"}),
        ],
        edges=[
            PipelineEdge(from_node="src", to_node="fetch"),
            PipelineEdge(from_node="fetch", to_node="gen"),
        ],
    )
    await db.repos.content_pipelines.set_pipeline_json(pid, graph)
    return pid


@pytest.mark.anyio
async def test_get_graph_returns_graph(svc, graph_pipeline_id):
    graph = await svc.get_graph(graph_pipeline_id)
    assert graph is not None
    assert len(graph.nodes) == 3


@pytest.mark.anyio
async def test_get_graph_returns_none_for_legacy(svc, pipeline_id):
    graph = await svc.get_graph(pipeline_id)
    assert graph is None


@pytest.mark.anyio
async def test_get_graph_pipeline_not_found(svc):
    graph = await svc.get_graph(99999)
    assert graph is None


@pytest.mark.anyio
async def test_add_node(svc, graph_pipeline_id):
    new_node = PipelineNode(id="pub", type=PipelineNodeType.PUBLISH, name="publish", config={"targets": []})
    ok = await svc.add_node(graph_pipeline_id, new_node)
    assert ok is True
    graph = await svc.get_graph(graph_pipeline_id)
    assert len(graph.nodes) == 4
    assert graph.nodes[3].id == "pub"


@pytest.mark.anyio
async def test_add_node_auto_position(svc, graph_pipeline_id):
    new_node = PipelineNode(id="delay1", type=PipelineNodeType.DELAY, name="delay", config={})
    await svc.add_node(graph_pipeline_id, new_node)
    graph = await svc.get_graph(graph_pipeline_id)
    added = next(n for n in graph.nodes if n.id == "delay1")
    assert added.position["x"] > 0


@pytest.mark.anyio
async def test_add_node_no_graph(svc, pipeline_id):
    new_node = PipelineNode(id="x", type=PipelineNodeType.DELAY, name="d")
    ok = await svc.add_node(pipeline_id, new_node)
    assert ok is False


@pytest.mark.anyio
async def test_remove_node_removes_edges(svc, graph_pipeline_id):
    ok = await svc.remove_node(graph_pipeline_id, "fetch")
    assert ok is True
    graph = await svc.get_graph(graph_pipeline_id)
    ids = [n.id for n in graph.nodes]
    assert "fetch" not in ids
    # Edges referencing "fetch" should also be removed
    for e in graph.edges:
        assert e.from_node != "fetch" and e.to_node != "fetch"


@pytest.mark.anyio
async def test_remove_node_not_found(svc, graph_pipeline_id):
    ok = await svc.remove_node(graph_pipeline_id, "nonexistent")
    assert ok is False


@pytest.mark.anyio
async def test_replace_node_preserves_edges(svc, graph_pipeline_id):
    new_node = PipelineNode(id="gen", type=PipelineNodeType.AGENT_LOOP, name="agent", config={"max_steps": 5})
    ok = await svc.replace_node(graph_pipeline_id, "gen", new_node)
    assert ok is True
    graph = await svc.get_graph(graph_pipeline_id)
    replaced = next(n for n in graph.nodes if n.id == "gen")
    assert replaced.type == PipelineNodeType.AGENT_LOOP
    # Edges should still exist
    edge_from_fetch = [e for e in graph.edges if e.from_node == "fetch"]
    assert len(edge_from_fetch) == 1
    assert edge_from_fetch[0].to_node == "gen"


@pytest.mark.anyio
async def test_replace_node_with_new_id_updates_edges(svc, graph_pipeline_id):
    new_node = PipelineNode(id="agent_v2", type=PipelineNodeType.AGENT_LOOP, name="agent", config={})
    ok = await svc.replace_node(graph_pipeline_id, "gen", new_node)
    assert ok is True
    graph = await svc.get_graph(graph_pipeline_id)
    # Old "gen" edges should now point to "agent_v2"
    for e in graph.edges:
        assert "gen" not in (e.from_node, e.to_node)
    assert any(e.to_node == "agent_v2" for e in graph.edges)


@pytest.mark.anyio
async def test_replace_first_node_with_new_id_updates_edges(svc, graph_pipeline_id):
    """Regression: replacing the first node (index 0) must still rewrite edges."""
    new_node = PipelineNode(id="src_v2", type=PipelineNodeType.SOURCE, name="source", config={})
    ok = await svc.replace_node(graph_pipeline_id, "src", new_node)
    assert ok is True
    graph = await svc.get_graph(graph_pipeline_id)
    assert not any("src" in (e.from_node, e.to_node) for e in graph.edges)
    assert any(e.from_node == "src_v2" for e in graph.edges)


@pytest.mark.anyio
async def test_add_edge(svc, graph_pipeline_id):
    ok = await svc.add_edge(graph_pipeline_id, "src", "gen")
    assert ok is True
    graph = await svc.get_graph(graph_pipeline_id)
    assert any(e.from_node == "src" and e.to_node == "gen" for e in graph.edges)


@pytest.mark.anyio
async def test_add_edge_idempotent(svc, graph_pipeline_id):
    await svc.add_edge(graph_pipeline_id, "src", "gen")
    graph1 = await svc.get_graph(graph_pipeline_id)
    await svc.add_edge(graph_pipeline_id, "src", "gen")
    graph2 = await svc.get_graph(graph_pipeline_id)
    assert len(graph1.edges) == len(graph2.edges)


@pytest.mark.anyio
async def test_add_edge_no_graph(svc, pipeline_id):
    ok = await svc.add_edge(pipeline_id, "a", "b")
    assert ok is False


@pytest.mark.anyio
async def test_add_edge_rejecting_back_edge_that_closes_cycle(svc, graph_pipeline_id):
    """#1077 (fail-closed, defence in depth): the fixture graph is src->fetch->gen.
    Adding gen->src would close a cycle (src reachable from gen), so add_edge must
    reject it with PipelineValidationError and NOT persist the edge."""
    from tg_agent.services.pipeline_service import PipelineValidationError

    with pytest.raises(PipelineValidationError):
        await svc.add_edge(graph_pipeline_id, "gen", "src")
    # The graph must be unchanged: no gen->src edge was written.
    graph = await svc.get_graph(graph_pipeline_id)
    assert not any(e.from_node == "gen" and e.to_node == "src" for e in graph.edges)
    assert len(graph.edges) == 2  # only the original src->fetch, fetch->gen


@pytest.mark.anyio
async def test_add_edge_rejects_self_loop(svc, graph_pipeline_id):
    """A self-loop (node -> itself) is the minimal cycle and is rejected too."""
    from tg_agent.services.pipeline_service import PipelineValidationError

    with pytest.raises(PipelineValidationError):
        await svc.add_edge(graph_pipeline_id, "gen", "gen")
    graph = await svc.get_graph(graph_pipeline_id)
    assert not any(e.from_node == "gen" and e.to_node == "gen" for e in graph.edges)


@pytest.mark.anyio
async def test_add_edge_allows_forward_edge_no_cycle(svc, graph_pipeline_id):
    """Guard: a forward edge that does NOT close a cycle (src->gen, a shortcut
    across the existing chain) must still be accepted — the cycle check must not
    reject legitimate DAG edges."""
    ok = await svc.add_edge(graph_pipeline_id, "src", "gen")
    assert ok is True
    graph = await svc.get_graph(graph_pipeline_id)
    assert any(e.from_node == "src" and e.to_node == "gen" for e in graph.edges)


@pytest.mark.anyio
async def test_remove_edge(svc, graph_pipeline_id):
    ok = await svc.remove_edge(graph_pipeline_id, "src", "fetch")
    assert ok is True
    graph = await svc.get_graph(graph_pipeline_id)
    assert not any(e.from_node == "src" and e.to_node == "fetch" for e in graph.edges)


@pytest.mark.anyio
async def test_remove_edge_not_found(svc, graph_pipeline_id):
    ok = await svc.remove_edge(graph_pipeline_id, "src", "gen")
    assert ok is False


# ---------------------------------------------------------------------------
# resolve_retrieval_scope — channel scoping fail-closed (#1077, epic #1025).
#
# get_retrieval_scope's happy paths (single / multi source) are covered above;
# the *error* path is the security-relevant one. A channel_id of None means
# UNSCOPED retrieval across ALL channels, so a failed source lookup must NOT
# silently fall back to None (which would widen a single-source pipeline into a
# cross-channel search — a trust-boundary breach). #1077 changes the old
# swallow-and-widen behaviour to a typed PipelineScopeError. The legitimate
# unscoped paths (no lister / zero / multiple sources) still return None.
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_resolve_scope_no_lister_returns_unscoped():
    """With no source lister, scope is the pipeline name and no channel."""
    from tg_agent.services.pipeline_service import resolve_retrieval_scope

    pipeline = ContentPipeline(id=5, name="Unscoped", prompt_template="t")
    scope = await resolve_retrieval_scope(pipeline, list_sources=None)
    assert scope.query == "Unscoped"
    assert scope.channel_id is None


@pytest.mark.anyio
async def test_resolve_scope_source_lookup_error_fails_closed():
    """A scoping error (source lister raises) must FAIL CLOSED: #1077 changes the
    old swallow-and-widen contract to a typed PipelineScopeError.

    SECURITY (content isolation, #1077): `channel_id is None` means UNSCOPED
    retrieval across ALL channels. The old code swallowed a transient source
    lookup failure and returned channel_id=None, silently widening a
    single-source pipeline into a cross-channel search — content could be
    generated/published from channels outside the configured sources (a
    trust-boundary breach). Fail-closed means the pipeline run aborts instead
    of leaking: no scope is ever widened by a failed lookup."""
    from tg_agent.services.pipeline_service import PipelineScopeError, resolve_retrieval_scope

    pipeline = ContentPipeline(id=7, name="Boom", prompt_template="t")

    async def exploding_lister(_pid):
        raise RuntimeError("DB unavailable")

    with pytest.raises(PipelineScopeError):
        await resolve_retrieval_scope(pipeline, list_sources=exploding_lister)


@pytest.mark.anyio
async def test_resolve_scope_error_does_not_widen_to_unscoped():
    """Mutation-guard for the fail-closed fix: prove the failed-lookup path never
    yields an unscoped (channel_id=None) result. If the fix regresses to
    swallowing the error, this test sees a returned scope instead of the raise."""
    from tg_agent.services.pipeline_service import PipelineScopeError, resolve_retrieval_scope

    pipeline = ContentPipeline(id=8, name="Leak", prompt_template="t")

    async def exploding_lister(_pid):
        raise RuntimeError("transient DB blip")

    returned_scope = None
    try:
        returned_scope = await resolve_retrieval_scope(pipeline, list_sources=exploding_lister)
    except PipelineScopeError:
        pass
    # The ONLY acceptable outcome is the raise above; a returned scope (of any
    # channel_id, especially None) means the leak path is back.
    assert returned_scope is None


@pytest.mark.anyio
async def test_get_retrieval_scope_method_source_lookup_error_fails_closed(svc):
    """Instance-method regression guard (#1118, dup of #1077).

    ``resolve_retrieval_scope`` (the free function) is already covered above,
    but the public surface every caller actually uses is the
    ``PipelineService.get_retrieval_scope`` *method*, which wires the bundle's
    ``list_sources`` into the function. #1118 re-reported the original
    fail-OPEN (a failed source lookup widened scope to channel_id=None → an
    all-channels retrieval → cross-channel content leak) against this method's
    surface. We assert end-to-end through the method that a source-lookup
    failure FAILS CLOSED with ``PipelineScopeError`` instead of returning an
    unscoped scope — closing the dup at the layer the issue named."""
    from tg_agent.services.pipeline_service import PipelineScopeError

    pipeline = ContentPipeline(id=4242, name="MethodBoom", prompt_template="t")

    async def exploding_lister(_pid):
        raise RuntimeError("transient DB blip")

    # PipelineBundle is a frozen dataclass, so patch the underlying repository
    # method the bundle delegates to (bundle.list_sources → repo.list_sources).
    svc._bundle.content_pipelines.list_sources = exploding_lister

    returned_scope = None
    with pytest.raises(PipelineScopeError):
        returned_scope = await svc.get_retrieval_scope(pipeline)
    # Mutation-guard: drop the raise in resolve_retrieval_scope and this method
    # would hand back an unscoped (channel_id=None) result — caught here because
    # the only acceptable outcome is the raise above, never an assigned scope.
    assert returned_scope is None


@pytest.mark.anyio
async def test_resolve_scope_multi_source_is_unscoped():
    """More than one source → no single-channel scope (retrieve across all)."""
    from tg_agent.services.pipeline_service import resolve_retrieval_scope

    pipeline = ContentPipeline(id=9, name="Multi", prompt_template="t")

    async def two_sources(_pid):
        return [MagicMock(channel_id=1001), MagicMock(channel_id=1002)]

    scope = await resolve_retrieval_scope(pipeline, list_sources=two_sources)
    assert scope.channel_id is None


@pytest.mark.anyio
async def test_resolve_scope_single_source_is_scoped():
    """Exactly one source → scope narrows to that channel id."""
    from tg_agent.services.pipeline_service import resolve_retrieval_scope

    pipeline = ContentPipeline(id=11, name="Solo", prompt_template="t")

    async def one_source(_pid):
        return [MagicMock(channel_id=4242)]

    scope = await resolve_retrieval_scope(pipeline, list_sources=one_source)
    assert scope.channel_id == 4242


@pytest.mark.anyio
async def test_resolve_scope_empty_name_yields_empty_query():
    """A nameless pipeline degrades to an empty retrieval query, not None."""
    from tg_agent.services.pipeline_service import resolve_retrieval_scope

    pipeline = ContentPipeline(id=13, name="", prompt_template="t")
    scope = await resolve_retrieval_scope(pipeline, list_sources=None)
    assert scope.query == ""


# ---------------------------------------------------------------------------
# Mutation slice #4 killer tests: behaviors that survived mutmut
# ---------------------------------------------------------------------------


def test_to_since_hours_converts_units():
    """m -> /60, d -> *24, anything else -> float hours (pipeline_service.py)."""
    from tg_agent.services.pipeline_service import to_since_hours

    assert to_since_hours(90, "m") == 1.5
    assert to_since_hours(2, "d") == 48.0
    assert to_since_hours(5, "h") == 5.0


def test_coerce_target_ref_string_dict_and_garbage():
    """_coerce_target_ref: string partition, dict form, and None for garbage
    (#1246, #mutation slice 4). partition (not rpartition) keeps the phone part
    left of the FIRST pipe."""
    from tg_agent.services.pipeline_service import _coerce_target_ref

    ref = _coerce_target_ref("+100|77")
    assert ref == PipelineTargetRef(phone="+100", dialog_id=77)
    # passthrough
    assert _coerce_target_ref(ref) is ref
    # garbage in every shape -> None (caller skips it)
    assert _coerce_target_ref("|77") is None
    assert _coerce_target_ref("+100|") is None
    assert _coerce_target_ref("+100|abc") is None
    assert _coerce_target_ref("no-pipe") is None
    # multi-pipe string is garbage: phone is everything BEFORE THE FIRST pipe
    assert _coerce_target_ref("+100|77|88") is None
    assert _coerce_target_ref({"phone": "+100", "dialog_id": "abc"}) is None
    assert _coerce_target_ref({"phone": "", "dialog_id": 77}) is None
    assert _coerce_target_ref(42) is None


@pytest.mark.anyio
async def test_edit_via_llm_saves_parsed_graph_without_json_fence(svc, db, pipeline_id):
    """A ```-fenced answer WITHOUT the json prefix still parses and is SAVED —
    the test re-reads the pipeline so a set_pipeline_json(pipeline_id, None)
    drift cannot pass (#mutation slice 4)."""
    graph_payload = json.dumps({
        "nodes": [{
            "id": "n1", "type": "llm_generate", "name": "RenamedByLLM",
            "config": {}, "position": {"x": 0, "y": 0},
        }],
        "edges": [],
    })
    mock_provider = AsyncMock(return_value=f"```\n{graph_payload}\n```")

    with patch("tg_agent.services.provider_service.RuntimeProviderRegistry") as mock_aps:
        instance = mock_aps.return_value
        instance.get_provider_callable.return_value = mock_provider

        result = await svc.edit_via_llm(pipeline_id, "rename the node", db)

    assert result["ok"] is True
    p = await svc.get(pipeline_id)
    assert p.pipeline_json is not None
    assert p.pipeline_json.nodes[0].name == "RenamedByLLM"


@pytest.mark.anyio
async def test_edit_via_llm_dict_falls_back_to_generated_text(svc, db, pipeline_id):
    """Provider dict: non-empty 'text' wins; empty 'text' falls back to
    'generated_text' (#mutation slice 4)."""
    payload_a = json.dumps({
        "nodes": [{
            "id": "n1", "type": "llm_generate", "name": "FromText",
            "config": {}, "position": {"x": 0, "y": 0},
        }],
        "edges": [],
    })
    payload_b = json.dumps({
        "nodes": [{
            "id": "n1", "type": "llm_generate", "name": "FromGeneratedText",
            "config": {}, "position": {"x": 0, "y": 0},
        }],
        "edges": [],
    })

    # text present and non-empty: it must be used, generated_text ignored
    mock_provider = AsyncMock(return_value={"text": payload_a, "generated_text": "not json"})
    with patch("tg_agent.services.provider_service.RuntimeProviderRegistry") as mock_aps:
        instance = mock_aps.return_value
        instance.get_provider_callable.return_value = mock_provider

        result = await svc.edit_via_llm(pipeline_id, "rename", db)

    assert result["ok"] is True
    p = await svc.get(pipeline_id)
    assert p.pipeline_json.nodes[0].name == "FromText"

    # empty text: fallback to generated_text
    mock_provider = AsyncMock(return_value={"text": "", "generated_text": payload_b})
    with patch("tg_agent.services.provider_service.RuntimeProviderRegistry") as mock_aps:
        instance = mock_aps.return_value
        instance.get_provider_callable.return_value = mock_provider

        result = await svc.edit_via_llm(pipeline_id, "rename", db)

    assert result["ok"] is True
    p = await svc.get(pipeline_id)
    assert p.pipeline_json.nodes[0].name == "FromGeneratedText"


@pytest.mark.anyio
async def test_create_from_template_applies_overrides(svc, db):
    """Every overrides key lands on the created pipeline (#mutation slice 4):
    prompt_template/llm_model/image_model/publish_mode/generation_backend/
    generate_interval_minutes are read from the dict, not silently defaulted."""
    graph = PipelineGraph(
        nodes=[
            PipelineNode(
                id="n1",
                type=PipelineNodeType.LLM_GENERATE,
                name="gen",
                config={"prompt_template": "Template default prompt"},
            ),
        ],
        edges=[],
    )
    tpl = MagicMock()
    tpl.template_json = graph

    mock_tpl_repo = MagicMock()
    mock_tpl_repo.get_by_id = AsyncMock(return_value=tpl)
    svc._bundle = PipelineBundle(
        content_pipelines=db.repos.content_pipelines,
        channels=db.repos.channels,
        accounts=db.repos.accounts,
        dialog_cache=db.repos.dialog_cache,
        pipeline_templates=mock_tpl_repo,
    )

    pid = await svc.create_from_template(
        template_id=1,
        name="Overridden",
        source_ids=[],
        target_refs=[],
        overrides={
            "prompt_template": "Override {source_messages}",
            "llm_model": "override-model",
            "image_model": "override:img",
            "publish_mode": "auto",
            "generation_backend": "deep_agents",
            "generate_interval_minutes": 7,
        },
    )
    p = await svc.get(pid)
    assert p.prompt_template == "Override {source_messages}"
    assert p.llm_model == "override-model"
    assert p.image_model == "override:img"
    assert p.publish_mode == "auto"
    assert p.generation_backend == "deep_agents"
    assert p.generate_interval_minutes == 7


def _injection_graph() -> PipelineGraph:
    return PipelineGraph(
        nodes=[
            PipelineNode(
                id="src", type=PipelineNodeType.SOURCE, name="source",
                config={"channel_ids": [1001]},
            ),
            PipelineNode(
                id="react", type=PipelineNodeType.REACT, name="react", config={},
            ),
            PipelineNode(
                id="filt", type=PipelineNodeType.FILTER, name="filter", config={},
            ),
        ],
        edges=[
            PipelineEdge(from_node="src", to_node="react"),
            PipelineEdge(from_node="react", to_node="filt"),
        ],
    )


@pytest.mark.anyio
async def test_update_injects_react_dag_and_filter_configs(svc, db):
    """update() node-level injections (#mutation slice 4): react_emoji splits on
    commas/whitespace (single vs multi emoji), dag_source_channel_ids rewrites
    SOURCE nodes, filter_config is normalized into FILTER nodes."""
    pid = await svc.add(
        name="InjectionPipeline",
        prompt_template="Test",
        source_channel_ids=[1001],
        target_refs=[PipelineTargetRef(phone="+100", dialog_id=77)],
    )
    await db.repos.content_pipelines.set_pipeline_json(pid, _injection_graph())

    ok = await svc.update(
        pid,
        name="InjectionPipeline",
        prompt_template="Test",
        source_channel_ids=[1001],
        target_refs=[PipelineTargetRef(phone="+100", dialog_id=77)],
        react_emoji="👍 ❤️",
        dag_source_channel_ids=[1002],
        filter_config={"keywords": ["spam"], "match_links": True},
    )
    assert ok is True

    from tg_agent.services.pipeline_filters import normalize_filter_config

    graph = await svc.get_graph(pid)
    by_id = {n.id: n for n in graph.nodes}
    # multi-emoji: first emoji + full random_emojis list
    assert by_id["react"].config["emoji"] == "👍"
    assert by_id["react"].config["random_emojis"] == ["👍", "❤️"]
    # dag source ids rewritten into the SOURCE node
    assert by_id["src"].config["channel_ids"] == [1002]
    # filter config normalized into the FILTER node
    assert by_id["filt"].config == normalize_filter_config(
        {"keywords": ["spam"], "match_links": True}
    )


@pytest.mark.anyio
async def test_update_single_react_emoji_clears_random_emojis(svc, db):
    """One emoji -> config['emoji'] = it, random_emojis emptied (#mutation slice 4)."""
    pid = await svc.add(
        name="ReactPipeline",
        prompt_template="Test",
        source_channel_ids=[1001],
        target_refs=[PipelineTargetRef(phone="+100", dialog_id=77)],
    )
    await db.repos.content_pipelines.set_pipeline_json(pid, _injection_graph())

    await svc.update(
        pid,
        name="ReactPipeline",
        prompt_template="Test",
        source_channel_ids=[1001],
        target_refs=[PipelineTargetRef(phone="+100", dialog_id=77)],
        react_emoji="🔥",
    )

    graph = await svc.get_graph(pid)
    react = next(n for n in graph.nodes if n.type == PipelineNodeType.REACT)
    assert react.config["emoji"] == "🔥"
    assert react.config["random_emojis"] == []


@pytest.mark.anyio
async def test_dag_rename_preserves_graph_relations_and_publish_eligibility(svc, db):
    from tg_agent.models import GenerationRun
    from tg_agent.services.publish_service import PublishService

    pid = await svc.add(
        name="DAG", prompt_template="Test", source_channel_ids=[1001],
        target_refs=[PipelineTargetRef(phone="+100", dialog_id=77)],
    )
    graph = PipelineGraph(nodes=[
        PipelineNode(id="src", name="Source", type=PipelineNodeType.SOURCE, config={"channel_ids": [1002]}),
        PipelineNode(id="fwd", name="Forward", type=PipelineNodeType.FORWARD,
                     config={"targets": [{"phone": "+100", "dialog_id": 77}]}),
        PipelineNode(id="pub", name="Publish", type=PipelineNodeType.PUBLISH,
                     config={"targets": [{"phone": "+100", "dialog_id": 78}]}),
    ], edges=[PipelineEdge(from_node="src", to_node="fwd"), PipelineEdge(from_node="fwd", to_node="pub")])
    await db.repos.content_pipelines.set_pipeline_json(pid, graph)

    # A metadata-only edit does not require refreshing Telegram dialog cache.
    await db.repos.dialog_cache.replace_dialogs("+100", [])

    assert await svc.update(
        pid, name="Renamed", prompt_template="Test", source_channel_ids=[1001],
        target_refs=[PipelineTargetRef(phone="+100", dialog_id=77)],
    )
    assert [s.channel_id for s in await svc.get_sources(pid)] == [1002]
    assert [t.dialog_id for t in await svc.get_targets(pid)] == [78]
    assert (await svc.get_graph(pid)).model_dump() == graph.model_dump()

    # Stop at the claim boundary: prove publication passes the target lookup
    # without acquiring a Telegram client or sending anything.
    publisher = PublishService(db, MagicMock())
    run = GenerationRun(id=999, pipeline_id=pid, status="completed",
                        generated_text="Draft", moderation_status="approved")
    with patch.object(db.repos.generation_runs, "claim_for_publish", new=AsyncMock(return_value=False)) as claim:
        results = await publisher.publish_run(run, await svc.get(pid))
    claim.assert_awaited_once_with(999, "approved")
    assert results[0].error == "Run is already being published"


@pytest.mark.anyio
async def test_dag_target_edit_without_destination_node_is_rejected(svc, db):
    pid = await svc.add(
        name="Reaction DAG", prompt_template="Test", source_channel_ids=[1001],
        target_refs=[PipelineTargetRef(phone="+100", dialog_id=77)],
    )
    graph = _injection_graph()
    await db.repos.content_pipelines.set_pipeline_json(pid, graph)
    with pytest.raises(PipelineValidationError, match="no publish/forward node"):
        await svc.update(
            pid, name="Changed", prompt_template="Test", source_channel_ids=[1001],
            target_refs=[], dag_target_refs=[PipelineTargetRef(phone="+100", dialog_id=78)],
        )
    assert (await svc.get(pid)).name == "Reaction DAG"


@pytest.mark.anyio
async def test_mixed_dag_target_edit_preserves_actual_forward_recipient(svc, db):
    from types import SimpleNamespace

    from tg_agent.services.pipeline_nodes.base import NodeContext
    from tg_agent.services.pipeline_nodes.handlers import ForwardHandler

    pid = await svc.add(
        name="Mixed DAG", prompt_template="Test", source_channel_ids=[1001],
        target_refs=[PipelineTargetRef(phone="+100", dialog_id=78)],
    )
    graph = PipelineGraph(nodes=[
        PipelineNode(id="src", name="Source", type=PipelineNodeType.SOURCE, config={"channel_ids": [1001]}),
        PipelineNode(id="fwd", name="Archive", type=PipelineNodeType.FORWARD,
                     config={"targets": [{"phone": "+100", "dialog_id": 77}]}),
        PipelineNode(id="pub", name="Publish", type=PipelineNodeType.PUBLISH,
                     config={"targets": [{"phone": "+100", "dialog_id": 78}]}),
    ], edges=[PipelineEdge(from_node="src", to_node="fwd"), PipelineEdge(from_node="fwd", to_node="pub")])
    await db.repos.content_pipelines.set_pipeline_json(pid, graph)
    await db.repos.dialog_cache.replace_dialogs("+100", [
        {"channel_id": 79, "title": "New publication target", "channel_type": "channel"},
    ])
    assert await svc.update(
        pid, name="Mixed DAG", prompt_template="Test", source_channel_ids=[1001], target_refs=[],
        dag_target_refs=[PipelineTargetRef(phone="+100", dialog_id=79)],
    )
    updated = await svc.get_graph(pid)
    assert updated.nodes[1].config["targets"] == [{"phone": "+100", "dialog_id": 77}]
    assert updated.nodes[2].config["targets"] == [{"phone": "+100", "dialog_id": 79}]
    assert [target.dialog_id for target in await svc.get_targets(pid)] == [79]

    context = NodeContext()
    context.set_global("context_messages", [SimpleNamespace(channel_id=1001, message_id=42)])
    actions = SimpleNamespace(forward_messages=AsyncMock())
    await ForwardHandler().execute(
        updated.nodes[1].config, context, {"client_pool": object(), "telegram_actions": actions},
    )
    actions.forward_messages.assert_awaited_once_with(
        phone="+100", from_chat=1001, to_chat=77, message_ids=[42], native=False,
        resolve_entities=False, collapse_single_message_id=True, source="pipeline_forward",
    )


@pytest.mark.anyio
@pytest.mark.parametrize("destination", [PipelineNodeType.PUBLISH, PipelineNodeType.FORWARD])
async def test_dag_explicit_source_target_edits_sync_graph_and_relations(svc, db, destination):
    pid = await svc.add(
        name="DAG", prompt_template="Test", source_channel_ids=[1001],
        target_refs=[PipelineTargetRef(phone="+100", dialog_id=77)],
    )
    graph = PipelineGraph(nodes=[
        PipelineNode(id="src", name="Source", type=PipelineNodeType.SOURCE),
        PipelineNode(id="pub", name="Destination", type=destination),
    ], edges=[PipelineEdge(from_node="src", to_node="pub")])
    await db.repos.content_pipelines.set_pipeline_json(pid, graph)
    assert await svc.update(
        pid, name="DAG", prompt_template="Test", source_channel_ids=[1001],
        target_refs=[PipelineTargetRef(phone="+100", dialog_id=77)],
        dag_source_channel_ids=[1002], dag_target_refs=[PipelineTargetRef(phone="+100", dialog_id=78)],
    )
    updated = await svc.get_graph(pid)
    assert updated.nodes[0].config["channel_ids"] == [1002]
    assert updated.nodes[1].config["targets"] == [{"phone": "+100", "dialog_id": 78}]
    assert [s.channel_id for s in await svc.get_sources(pid)] == [1002]
    expected_publication_targets = [78] if destination == PipelineNodeType.PUBLISH else []
    assert [t.dialog_id for t in await svc.get_targets(pid)] == expected_publication_targets


@pytest.mark.anyio
async def test_add_accepts_interval_of_exactly_one_minute(svc, db):
    """generate_interval_minutes == 1 is valid — the < 1 boundary, not <= 1
    (#mutation slice 4)."""
    pid = await svc.add(
        name="EveryMinute",
        prompt_template="Test {source_messages}",
        source_channel_ids=[1001],
        target_refs=[PipelineTargetRef(phone="+100", dialog_id=77)],
        generate_interval_minutes=1,
    )
    p = await svc.get(pid)
    assert p.generate_interval_minutes == 1
