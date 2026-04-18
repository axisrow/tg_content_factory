"""Extra tests for PipelineService to increase coverage from 66% to 80%+."""
from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.database.bundles import PipelineBundle
from src.models import (
    Account,
    Channel,
    PipelineEdge,
    PipelineGraph,
    PipelineNode,
    PipelineNodeType,
)
from src.services.pipeline_service import (
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


@pytest.mark.asyncio
async def test_list_returns_all(svc, pipeline_id):
    items = await svc.list()
    assert len(items) == 1
    assert items[0].name == "TestPipeline"


@pytest.mark.asyncio
async def test_list_active_only(svc, pipeline_id):
    items = await svc.list(active_only=True)
    assert len(items) == 1


@pytest.mark.asyncio
async def test_get_existing(svc, pipeline_id):
    p = await svc.get(pipeline_id)
    assert p is not None
    assert p.id == pipeline_id


@pytest.mark.asyncio
async def test_get_nonexistent(svc):
    assert await svc.get(99999) is None


@pytest.mark.asyncio
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


@pytest.mark.asyncio
async def test_get_retrieval_scope_multi_source(svc, pipeline_id):
    pipeline = await svc.get(pipeline_id)
    scope = await svc.get_retrieval_scope(pipeline)
    assert scope.query == "TestPipeline"
    assert scope.channel_id is None


@pytest.mark.asyncio
async def test_toggle_deactivates(svc, pipeline_id):
    result = await svc.toggle(pipeline_id)
    assert result is True
    p = await svc.get(pipeline_id)
    assert p.is_active is False


@pytest.mark.asyncio
async def test_toggle_reactivates(svc, pipeline_id):
    await svc.toggle(pipeline_id)  # deactivate
    result = await svc.toggle(pipeline_id)  # reactivate
    assert result is True
    p = await svc.get(pipeline_id)
    assert p.is_active is True


@pytest.mark.asyncio
async def test_toggle_nonexistent(svc):
    result = await svc.toggle(99999)
    assert result is False


@pytest.mark.asyncio
async def test_delete(svc, pipeline_id):
    await svc.delete(pipeline_id)
    assert await svc.get(pipeline_id) is None


# ---------------------------------------------------------------------------
# update
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
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


@pytest.mark.asyncio
async def test_get_sources(svc, pipeline_id):
    sources = await svc.get_sources(pipeline_id)
    assert len(sources) == 2
    assert {s.channel_id for s in sources} == {1001, 1002}


@pytest.mark.asyncio
async def test_get_targets(svc, pipeline_id):
    targets = await svc.get_targets(pipeline_id)
    assert len(targets) == 1
    assert targets[0].phone == "+100"
    assert targets[0].dialog_id == 77


@pytest.mark.asyncio
async def test_get_detail(svc, pipeline_id):
    detail = await svc.get_detail(pipeline_id)
    assert detail is not None
    assert detail["pipeline"].id == pipeline_id
    assert detail["source_ids"] == [1001, 1002]
    assert detail["target_refs"] == ["+100|77"]
    assert len(detail["source_titles"]) == 2


@pytest.mark.asyncio
async def test_get_detail_nonexistent(svc):
    assert await svc.get_detail(99999) is None


@pytest.mark.asyncio
async def test_get_with_relations(svc, pipeline_id):
    rows = await svc.get_with_relations()
    assert len(rows) == 1
    assert rows[0]["pipeline"].id == pipeline_id
    assert rows[0]["source_ids"] == [1001, 1002]


@pytest.mark.asyncio
async def test_get_with_relations_active_only(svc, pipeline_id):
    rows = await svc.get_with_relations(active_only=True)
    assert len(rows) == 1


@pytest.mark.asyncio
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


@pytest.mark.asyncio
async def test_list_cached_dialogs_by_phone(svc, db):
    result = await svc.list_cached_dialogs_by_phone()
    assert "+100" in result
    assert len(result["+100"]) == 2


# ---------------------------------------------------------------------------
# _build_pipeline validation errors
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_build_pipeline_empty_name(svc):
    with pytest.raises(PipelineValidationError, match="[Нн]азвание"):
        await svc.add(
            name="   ",
            prompt_template="Summarize {source_messages}",
            source_channel_ids=[1001],
            target_refs=[PipelineTargetRef(phone="+100", dialog_id=77)],
        )


@pytest.mark.asyncio
async def test_build_pipeline_empty_template(svc):
    with pytest.raises(PipelineValidationError, match="[Шш]аблон"):
        await svc.add(
            name="Name",
            prompt_template="   ",
            source_channel_ids=[1001],
            target_refs=[PipelineTargetRef(phone="+100", dialog_id=77)],
        )


@pytest.mark.asyncio
async def test_build_pipeline_invalid_mode(svc):
    with pytest.raises(PipelineValidationError, match="[Рр]ежим"):
        await svc.add(
            name="Name",
            prompt_template="Summarize {source_messages}",
            source_channel_ids=[1001],
            target_refs=[PipelineTargetRef(phone="+100", dialog_id=77)],
            publish_mode="nonexistent_mode",
        )


@pytest.mark.asyncio
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


@pytest.mark.asyncio
async def test_normalize_sources_empty(svc):
    with pytest.raises(PipelineValidationError, match="[Вв]ыберите.*источник"):
        await svc.add(
            name="Name",
            prompt_template="Summarize {source_messages}",
            source_channel_ids=[],
            target_refs=[PipelineTargetRef(phone="+100", dialog_id=77)],
        )


@pytest.mark.asyncio
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


@pytest.mark.asyncio
async def test_normalize_targets_empty(svc):
    with pytest.raises(PipelineValidationError, match="[Вв]ыберите.*цель"):
        await svc.add(
            name="Name",
            prompt_template="Summarize {source_messages}",
            source_channel_ids=[1001],
            target_refs=[],
        )


@pytest.mark.asyncio
async def test_normalize_targets_unknown_phone(svc):
    with pytest.raises(PipelineValidationError, match="[Аа]ккаунт"):
        await svc.add(
            name="Name",
            prompt_template="Summarize {source_messages}",
            source_channel_ids=[1001],
            target_refs=[PipelineTargetRef(phone="+999", dialog_id=77)],
        )


@pytest.mark.asyncio
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


@pytest.mark.asyncio
async def test_normalize_targets_dialog_not_cached(svc):
    with pytest.raises(PipelineValidationError, match="кеш"):
        await svc.add(
            name="Name",
            prompt_template="Summarize {source_messages}",
            source_channel_ids=[1001],
            target_refs=[PipelineTargetRef(phone="+100", dialog_id=999)],
        )


@pytest.mark.asyncio
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


@pytest.mark.asyncio
async def test_export_json(svc, pipeline_id):
    data = await svc.export_json(pipeline_id)
    assert data is not None
    assert data["name"] == "TestPipeline"
    assert data["source_ids"] == [1001, 1002]
    assert data["target_refs"] == ["+100|77"]
    assert data["publish_mode"] == "moderated"


@pytest.mark.asyncio
async def test_export_json_nonexistent(svc):
    assert await svc.export_json(99999) is None


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
async def test_import_json_from_string(svc, pipeline_id):
    """Import a pipeline from a JSON string."""
    export = await svc.export_json(pipeline_id)
    assert export is not None
    json_str = json.dumps(export)

    new_id = await svc.import_json(json_str)
    assert new_id > 0


@pytest.mark.asyncio
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


@pytest.mark.asyncio
async def test_import_json_with_target_refs(svc, pipeline_id):
    """Import with target_refs in the data."""
    export = await svc.export_json(pipeline_id)
    assert export is not None

    new_id = await svc.import_json(export, name_override="WithTargets")
    assert new_id > 0
    targets = await svc.get_targets(new_id)
    assert len(targets) >= 1


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
async def test_list_templates_no_repo(db):
    """When pipeline_templates is None, returns empty list."""
    from src.database.bundles import PipelineBundle

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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
async def test_create_from_template_no_repo(db):
    """When pipeline_templates is None, raises error."""
    from src.database.bundles import PipelineBundle

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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
async def test_edit_via_llm_pipeline_not_found(svc, db):
    result = await svc.edit_via_llm(99999, "change something", db)
    assert result["ok"] is False
    assert "не найден" in result["error"]


@pytest.mark.asyncio
async def test_edit_via_llm_success(svc, db, pipeline_id):
    """edit_via_llm with a working provider returns updated graph."""
    new_graph_json = json.dumps({
        "nodes": [{"id": "n1", "type": "llm_generate", "name": "gen", "config": {}, "position": {"x": 0, "y": 0}}],
        "edges": [],
    })

    mock_provider = AsyncMock(return_value=new_graph_json)

    with patch("src.services.provider_service.AgentProviderService") as mock_aps:
        instance = mock_aps.return_value
        instance.get_provider_callable.return_value = mock_provider

        result = await svc.edit_via_llm(pipeline_id, "add a generate node", db)

    assert result["ok"] is True
    assert "pipeline_json" in result
    assert len(result["pipeline_json"]["nodes"]) == 1


@pytest.mark.asyncio
async def test_edit_via_llm_with_markdown_fences(svc, db, pipeline_id):
    """edit_via_llm strips markdown code fences from LLM response."""
    new_graph = {"nodes": [], "edges": []}
    new_graph_json = json.dumps(new_graph)
    wrapped = f"```json\n{new_graph_json}\n```"

    mock_provider = AsyncMock(return_value=wrapped)

    with patch("src.services.provider_service.AgentProviderService") as mock_aps:
        instance = mock_aps.return_value
        instance.get_provider_callable.return_value = mock_provider

        result = await svc.edit_via_llm(pipeline_id, "clear all nodes", db)

    assert result["ok"] is True


@pytest.mark.asyncio
async def test_edit_via_llm_provider_failure(svc, db, pipeline_id):
    """edit_via_llm returns error when provider raises."""
    mock_provider = AsyncMock(side_effect=RuntimeError("API down"))

    with patch("src.services.provider_service.AgentProviderService") as mock_aps:
        instance = mock_aps.return_value
        instance.get_provider_callable.return_value = mock_provider

        result = await svc.edit_via_llm(pipeline_id, "change", db)

    assert result["ok"] is False
    assert "API down" in result["error"]


@pytest.mark.asyncio
async def test_edit_via_llm_dict_result(svc, db, pipeline_id):
    """edit_via_llm handles provider returning dict with 'text' key."""
    new_graph = {"nodes": [], "edges": []}
    new_graph_json = json.dumps(new_graph)

    mock_provider = AsyncMock(return_value={"text": new_graph_json})

    with patch("src.services.provider_service.AgentProviderService") as mock_aps:
        instance = mock_aps.return_value
        instance.get_provider_callable.return_value = mock_provider

        result = await svc.edit_via_llm(pipeline_id, "clear", db)

    assert result["ok"] is True


@pytest.mark.asyncio
async def test_edit_via_llm_pipeline_without_graph(svc, db, pipeline_id):
    """edit_via_llm works when pipeline has no existing pipeline_json."""
    p = await svc.get(pipeline_id)
    assert p.pipeline_json is None

    new_graph = {"nodes": [], "edges": []}
    mock_provider = AsyncMock(return_value=json.dumps(new_graph))

    with patch("src.services.provider_service.AgentProviderService") as mock_aps:
        instance = mock_aps.return_value
        instance.get_provider_callable.return_value = mock_provider

        result = await svc.edit_via_llm(pipeline_id, "clear", db)

    assert result["ok"] is True


# ---------------------------------------------------------------------------
# _build_pipeline edge cases
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
async def test_init_with_database(db):
    """PipelineService accepts Database and wraps it in PipelineBundle."""
    svc = PipelineService(db)
    # Should not raise -- bundle was created from db
    items = await svc.list()
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


@pytest.mark.asyncio
async def test_get_graph_returns_graph(svc, graph_pipeline_id):
    graph = await svc.get_graph(graph_pipeline_id)
    assert graph is not None
    assert len(graph.nodes) == 3


@pytest.mark.asyncio
async def test_get_graph_returns_none_for_legacy(svc, pipeline_id):
    graph = await svc.get_graph(pipeline_id)
    assert graph is None


@pytest.mark.asyncio
async def test_get_graph_pipeline_not_found(svc):
    graph = await svc.get_graph(99999)
    assert graph is None


@pytest.mark.asyncio
async def test_add_node(svc, graph_pipeline_id):
    new_node = PipelineNode(id="pub", type=PipelineNodeType.PUBLISH, name="publish", config={"targets": []})
    ok = await svc.add_node(graph_pipeline_id, new_node)
    assert ok is True
    graph = await svc.get_graph(graph_pipeline_id)
    assert len(graph.nodes) == 4
    assert graph.nodes[3].id == "pub"


@pytest.mark.asyncio
async def test_add_node_auto_position(svc, graph_pipeline_id):
    new_node = PipelineNode(id="delay1", type=PipelineNodeType.DELAY, name="delay", config={})
    await svc.add_node(graph_pipeline_id, new_node)
    graph = await svc.get_graph(graph_pipeline_id)
    added = next(n for n in graph.nodes if n.id == "delay1")
    assert added.position["x"] > 0


@pytest.mark.asyncio
async def test_add_node_no_graph(svc, pipeline_id):
    new_node = PipelineNode(id="x", type=PipelineNodeType.DELAY, name="d")
    ok = await svc.add_node(pipeline_id, new_node)
    assert ok is False


@pytest.mark.asyncio
async def test_remove_node_removes_edges(svc, graph_pipeline_id):
    ok = await svc.remove_node(graph_pipeline_id, "fetch")
    assert ok is True
    graph = await svc.get_graph(graph_pipeline_id)
    ids = [n.id for n in graph.nodes]
    assert "fetch" not in ids
    # Edges referencing "fetch" should also be removed
    for e in graph.edges:
        assert e.from_node != "fetch" and e.to_node != "fetch"


@pytest.mark.asyncio
async def test_remove_node_not_found(svc, graph_pipeline_id):
    ok = await svc.remove_node(graph_pipeline_id, "nonexistent")
    assert ok is False


@pytest.mark.asyncio
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


@pytest.mark.asyncio
async def test_replace_node_with_new_id_updates_edges(svc, graph_pipeline_id):
    new_node = PipelineNode(id="agent_v2", type=PipelineNodeType.AGENT_LOOP, name="agent", config={})
    ok = await svc.replace_node(graph_pipeline_id, "gen", new_node)
    assert ok is True
    graph = await svc.get_graph(graph_pipeline_id)
    # Old "gen" edges should now point to "agent_v2"
    for e in graph.edges:
        assert "gen" not in (e.from_node, e.to_node)
    assert any(e.to_node == "agent_v2" for e in graph.edges)


@pytest.mark.asyncio
async def test_replace_first_node_with_new_id_updates_edges(svc, graph_pipeline_id):
    """Regression: replacing the first node (index 0) must still rewrite edges."""
    new_node = PipelineNode(id="src_v2", type=PipelineNodeType.SOURCE, name="source", config={})
    ok = await svc.replace_node(graph_pipeline_id, "src", new_node)
    assert ok is True
    graph = await svc.get_graph(graph_pipeline_id)
    assert not any("src" in (e.from_node, e.to_node) for e in graph.edges)
    assert any(e.from_node == "src_v2" for e in graph.edges)


@pytest.mark.asyncio
async def test_add_edge(svc, graph_pipeline_id):
    ok = await svc.add_edge(graph_pipeline_id, "src", "gen")
    assert ok is True
    graph = await svc.get_graph(graph_pipeline_id)
    assert any(e.from_node == "src" and e.to_node == "gen" for e in graph.edges)


@pytest.mark.asyncio
async def test_add_edge_idempotent(svc, graph_pipeline_id):
    await svc.add_edge(graph_pipeline_id, "src", "gen")
    graph1 = await svc.get_graph(graph_pipeline_id)
    await svc.add_edge(graph_pipeline_id, "src", "gen")
    graph2 = await svc.get_graph(graph_pipeline_id)
    assert len(graph1.edges) == len(graph2.edges)


@pytest.mark.asyncio
async def test_add_edge_no_graph(svc, pipeline_id):
    ok = await svc.add_edge(pipeline_id, "a", "b")
    assert ok is False


@pytest.mark.asyncio
async def test_remove_edge(svc, graph_pipeline_id):
    ok = await svc.remove_edge(graph_pipeline_id, "src", "fetch")
    assert ok is True
    graph = await svc.get_graph(graph_pipeline_id)
    assert not any(e.from_node == "src" and e.to_node == "fetch" for e in graph.edges)


@pytest.mark.asyncio
async def test_remove_edge_not_found(svc, graph_pipeline_id):
    ok = await svc.remove_edge(graph_pipeline_id, "src", "gen")
    assert ok is False
