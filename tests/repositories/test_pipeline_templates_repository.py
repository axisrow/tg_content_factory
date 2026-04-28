import pytest

from src.models import PipelineGraph, PipelineTemplate


@pytest.fixture
def sample_template():
    return PipelineTemplate(
        name="Basic Pipeline",
        description="A test template",
        category="test",
        template_json=PipelineGraph(nodes=[], edges=[]),
        is_builtin=False,
    )


@pytest.mark.anyio
async def test_add_and_get_by_id(db, sample_template):
    repo = db.repos.pipeline_templates
    tpl_id = await repo.add(sample_template)
    assert tpl_id > 0
    tpl = await repo.get_by_id(tpl_id)
    assert tpl is not None
    assert tpl.name == "Basic Pipeline"
    assert tpl.description == "A test template"
    assert tpl.category == "test"
    assert tpl.is_builtin is False


@pytest.mark.anyio
async def test_get_by_id_not_found(db):
    repo = db.repos.pipeline_templates
    assert await repo.get_by_id(99999) is None


@pytest.mark.anyio
async def test_get_by_name(db, sample_template):
    repo = db.repos.pipeline_templates
    await repo.add(sample_template)
    tpl = await repo.get_by_name("Basic Pipeline")
    assert tpl is not None
    assert tpl.name == "Basic Pipeline"


@pytest.mark.anyio
async def test_get_by_name_not_found(db):
    repo = db.repos.pipeline_templates
    assert await repo.get_by_name("nonexistent") is None


@pytest.mark.anyio
async def test_list_all(db, sample_template):
    repo = db.repos.pipeline_templates
    await repo.add(sample_template)
    t2 = PipelineTemplate(
        name="Other",
        description="desc2",
        category="other",
        template_json=PipelineGraph(nodes=[], edges=[]),
    )
    await repo.add(t2)
    all_tpls = await repo.list_all()
    # Builtins are auto-inserted on init; just check our 2 are present
    names = {t.name for t in all_tpls}
    assert "Basic Pipeline" in names
    assert "Other" in names


@pytest.mark.anyio
async def test_list_all_with_category_filter(db, sample_template):
    repo = db.repos.pipeline_templates
    await repo.add(sample_template)
    t2 = PipelineTemplate(
        name="Other",
        description="desc2",
        category="other",
        template_json=PipelineGraph(nodes=[], edges=[]),
    )
    await repo.add(t2)
    filtered = await repo.list_all(category="test")
    assert len(filtered) == 1
    assert filtered[0].name == "Basic Pipeline"


@pytest.mark.anyio
async def test_delete(db, sample_template):
    repo = db.repos.pipeline_templates
    tpl_id = await repo.add(sample_template)
    await repo.delete(tpl_id)
    assert await repo.get_by_id(tpl_id) is None


@pytest.mark.anyio
async def test_ensure_builtins_inserts_missing(db):
    repo = db.repos.pipeline_templates
    builtins = [
        PipelineTemplate(
            name="Builtin1",
            description="Builtin template 1",
            category="builtin",
            template_json=PipelineGraph(nodes=[], edges=[]),
            is_builtin=True,
        ),
        PipelineTemplate(
            name="Builtin2",
            description="Builtin template 2",
            category="builtin",
            template_json=PipelineGraph(nodes=[], edges=[]),
            is_builtin=True,
        ),
    ]
    await repo.ensure_builtins(builtins)
    all_tpls = await repo.list_all()
    names = {t.name for t in all_tpls}
    assert "Builtin1" in names
    assert "Builtin2" in names


@pytest.mark.anyio
async def test_ensure_builtins_idempotent(db):
    repo = db.repos.pipeline_templates
    builtins = [
        PipelineTemplate(
            name="Builtin1",
            description="desc",
            category="builtin",
            template_json=PipelineGraph(nodes=[], edges=[]),
            is_builtin=True,
        ),
    ]
    _count_before = len(await repo.list_all())
    await repo.ensure_builtins(builtins)
    await repo.ensure_builtins(builtins)
    all_tpls = await repo.list_all()
    # Should have exactly one "Builtin1" (not duplicated)
    builtin1_count = sum(1 for t in all_tpls if t.name == "Builtin1")
    assert builtin1_count == 1
