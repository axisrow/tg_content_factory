import pytest


@pytest.mark.anyio
async def test_save_returns_id(db):
    repo = db.repos.generated_images
    img_id = await repo.save("a sunset", "together:flux", "https://img.example/1.png", None)
    assert img_id > 0


@pytest.mark.anyio
async def test_get_by_id_found(db):
    repo = db.repos.generated_images
    img_id = await repo.save("a sunset", "together:flux", "https://img.example/1.png", "/tmp/img.png")
    img = await repo.get_by_id(img_id)
    assert img is not None
    assert img.prompt == "a sunset"
    assert img.model == "together:flux"
    assert img.image_url == "https://img.example/1.png"
    assert img.local_path == "/tmp/img.png"


@pytest.mark.anyio
async def test_get_by_id_not_found(db):
    repo = db.repos.generated_images
    assert await repo.get_by_id(99999) is None


@pytest.mark.anyio
async def test_list_recent_default_limit(db):
    repo = db.repos.generated_images
    for i in range(60):
        await repo.save(f"prompt-{i}", None, None, None)
    result = await repo.list_recent()
    assert len(result) == 50


@pytest.mark.anyio
async def test_list_recent_custom_limit(db):
    repo = db.repos.generated_images
    for i in range(10):
        await repo.save(f"prompt-{i}", None, None, None)
    result = await repo.list_recent(limit=3)
    assert len(result) == 3


@pytest.mark.anyio
async def test_save_with_null_optional_fields(db):
    repo = db.repos.generated_images
    img_id = await repo.save("test", None, None, None)
    img = await repo.get_by_id(img_id)
    assert img is not None
    assert img.model is None
    assert img.image_url is None
    assert img.local_path is None
