from datetime import datetime, timezone

import pytest

from src.models import PhotoAutoUploadJob, PhotoBatch, PhotoBatchItem, PhotoBatchStatus


@pytest.mark.anyio
async def test_create_and_get_batch(db):
    repo = db.repos.photo_loader
    batch = PhotoBatch(phone="+123", target_dialog_id=100, target_title="Chat", target_type="channel")
    batch_id = await repo.create_batch(batch)
    assert batch_id > 0
    fetched = await repo.get_batch(batch_id)
    assert fetched is not None
    assert fetched.phone == "+123"
    assert fetched.target_dialog_id == 100


@pytest.mark.anyio
async def test_get_batch_not_found(db):
    assert await db.repos.photo_loader.get_batch(99999) is None


@pytest.mark.anyio
async def test_list_batches(db):
    repo = db.repos.photo_loader
    await repo.create_batch(PhotoBatch(phone="+1", target_dialog_id=1))
    await repo.create_batch(PhotoBatch(phone="+2", target_dialog_id=2))
    assert len(await repo.list_batches()) == 2


@pytest.mark.anyio
async def test_update_batch_status(db):
    repo = db.repos.photo_loader
    bid = await repo.create_batch(PhotoBatch(phone="+1", target_dialog_id=1))
    await repo.update_batch(bid, status=PhotoBatchStatus.COMPLETED, error="ok")
    b = await repo.get_batch(bid)
    assert b.status == PhotoBatchStatus.COMPLETED
    assert b.error == "ok"


@pytest.mark.anyio
async def test_update_batch_no_changes(db):
    repo = db.repos.photo_loader
    bid = await repo.create_batch(PhotoBatch(phone="+1", target_dialog_id=1))
    await repo.update_batch(bid)
    b = await repo.get_batch(bid)
    assert b.status == PhotoBatchStatus.PENDING


@pytest.mark.anyio
async def test_update_batch_last_run_at(db):
    repo = db.repos.photo_loader
    bid = await repo.create_batch(PhotoBatch(phone="+1", target_dialog_id=1))
    now = datetime.now(timezone.utc)
    await repo.update_batch(bid, last_run_at=now)
    b = await repo.get_batch(bid)
    assert b.last_run_at is not None


@pytest.mark.anyio
async def test_create_and_get_item(db):
    repo = db.repos.photo_loader
    bid = await repo.create_batch(PhotoBatch(phone="+1", target_dialog_id=1))
    item = PhotoBatchItem(
        batch_id=bid, phone="+1", target_dialog_id=1,
        file_paths=["/tmp/a.jpg", "/tmp/b.jpg"],
    )
    item_id = await repo.create_item(item)
    assert item_id > 0
    fetched = await repo.get_item(item_id)
    assert fetched is not None
    assert fetched.file_paths == ["/tmp/a.jpg", "/tmp/b.jpg"]
    assert fetched.batch_id == bid


@pytest.mark.anyio
async def test_list_items(db):
    repo = db.repos.photo_loader
    bid = await repo.create_batch(PhotoBatch(phone="+1", target_dialog_id=1))
    for i in range(3):
        await repo.create_item(PhotoBatchItem(
            batch_id=bid, phone="+1", target_dialog_id=1,
            file_paths=[f"/tmp/{i}.jpg"],
        ))
    items = await repo.list_items()
    assert len(items) == 3


@pytest.mark.anyio
async def test_list_items_for_batch(db):
    repo = db.repos.photo_loader
    b1 = await repo.create_batch(PhotoBatch(phone="+1", target_dialog_id=1))
    b2 = await repo.create_batch(PhotoBatch(phone="+2", target_dialog_id=2))
    await repo.create_item(PhotoBatchItem(batch_id=b1, phone="+1", target_dialog_id=1, file_paths=["/a"]))
    await repo.create_item(PhotoBatchItem(batch_id=b1, phone="+1", target_dialog_id=1, file_paths=["/b"]))
    await repo.create_item(PhotoBatchItem(batch_id=b2, phone="+2", target_dialog_id=2, file_paths=["/c"]))
    assert len(await repo.list_items_for_batch(b1)) == 2
    assert len(await repo.list_items_for_batch(b2)) == 1


@pytest.mark.anyio
async def test_update_item_status(db):
    repo = db.repos.photo_loader
    bid = await repo.create_batch(PhotoBatch(phone="+1", target_dialog_id=1))
    iid = await repo.create_item(PhotoBatchItem(batch_id=bid, phone="+1", target_dialog_id=1, file_paths=["/a"]))
    await repo.update_item(iid, status=PhotoBatchStatus.COMPLETED, telegram_message_ids=[10, 20])
    item = await repo.get_item(iid)
    assert item.status == PhotoBatchStatus.COMPLETED
    assert item.telegram_message_ids == [10, 20]


@pytest.mark.anyio
async def test_update_item_no_changes(db):
    repo = db.repos.photo_loader
    bid = await repo.create_batch(PhotoBatch(phone="+1", target_dialog_id=1))
    iid = await repo.create_item(PhotoBatchItem(batch_id=bid, phone="+1", target_dialog_id=1, file_paths=["/a"]))
    await repo.update_item(iid)
    item = await repo.get_item(iid)
    assert item.status == PhotoBatchStatus.PENDING


@pytest.mark.anyio
async def test_cancel_item(db):
    repo = db.repos.photo_loader
    bid = await repo.create_batch(PhotoBatch(phone="+1", target_dialog_id=1))
    iid = await repo.create_item(PhotoBatchItem(batch_id=bid, phone="+1", target_dialog_id=1, file_paths=["/a"]))
    assert await repo.cancel_item(iid) is True
    item = await repo.get_item(iid)
    assert item.status == PhotoBatchStatus.CANCELLED


@pytest.mark.anyio
async def test_cancel_item_already_completed(db):
    repo = db.repos.photo_loader
    bid = await repo.create_batch(PhotoBatch(phone="+1", target_dialog_id=1))
    iid = await repo.create_item(PhotoBatchItem(batch_id=bid, phone="+1", target_dialog_id=1, file_paths=["/a"]))
    await repo.update_item(iid, status=PhotoBatchStatus.COMPLETED)
    assert await repo.cancel_item(iid) is False


@pytest.mark.anyio
async def test_claim_next_due_item(db):
    repo = db.repos.photo_loader
    bid = await repo.create_batch(PhotoBatch(phone="+1", target_dialog_id=1))
    iid = await repo.create_item(PhotoBatchItem(
        batch_id=bid, phone="+1", target_dialog_id=1, file_paths=["/a"],
        status=PhotoBatchStatus.PENDING,
    ))
    now = datetime.now(timezone.utc)
    claimed = await repo.claim_next_due_item(now)
    assert claimed is not None
    assert claimed.id == iid
    assert claimed.status == PhotoBatchStatus.RUNNING


@pytest.mark.anyio
async def test_claim_next_due_item_none(db):
    repo = db.repos.photo_loader
    now = datetime.now(timezone.utc)
    assert await repo.claim_next_due_item(now) is None


@pytest.mark.anyio
async def test_requeue_running_items_on_startup(db):
    repo = db.repos.photo_loader
    bid = await repo.create_batch(PhotoBatch(phone="+1", target_dialog_id=1))
    iid = await repo.create_item(PhotoBatchItem(
        batch_id=bid, phone="+1", target_dialog_id=1, file_paths=["/a"],
        status=PhotoBatchStatus.PENDING,
    ))
    await repo.update_item(iid, status=PhotoBatchStatus.RUNNING)
    now = datetime.now(timezone.utc)
    count = await repo.requeue_running_items_on_startup(now)
    assert count == 1
    item = await repo.get_item(iid)
    assert item.status == PhotoBatchStatus.PENDING


@pytest.mark.anyio
async def test_create_and_get_auto_job(db):
    repo = db.repos.photo_loader
    job = PhotoAutoUploadJob(phone="+1", target_dialog_id=100, folder_path="/tmp/photos", interval_minutes=30)
    job_id = await repo.create_auto_job(job)
    assert job_id > 0
    fetched = await repo.get_auto_job(job_id)
    assert fetched is not None
    assert fetched.folder_path == "/tmp/photos"
    assert fetched.interval_minutes == 30


@pytest.mark.anyio
async def test_update_auto_job(db):
    repo = db.repos.photo_loader
    job = PhotoAutoUploadJob(phone="+1", target_dialog_id=100, folder_path="/tmp/photos")
    job_id = await repo.create_auto_job(job)
    await repo.update_auto_job(job_id, folder_path="/tmp/new", interval_minutes=60, is_active=False)
    updated = await repo.get_auto_job(job_id)
    assert updated.folder_path == "/tmp/new"
    assert updated.interval_minutes == 60
    assert updated.is_active is False


@pytest.mark.anyio
async def test_list_auto_jobs(db):
    repo = db.repos.photo_loader
    for i in range(3):
        await repo.create_auto_job(PhotoAutoUploadJob(
            phone=f"+{i}", target_dialog_id=100 + i,
            folder_path=f"/tmp/p{i}", is_active=(i < 2),
        ))
    assert len(await repo.list_auto_jobs()) == 3
    assert len(await repo.list_auto_jobs(active_only=True)) == 2


@pytest.mark.anyio
async def test_delete_auto_job(db):
    repo = db.repos.photo_loader
    job_id = await repo.create_auto_job(
        PhotoAutoUploadJob(phone="+1", target_dialog_id=100, folder_path="/tmp/p")
    )
    await repo.delete_auto_job(job_id)
    assert await repo.get_auto_job(job_id) is None


@pytest.mark.anyio
async def test_auto_file_sent_tracking(db):
    repo = db.repos.photo_loader
    job_id = await repo.create_auto_job(
        PhotoAutoUploadJob(phone="+1", target_dialog_id=100, folder_path="/tmp/p")
    )
    assert await repo.has_sent_auto_file(job_id, "/tmp/img.jpg") is False
    await repo.mark_auto_file_sent(job_id, "/tmp/img.jpg")
    assert await repo.has_sent_auto_file(job_id, "/tmp/img.jpg") is True
