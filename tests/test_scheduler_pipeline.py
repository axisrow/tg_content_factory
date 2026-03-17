import pytest

from src.models import (
    CollectionTask,
    CollectionTaskStatus,
    CollectionTaskType,
    PipelineRunTaskPayload,
)
from src.scheduler.manager import SchedulerManager
from src.services.unified_dispatcher import UnifiedDispatcher


class FakePipeline:
    def __init__(self, id, generate_interval_minutes, is_active=True):
        self.id = id
        self.generate_interval_minutes = generate_interval_minutes
        self.is_active = is_active


class FakePipelineBundle:
    def __init__(self, pipelines):
        self._pipelines = pipelines

    async def get_all(self, active_only=True):
        return self._pipelines


class FakeJob:
    def __init__(self, id):
        self.id = id


class FakeScheduler:
    def __init__(self, existing_jobs=None):
        self.added = []
        self.removed = []
        self.existing = existing_jobs or []

    def get_jobs(self):
        return list(self.existing)

    def add_job(self, func, trigger, id=None, replace_existing=False, args=None):
        self.added.append({"func": func, "trigger": trigger, "id": id, "args": args})
        # emulate job presence
        self.existing.append(FakeJob(id))

    def remove_job(self, job_id):
        self.removed.append(job_id)
        self.existing = [j for j in self.existing if j.id != job_id]


@pytest.mark.asyncio
async def test_sync_pipeline_jobs_adds_and_removes():
    p1 = FakePipeline(1, 10, is_active=True)
    p2 = FakePipeline(2, 20, is_active=True)
    bundle = FakePipelineBundle([p1, p2])
    mgr = SchedulerManager()
    mgr._pipeline_bundle = bundle
    # existing job for a deleted pipeline
    fake_existing_job = FakeJob("pipeline_run_3")
    scheduler = FakeScheduler(existing_jobs=[fake_existing_job])
    mgr._scheduler = scheduler

    await mgr.sync_pipeline_jobs()

    ids = [a["id"] for a in scheduler.added]
    assert "pipeline_run_1" in ids
    assert "pipeline_run_2" in ids
    assert "pipeline_run_3" in scheduler.removed



# --- UnifiedDispatcher pipeline run handling (missing env) ---
class FakeTasksRepo:
    def __init__(self):
        self.calls = []

    async def update_collection_task(
        self, task_id, status, messages_collected=None, error=None, note=None
    ):
        self.calls.append(
            {
                "task_id": task_id,
                "status": status,
                "messages_collected": messages_collected,
                "error": error,
                "note": note,
            }
        )


@pytest.mark.asyncio
async def test_pipeline_run_handler_without_env_marks_failed():
    fake_tasks = FakeTasksRepo()
    # collector and channel_bundle can be dummies; handler will fail early due to missing env
    ud = UnifiedDispatcher(collector=None, channel_bundle=None, tasks_repo=fake_tasks)

    payload = PipelineRunTaskPayload(pipeline_id=1)
    task = CollectionTask(id=99, task_type=CollectionTaskType.PIPELINE_RUN, payload=payload)

    await ud._handle_pipeline_run(task)

    assert fake_tasks.calls, "update_collection_task was not called"
    last = fake_tasks.calls[-1]
    assert last["status"] == CollectionTaskStatus.FAILED
    assert last["error"] is not None
    assert "Pipeline execution environment not configured" in last["error"]
