import pytest
from datetime import datetime

from src.scheduler.manager import SchedulerManager


class FakeJob:
    def __init__(self, id, next_run_time=None):
        self.id = id
        self.next_run_time = next_run_time


class FakeScheduler:
    def __init__(self, job=None):
        self._job = job

    def get_job(self, job_id):
        return self._job if self._job and self._job.id == job_id else None


@pytest.mark.asyncio
async def test_get_job_next_run_returns_none_when_no_scheduler():
    mgr = SchedulerManager()
    assert mgr.get_job_next_run("pipeline_run_1") is None


@pytest.mark.asyncio
async def test_get_job_next_run_returns_time_when_job_exists():
    dt = datetime(2026, 3, 17, 12, 0)
    job = FakeJob("pipeline_run_1", next_run_time=dt)
    mgr = SchedulerManager()
    mgr._scheduler = FakeScheduler(job)
    assert mgr.get_job_next_run("pipeline_run_1") == dt
