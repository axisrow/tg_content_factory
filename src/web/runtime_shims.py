from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace

from src.database import Database


class SnapshotClientPool:
    def __init__(self, db: Database):
        self._db = db

    @property
    def clients(self) -> dict[str, object]:
        return getattr(self, "_clients_cache", {})

    def connected_phones(self) -> set[str]:
        return set(self.clients.keys())

    async def refresh(self) -> None:
        snapshot = await self._db.repos.runtime_snapshots.get_snapshot("accounts_status")
        payload = snapshot.payload if snapshot is not None else {}
        phones = payload.get("connected_phones", [])
        if not isinstance(phones, list):
            phones = []
        self._clients_cache = {str(phone): object() for phone in phones}

    async def initialize(self) -> None:
        await self.refresh()

    async def warm_all_dialogs(self) -> None:
        return None

    async def disconnect_all(self) -> None:
        return None

    async def get_native_client_by_phone(self, phone: str):
        raise RuntimeError("Telegram runtime is only available in the worker process.")

    async def release_client(self, phone: str) -> None:
        return None


class SnapshotCollector:
    def __init__(self, db: Database):
        self._db = db
        self.is_running = False

    async def refresh(self) -> None:
        snapshot = await self._db.repos.runtime_snapshots.get_snapshot("collector_status")
        payload = snapshot.payload if snapshot is not None else {}
        self.is_running = bool(payload.get("is_running", False))

    async def get_collection_availability(self):
        snapshot = await self._db.repos.runtime_snapshots.get_snapshot("collector_status")
        payload = snapshot.payload if snapshot is not None else {}
        next_available_raw = payload.get("next_available_at_utc")
        next_available = None
        if isinstance(next_available_raw, str):
            try:
                next_available = datetime.fromisoformat(next_available_raw)
            except ValueError:
                next_available = None
        return SimpleNamespace(
            state=payload.get("state", "no_connected_active"),
            retry_after_sec=payload.get("retry_after_sec"),
            next_available_at_utc=next_available,
        )

    async def cancel(self) -> None:
        return None


class SnapshotSchedulerManager:
    def __init__(self, db: Database, default_interval_minutes: int):
        self._db = db
        self._default_interval_minutes = default_interval_minutes
        self._is_running = False
        self._interval_minutes = default_interval_minutes

    @property
    def is_running(self) -> bool:
        return self._is_running

    @property
    def interval_minutes(self) -> int:
        return self._interval_minutes

    async def load_settings(self) -> None:
        snapshot = await self._db.repos.runtime_snapshots.get_snapshot("scheduler_status")
        payload = snapshot.payload if snapshot is not None else {}
        self._is_running = bool(payload.get("is_running", False))
        self._interval_minutes = int(payload.get("interval_minutes", self._default_interval_minutes))

    async def start(self) -> None:
        # Web mode cannot run the live scheduler loop; it only persists the
        # `scheduler_autostart` setting via the route. The worker picks that up
        # on next startup. We return silently so the web route can continue
        # without crashing; callers must persist the setting themselves.
        return None

    async def stop(self) -> None:
        return None

    async def get_potential_jobs(self) -> list[dict]:
        snapshot = await self._db.repos.runtime_snapshots.get_snapshot("scheduler_jobs")
        payload = snapshot.payload if snapshot is not None else {}
        jobs = payload.get("jobs", [])
        return jobs if isinstance(jobs, list) else []

    def get_all_jobs_next_run(self) -> dict[str, object]:
        return {}

    async def trigger_warm_background(self) -> None:
        # Live warm-dialogs runs only inside the worker process. In web mode
        # the worker will pick up scheduled work on its own cadence, so this
        # is a no-op here (the route response still redirects to /scheduler).
        return None

    async def sync_job_state(self, job_id: str, *, enabled: bool) -> None:
        return None

    async def set_interval(self, minutes: int) -> None:
        return None

    def update_interval(self, minutes: int) -> None:
        # /settings/save-scheduler and /scheduler/jobs/*/set-interval call
        # this synchronously after persisting the setting. The worker
        # re-reads the interval on its own load cycle, so the web-side
        # call is a no-op here.
        return None

    async def sync_search_query_jobs(self) -> None:
        # Search-query mutation routes call this when the scheduler is
        # running (snapshot says is_running=True). The worker will pick
        # up the DB change on its next sync cycle.
        return None

    async def sync_pipeline_jobs(self) -> None:
        # Pipeline mutation routes call this too; same rationale as above.
        return None
