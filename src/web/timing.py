from __future__ import annotations

import contextvars
from collections import deque
from typing import TypedDict


class TimingRecord(TypedDict, total=False):
    time: str    # "HH:MM:SS"
    method: str  # GET / POST
    path: str    # /channels
    status: int  # 200
    ms: int      # длительность в миллисекундах
    # profiling breakdown (only present when ENV=DEV)
    db_ms: int
    db_queries: int


_current_profiler: contextvars.ContextVar[RequestProfiler | None] = contextvars.ContextVar(
    "request_profiler", default=None
)


def get_current_profiler() -> RequestProfiler | None:
    return _current_profiler.get()


class RequestProfiler:
    __slots__ = ("db_ns", "db_queries", "_token")

    def __init__(self) -> None:
        self.db_ns: int = 0
        self.db_queries: int = 0
        self._token: contextvars.Token | None = None

    def activate(self) -> None:
        self._token = _current_profiler.set(self)

    def deactivate(self) -> None:
        if self._token is not None:
            _current_profiler.reset(self._token)
            self._token = None

    def record_db(self, elapsed_ns: int) -> None:
        self.db_ns += elapsed_ns
        self.db_queries += 1

    def to_breakdown(self) -> dict:
        return {
            "db_ms": self.db_ns // 1_000_000,
            "db_queries": self.db_queries,
        }


class TimingBuffer:
    def __init__(self, maxlen: int = 200):
        self._records: deque[TimingRecord] = deque(maxlen=maxlen)

    def add(self, record: TimingRecord) -> None:
        self._records.append(record)

    def get_records(self) -> list[TimingRecord]:
        return list(self._records)
