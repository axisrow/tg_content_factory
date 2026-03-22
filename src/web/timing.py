from __future__ import annotations

from collections import deque
from typing import TypedDict


class TimingRecord(TypedDict):
    time: str    # "HH:MM:SS"
    method: str  # GET / POST
    path: str    # /channels
    status: int  # 200
    ms: int      # длительность в миллисекундах


class TimingBuffer:
    def __init__(self, maxlen: int = 200):
        self._records: deque[TimingRecord] = deque(maxlen=maxlen)

    def add(self, record: TimingRecord) -> None:
        self._records.append(record)

    def get_records(self) -> list[TimingRecord]:
        return list(self._records)
