from __future__ import annotations

import logging
from collections import deque


class LogBuffer(logging.Handler):
    def __init__(self, maxlen: int = 500):
        super().__init__()
        self._records: deque[dict] = deque(maxlen=maxlen)

    def emit(self, record: logging.LogRecord) -> None:
        self._records.append({
            "time": logging.Formatter().formatTime(record, "%Y-%m-%d %H:%M:%S"),
            "level": record.levelname,
            "logger": record.name,
            "message": self.format(record),
        })

    def get_records(self) -> list[dict]:
        return list(self._records)
