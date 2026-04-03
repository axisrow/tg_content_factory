"""Backward-compatibility shim — canonical location is src/scheduler/service.py."""
from src.scheduler.service import SchedulerManager  # noqa: F401

__all__ = ["SchedulerManager"]
