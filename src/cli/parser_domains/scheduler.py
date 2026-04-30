from __future__ import annotations

import argparse


def register(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser | None:
    sched_parser = subparsers.add_parser("scheduler", help="Scheduler control")
    sched_sub = sched_parser.add_subparsers(dest="scheduler_action")
    sched_sub.add_parser("start", help="Start scheduler (foreground)")
    sched_sub.add_parser("trigger", help="Trigger one-shot collection")
    sched_sub.add_parser("status", help="Show scheduler configuration and status")
    sched_sub.add_parser("stop", help="Disable scheduler autostart")
    sched_job_toggle = sched_sub.add_parser("job-toggle", help="Toggle scheduler job enabled/disabled")
    sched_job_toggle.add_argument("job_id", help="Job identifier (e.g. collect_all, sq_1)")
    sched_interval = sched_sub.add_parser("set-interval", help="Set scheduler job interval")
    sched_interval.add_argument("job_id", help="Job identifier")
    sched_interval.add_argument("minutes", type=int, help="Interval in minutes (1-1440)")
    sched_task_cancel = sched_sub.add_parser("task-cancel", help="Cancel a collection task")
    sched_task_cancel.add_argument("task_id", type=int, help="Task ID to cancel")
    sched_sub.add_parser("clear-pending", help="Clear all pending collection tasks")
