"""Agent tools for scheduler management."""

from __future__ import annotations

from claude_agent_sdk import tool

from src.agent.tools._registry import _text_response, require_confirmation, require_pool


def register(db, client_pool, embedding_service, **kwargs):
    scheduler_manager = kwargs.get("scheduler_manager")
    tools = []

    def _get_mgr():
        """Return the live scheduler manager or raise."""
        if scheduler_manager is not None:
            return scheduler_manager
        raise RuntimeError(
            "Планировщик недоступен — live SchedulerManager не передан. "
            "Эта операция доступна только через web-интерфейс."
        )

    @tool("get_scheduler_status", "Get current scheduler status, jobs, and next run times", {})
    async def get_scheduler_status(args):
        try:
            mgr = _get_mgr()
            running = mgr.is_running
            jobs = await mgr.get_potential_jobs()
            next_runs = mgr.get_all_jobs_next_run()
            lines = [
                f"Планировщик: {'запущен' if running else 'остановлен'}",
                f"Интервал: {mgr.interval_minutes} мин",
                f"Задачи ({len(jobs)}):",
            ]
            for j in jobs:
                job_id = j.get("id", "?")
                enabled = j.get("enabled", False)
                next_run = next_runs.get(job_id, "—")
                lines.append(f"  - {job_id}: {'вкл' if enabled else 'выкл'}, след.запуск: {next_run}")
            return _text_response("\n".join(lines))
        except Exception as e:
            return _text_response(f"Ошибка получения статуса планировщика: {e}")

    tools.append(get_scheduler_status)

    @tool(
        "start_scheduler",
        "⚠️ Start the scheduler for periodic message collection. Ask user for confirmation first.",
        {"confirm": bool},
    )
    async def start_scheduler(args):
        pool_gate = require_pool(client_pool, "Запуск планировщика")
        if pool_gate:
            return pool_gate
        gate = require_confirmation("запустит планировщик периодического сбора сообщений", args)
        if gate:
            return gate
        try:
            mgr = _get_mgr()
            await mgr.start()
            return _text_response("Планировщик запущен.")
        except Exception as e:
            return _text_response(f"Ошибка запуска планировщика: {e}")

    tools.append(start_scheduler)

    @tool(
        "stop_scheduler",
        "⚠️ Stop the scheduler. Ask user for confirmation first.",
        {"confirm": bool},
    )
    async def stop_scheduler(args):
        gate = require_confirmation("остановит планировщик", args)
        if gate:
            return gate
        try:
            mgr = _get_mgr()
            await mgr.stop()
            return _text_response("Планировщик остановлен.")
        except Exception as e:
            return _text_response(f"Ошибка остановки планировщика: {e}")

    tools.append(stop_scheduler)

    @tool(
        "trigger_collection",
        "⚠️ Trigger immediate collection run. Ask user for confirmation first.",
        {"confirm": bool},
    )
    async def trigger_collection(args):
        pool_gate = require_pool(client_pool, "Запуск сбора")
        if pool_gate:
            return pool_gate
        gate = require_confirmation("немедленно запустит сбор сообщений из всех активных каналов", args)
        if gate:
            return gate
        try:
            mgr = _get_mgr()
            result = await mgr.trigger_now()
            return _text_response(f"Сбор запущен: {result}")
        except Exception as e:
            return _text_response(f"Ошибка запуска сбора: {e}")

    tools.append(trigger_collection)

    @tool("toggle_scheduler_job", "Toggle a scheduler job on/off by job_id", {"job_id": str})
    async def toggle_scheduler_job(args):
        job_id = args.get("job_id", "")
        if not job_id:
            return _text_response("Ошибка: job_id обязателен.")
        try:
            mgr = _get_mgr()
            current = await mgr.is_job_enabled(job_id)
            new_state = not current
            await mgr.sync_job_state(job_id, new_state)
            status = "включена" if new_state else "выключена"
            return _text_response(f"Задача '{job_id}' {status}.")
        except Exception as e:
            return _text_response(f"Ошибка переключения задачи: {e}")

    tools.append(toggle_scheduler_job)

    # ------------------------------------------------------------------
    # set_scheduler_interval (WRITE + confirm)
    # ------------------------------------------------------------------

    @tool(
        "set_scheduler_interval",
        "Set the collection interval (in minutes). Currently only 'collect_all' is supported. Range: 1–1440.",
        {"job_id": str, "minutes": int, "confirm": bool},
    )
    async def set_scheduler_interval(args):
        job_id = args.get("job_id", "")
        if not job_id:
            return _text_response("Ошибка: job_id обязателен.")
        if job_id != "collect_all":
            return _text_response(
                f"Ошибка: интервал можно установить только для 'collect_all'. "
                f"Получено: '{job_id}'."
            )
        minutes = args.get("minutes")
        if minutes is None:
            return _text_response("Ошибка: minutes обязателен.")
        minutes = max(1, min(int(minutes), 1440))
        gate = require_confirmation(f"установит интервал сбора = {minutes} мин", args)
        if gate:
            return gate
        try:
            await db.repos.settings.set_setting("collect_interval_minutes", str(minutes))
            return _text_response(f"Интервал сбора установлен: {minutes} мин.")
        except Exception as e:
            return _text_response(f"Ошибка установки интервала: {e}")

    tools.append(set_scheduler_interval)

    # ------------------------------------------------------------------
    # cancel_scheduler_task (WRITE + confirm)
    # ------------------------------------------------------------------

    @tool(
        "cancel_scheduler_task",
        "Cancel a collection task by task_id. Works for pending tasks; running tasks will be "
        "marked cancelled in DB but the active collection may continue until completion. "
        "Requires confirmation.",
        {"task_id": int, "confirm": bool},
    )
    async def cancel_scheduler_task(args):
        task_id = args.get("task_id")
        if task_id is None:
            return _text_response("Ошибка: task_id обязателен.")
        gate = require_confirmation(f"отменит задачу id={task_id}", args)
        if gate:
            return gate
        try:
            ok = await db.repos.tasks.cancel_collection_task(int(task_id))
            if ok:
                return _text_response(f"Задача {task_id} отменена.")
            return _text_response(f"Задача {task_id} не найдена или уже завершена.")
        except Exception as e:
            return _text_response(f"Ошибка отмены задачи: {e}")

    tools.append(cancel_scheduler_task)

    # ------------------------------------------------------------------
    # clear_pending_tasks (WRITE + confirm)
    # ------------------------------------------------------------------

    @tool(
        "clear_pending_tasks",
        "Clear all pending collection tasks from the queue. Requires confirmation.",
        {"confirm": bool},
    )
    async def clear_pending_tasks(args):
        gate = require_confirmation("удалит все ожидающие задачи сбора из очереди", args)
        if gate:
            return gate
        try:
            deleted = await db.repos.tasks.delete_pending_channel_tasks()
            return _text_response(f"Удалено ожидающих задач: {deleted}.")
        except Exception as e:
            return _text_response(f"Ошибка очистки очереди: {e}")

    tools.append(clear_pending_tasks)

    return tools
