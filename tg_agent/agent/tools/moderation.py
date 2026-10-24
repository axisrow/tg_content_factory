"""Agent tools for content moderation (approve / reject generation runs)."""

from __future__ import annotations

from typing import Annotated

from claude_agent_sdk import tool
from mcp.types import ToolAnnotations

from tg_agent.agent.tools._categories import ToolCategory, ToolMeta
from tg_agent.agent.tools._registry import (
    _text_response,
    agent_decision_origin,
    arg_bool,
    require_confirmation,
)

# Permission metadata for this module's tools (#245). Single source of
# truth: permissions.py derives TOOL_CATEGORIES / MODULE_GROUPS /
# PHONE_BINDED_TOOLS from these declarations; invariants in
# tests/test_tool_permissions_autoderive.py keep them in sync with the
# @tool() definitions.
TOOL_GROUPS: list[tuple[str, dict[str, ToolMeta]]] = [
    ("Модерация", {
        "list_pending_moderation": ToolMeta(ToolCategory.READ),
        "view_moderation_run": ToolMeta(ToolCategory.READ),
        "approve_run": ToolMeta(ToolCategory.WRITE),
        "reject_run": ToolMeta(ToolCategory.WRITE),
        "bulk_approve_runs": ToolMeta(ToolCategory.WRITE),
        "bulk_reject_runs": ToolMeta(ToolCategory.WRITE),
    }),
]

_ON_BEHALF_ARG = Annotated[bool, "true только если владелец прямо попросил выполнить действие"]
_SUPPRESSED_NOTE = (
    "подавлено — решение владельца (или публикация) не перезаписывается"
)


def register(db, client_pool, embedding_service, **kwargs):
    tools = []

    async def _single_decision(args, run_id: int, run, status: str) -> int:
        """Write one moderation decision with agent provenance (#1315)."""
        origin = await agent_decision_origin(
            db,
            args,
            confirmation_passed=arg_bool(args, "confirm"),
            entity="generation_run",
            entity_key=run_id,
            entity_name=None,
            field="moderation_status",
            old_value=getattr(run, "moderation_status", None),
            new_value=status,
        )
        return await db.repos.generation_runs.set_moderation_status(
            run_id, status, origin=origin, actor="agent"
        )

    async def _bulk_decision(args, ids: list[int], status: str) -> list[int]:
        """Bulk moderation with agent provenance; the confirm gate has already passed."""
        origin = await agent_decision_origin(
            db,
            args,
            confirmation_passed=True,
            entity="generation_run",
            entity_key=None,
            entity_name=None,
            field="moderation_status",
            old_value=None,
            new_value=status,
        )
        skipped = await db.repos.generation_runs.set_moderation_status_bulk(
            ids, status, origin=origin, actor="agent"
        )
        return list(skipped or [])

    # ------------------------------------------------------------------
    # READ
    # ------------------------------------------------------------------

    @tool(
        "list_pending_moderation",
        "List generation runs awaiting moderation. Filter by pipeline_id. "
        "Use run_id from results with get_pipeline_run to view full text, then approve_run or reject_run.",
        {
            "pipeline_id": Annotated[int, "ID пайплайна для фильтрации"],
            "limit": Annotated[int, "Максимальное количество результатов"],
        },
    )
    async def list_pending_moderation(args):
        try:
            pipeline_id = args.get("pipeline_id")
            limit = int(args.get("limit", 20))
            runs = await db.repos.generation_runs.list_pending_moderation(
                pipeline_id=int(pipeline_id) if pipeline_id is not None else None,
                limit=limit,
            )
            if not runs:
                return _text_response("Нет черновиков на модерации.")
            lines = [f"На модерации ({len(runs)} шт.):"]
            for r in runs:
                preview = (r.generated_text or "")[:200]
                lines.append(
                    f"- run_id={r.id}, pipeline_id={r.pipeline_id}, "
                    f"created={getattr(r, 'created_at', 'unknown')}: {preview}"
                )
            return _text_response("\n".join(lines))
        except Exception as e:
            return _text_response(f"Ошибка получения очереди модерации: {e}")

    tools.append(list_pending_moderation)

    @tool(
        "view_moderation_run",
        "View full details of a specific generation run for moderation review: "
        "generated text, pipeline name, status, quality score. "
        "run_id from list_pending_moderation. Then use approve_run or reject_run.",
        {"run_id": Annotated[int, "ID генерации из list_pipeline_runs"]},
    )
    async def view_moderation_run(args):
        run_id = args.get("run_id")
        if run_id is None:
            return _text_response("Ошибка: run_id обязателен.")
        try:
            run = await db.repos.generation_runs.get(int(run_id))
            if run is None:
                return _text_response(f"Run id={run_id} не найден.")
            lines = [
                f"Run id={run.id} (pipeline_id={run.pipeline_id})",
                f"  Статус: {run.status}",
                f"  Модерация: {run.moderation_status}",
                f"  Создан: {run.created_at}",
                "",
                "Текст для проверки:",
                run.generated_text or "(пусто)",
            ]
            return _text_response("\n".join(lines))
        except Exception as e:
            return _text_response(f"Ошибка получения run: {e}")

    tools.append(view_moderation_run)

    # ------------------------------------------------------------------
    # WRITE
    # ------------------------------------------------------------------

    @tool(
        "approve_run",
        "Approve a generation run for publishing. Then use publish_pipeline_run to publish it. "
        "run_id from list_pending_moderation or list_pipeline_runs. The owner's own "
        "moderation decision is never overridden unless the owner asked for it "
        "(on_behalf_of_user=true) and confirmed (confirm=true).",
        {
            "run_id": Annotated[int, "ID генерации из list_pipeline_runs"],
            "confirm": Annotated[bool, "Подтверждение действия владельцем"],
            "on_behalf_of_user": _ON_BEHALF_ARG,
        },
    )
    async def approve_run(args):
        run_id = args.get("run_id")
        if run_id is None:
            return _text_response("Ошибка: run_id обязателен.")
        try:
            run = await db.repos.generation_runs.get(int(run_id))
            if run is None:
                return _text_response(f"Run id={run_id} не найден.")
            changed = await _single_decision(args, int(run_id), run, "approved")
            if changed == 0:
                return _text_response(f"Одобрение run id={run_id} {_SUPPRESSED_NOTE}.")
            return _text_response(f"Run id={run_id} одобрен для публикации.")
        except Exception as e:
            return _text_response(f"Ошибка одобрения: {e}")

    tools.append(approve_run)

    @tool(
        "reject_run",
        "Reject a generation run. Provide the run_id. The owner's own moderation "
        "decision is never overridden unless the owner asked for it "
        "(on_behalf_of_user=true) and confirmed (confirm=true).",
        {
            "run_id": Annotated[int, "ID генерации из list_pipeline_runs"],
            "confirm": Annotated[bool, "Подтверждение действия владельцем"],
            "on_behalf_of_user": _ON_BEHALF_ARG,
        },
    )
    async def reject_run(args):
        run_id = args.get("run_id")
        if run_id is None:
            return _text_response("Ошибка: run_id обязателен.")
        try:
            run = await db.repos.generation_runs.get(int(run_id))
            if run is None:
                return _text_response(f"Run id={run_id} не найден.")
            changed = await _single_decision(args, int(run_id), run, "rejected")
            if changed == 0:
                return _text_response(f"Отклонение run id={run_id} {_SUPPRESSED_NOTE}.")
            return _text_response(f"Run id={run_id} отклонён.")
        except Exception as e:
            return _text_response(f"Ошибка отклонения: {e}")

    tools.append(reject_run)

    # ------------------------------------------------------------------
    # BULK WRITE + confirm
    # ------------------------------------------------------------------

    @tool(
        "bulk_approve_runs",
        "Approve multiple generation runs at once. Provide comma-separated run_ids (e.g. '1,2,3'). "
        "Requires confirm=true. Runs the owner already decided on are skipped unless the "
        "owner asked for this (on_behalf_of_user=true).",
        {
            "run_ids": Annotated[str, "ID генераций через запятую (например 1,2,3)"],
            "confirm": Annotated[bool, "Установите true для подтверждения действия"],
            "on_behalf_of_user": _ON_BEHALF_ARG,
        },
        annotations=ToolAnnotations(destructiveHint=False),
    )
    async def bulk_approve_runs(args):
        raw = args.get("run_ids", "")
        try:
            ids = [int(x.strip()) for x in raw.split(",") if x.strip()]
        except ValueError:
            return _text_response("Ошибка: run_ids должны быть числами через запятую.")
        if not ids:
            return _text_response("Ошибка: run_ids пуст.")
        gate = require_confirmation(f"одобрит {len(ids)} run(s) для публикации", args)
        if gate:
            return gate
        try:
            # Atomic batch (#1041): one transaction for all ids — either every
            # run flips to "approved" or none does. The previous per-id loop
            # autocommitted each update, so a mid-batch failure left earlier
            # ids approved with no rollback yet still reported full success.
            skipped = await _bulk_decision(args, ids, "approved")
            return _text_response(_bulk_summary("Одобрено", ids, skipped))
        except Exception as e:
            return _text_response(f"Ошибка массового одобрения: {e}")

    tools.append(bulk_approve_runs)

    @tool(
        "bulk_reject_runs",
        "Reject multiple generation runs at once. Provide comma-separated run_ids (e.g. '1,2,3'). "
        "Requires confirm=true. Runs the owner already decided on are skipped unless the "
        "owner asked for this (on_behalf_of_user=true).",
        {
            "run_ids": Annotated[str, "ID генераций через запятую (например 1,2,3)"],
            "confirm": Annotated[bool, "Установите true для подтверждения действия"],
            "on_behalf_of_user": _ON_BEHALF_ARG,
        },
        annotations=ToolAnnotations(destructiveHint=False),
    )
    async def bulk_reject_runs(args):
        raw = args.get("run_ids", "")
        try:
            ids = [int(x.strip()) for x in raw.split(",") if x.strip()]
        except ValueError:
            return _text_response("Ошибка: run_ids должны быть числами через запятую.")
        if not ids:
            return _text_response("Ошибка: run_ids пуст.")
        gate = require_confirmation(f"отклонит {len(ids)} run(s)", args)
        if gate:
            return gate
        try:
            # Atomic batch (#1041): all ids reject together or none does. See
            # bulk_approve_runs for the rationale on the prior per-id loop.
            skipped = await _bulk_decision(args, ids, "rejected")
            return _text_response(_bulk_summary("Отклонено", ids, skipped))
        except Exception as e:
            return _text_response(f"Ошибка массового отклонения: {e}")

    tools.append(bulk_reject_runs)

    return tools


def _bulk_summary(verb: str, ids: list[int], skipped: list[int]) -> str:
    applied = [run_id for run_id in ids if run_id not in skipped]
    text = f"{verb} {len(applied)} run(s): {applied}"
    if skipped:
        text += f"; пропущено {len(skipped)}: {skipped} ({_SUPPRESSED_NOTE} или run не найден)"
    return text
