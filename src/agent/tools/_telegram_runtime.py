from __future__ import annotations

from src.agent.tools._registry import (
    find_dialogs_by_exact_title,
    format_dialog_title_candidates,
)


async def prepare_telegram_tool(ctx, args: dict, *, tool_name: str, action: str) -> tuple[str, dict | None]:
    live_gate = ctx.require_live_runtime(action, tool_name=tool_name)
    if live_gate:
        return "", live_gate
    phone, err = await ctx.resolve_phone(args.get("phone", ""))
    if err:
        return "", err
    perm_gate = await ctx.require_phone_permission(phone, tool_name)
    if perm_gate:
        return "", perm_gate
    return phone, None


async def find_single_dialog_id_by_title(
    db,
    client_pool,
    phone: str,
    title: object,
) -> tuple[str, dict | None]:
    matches = await find_dialogs_by_exact_title(
        db,
        client_pool,
        phone,
        title,
        allow_refresh=True,
    )
    if not matches:
        return "", None
    if len(matches) > 1:
        return "", format_dialog_title_candidates(title, matches)
    dialog_id = matches[0].get("channel_id")
    if dialog_id in (None, ""):
        return "", None
    return str(dialog_id), None
