"""Install with ``pip install ./examples/plugin_digest`` and restart TG Agent."""
from datetime import UTC, datetime, timedelta

from tg_agent.plugin_api import Plugin, PluginContext, text_response, tool


async def setup(ctx: PluginContext) -> None:
    await ctx.db.execute_write(
        "CREATE TABLE IF NOT EXISTS plugin_my_digest_runs (ts TEXT NOT NULL)"
    )


def register(ctx: PluginContext) -> list:
    @tool("digest_today", "Собранные сообщения за последние 24 часа", {
        "type": "object",
        "properties": {"limit": {"type": "integer", "minimum": 1, "maximum": 100, "default": 20}},
        "additionalProperties": False,
    }, category="read")
    async def digest_today(args):
        limit = args.get("limit", 20)
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 100:
            return text_response("limit должен быть целым числом от 1 до 100")
        now = datetime.now(UTC)
        since = (now - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
        rows = await ctx.db.repos.messages.get_messages_collected_since(since, limit=limit)
        await ctx.db.execute_write("INSERT INTO plugin_my_digest_runs (ts) VALUES (?)", (now.isoformat(),))
        return text_response("\n".join((row.text or "")[:80] for row in rows) or "Нет собранных сообщений за 24 часа.")

    return [digest_today]


PLUGIN = Plugin(id="my_digest", version="0.1.0", requires_api=1, setup=setup, register=register)
