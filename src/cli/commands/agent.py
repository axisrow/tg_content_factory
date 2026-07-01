"""Shared async bodies for the ``agent`` CLI group (epic #959, Wave 3 — #1123).

Migrated off the argparse dispatcher onto the Typer ``app`` (see
``src/cli/typer_commands.py``). Each leaf sub-command is a plain ``async def
*_impl`` here — no local ``asyncio.run`` and no ``argparse.Namespace``. A thin
``run(args)`` adapter is kept for the argparse leaf audit and existing tests.

``test-escaping`` / ``test-tools`` and the ``chat`` streaming flow are delicate
(agent backend, StringSession-bound state); only the argparse→Typer wiring
changed — the streaming/escaping logic is preserved verbatim.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys

import typer

from src.cli import runtime
from src.cli.commands.common import (
    apply_startup,
    run_async,
)

logger = logging.getLogger(__name__)

_ESCAPING_CASES = [
    ("xml_tags", "Привет <user>тег</user> и <assistant>тег</assistant>"),
    ("quotes", "Он сказал \"привет\" и 'пока'"),
    ("backslashes", "C:\\Users\\test\\file.txt"),
    ("newlines", "строка 1\nстрока 2\n\nстрока 3"),
    ("json_in_text", '{"key": "value", "arr": [1,2,3]}'),
    ("code_block", "```python\nprint('hello')\n```"),
    ("unicode_emoji", "Привет 🎉 мир 🌍 тест ✅"),
    ("special_chars", "a & b < c > d \"e\" 'f'"),
    ("markdown_links", "[ссылка](https://example.com?a=1&b=2)"),
    ("curly_braces", "{{template}} ${variable} %(format)s"),
]


async def _test_escaping(db, config) -> None:
    from src.agent.manager import AgentManager

    mgr = AgentManager(db, config)
    await mgr.refresh_settings_cache(preflight=True)
    if not mgr.available:
        print("Ни claude-agent-sdk, ни deepagents fallback не настроены — пропуск.")
        return
    mgr.initialize()

    thread_id = await db.create_agent_thread("test-escaping")
    passed, failed = 0, 0

    for name, text in _ESCAPING_CASES:
        print(f"  [{name}] ", end="", flush=True)
        await db.save_agent_message(thread_id, "user", text)
        try:
            full_text = ""
            error = None
            async for chunk in mgr.chat_stream(thread_id, text):
                raw = chunk.removeprefix("data: ").strip()
                try:
                    payload = json.loads(raw)
                except Exception:
                    continue
                if "text" in payload:
                    full_text += payload["text"]
                if payload.get("done"):
                    break
                if "error" in payload:
                    error = payload["error"]
                    break

            if error:
                print(f"FAIL — {error}")
                failed += 1
            else:
                await db.save_agent_message(thread_id, "assistant", full_text)
                preview = full_text[:60].replace("\n", "\\n")
                print(f"OK — {preview}...")
                passed += 1
        except Exception as e:
            print(f"FAIL — exception: {e}")
            failed += 1

    await db.delete_agent_thread(thread_id)
    print(f"\nИтого: {passed} passed, {failed} failed из {len(_ESCAPING_CASES)}")


async def _test_tools(db, config) -> None:
    """Send a prompt that guarantees a tool call and verify tool_start/tool_end events arrive."""
    from src.agent.manager import AgentManager

    mgr = AgentManager(db, config)
    await mgr.refresh_settings_cache(preflight=True)
    if not mgr.available:
        print("Агент не настроен (нет API-ключа) — пропуск.")
        return
    mgr.initialize()

    # Prompts ordered from most to least likely to trigger a tool call.
    # We stop at the first one that succeeds.
    cases = [
        ("list_channels", "Перечисли все каналы в базе данных. Используй инструмент list_channels."),
        ("search_messages", "Найди сообщения в базе данных по слову 'test'. Используй поиск."),
    ]

    thread_id = await db.create_agent_thread("test-tools")
    passed = failed = 0

    try:
        for name, prompt in cases:
            print(f"  [{name}] ", end="", flush=True)
            await db.save_agent_message(thread_id, "user", prompt)
            try:
                tool_starts: list[str] = []
                tool_ends: list[str] = []
                full_text = ""
                error = None

                async for chunk in mgr.chat_stream(thread_id, prompt):
                    raw = chunk.removeprefix("data: ").strip()
                    try:
                        payload = json.loads(raw)
                    except Exception:
                        continue
                    t = payload.get("type")
                    if t == "tool_start":
                        tool_starts.append(payload.get("tool", ""))
                    elif t == "tool_end":
                        tool_ends.append(payload.get("tool", ""))
                    elif "text" in payload:
                        full_text += payload["text"]
                    if payload.get("done"):
                        break
                    if "error" in payload:
                        error = payload["error"]
                        break

                if error:
                    print(f"FAIL — ошибка агента: {error}")
                    failed += 1
                elif not tool_starts:
                    preview = full_text[:80].replace("\n", "\\n")
                    print(f"FAIL — tool_start не получен. Ответ: {preview}...")
                    failed += 1
                elif tool_starts != tool_ends:
                    print(f"FAIL — tool_start={tool_starts} не совпадает с tool_end={tool_ends}")
                    failed += 1
                else:
                    tools_str = ", ".join(tool_starts)
                    print(f"OK — инструменты вызваны: {tools_str}")
                    passed += 1

                await db.save_agent_message(thread_id, "assistant", full_text)

            except Exception as e:
                print(f"FAIL — исключение: {e}")
                failed += 1
    finally:
        await db.delete_agent_thread(thread_id)

    print(f"\nИтого: {passed} passed, {failed} failed из {len(cases)}")
    if failed:
        sys.exit(1)


async def threads_impl(config_path: str) -> None:
    """List agent threads."""
    _, db = await runtime.init_db(config_path)
    try:
        threads = await db.get_agent_threads()
        if not threads:
            print("Нет тредов.")
        else:
            for t in threads:
                print(f"[{t['id']}] {t['title']}  ({t['created_at']})")
    finally:
        await db.close()


async def thread_create_impl(config_path: str, *, title: str | None = None) -> None:
    """Create a new agent thread."""
    _, db = await runtime.init_db(config_path)
    try:
        title = title or "Новый тред"
        tid = await db.create_agent_thread(title)
        print(f"Создан тред #{tid}: {title}")
    finally:
        await db.close()


async def thread_delete_impl(config_path: str, *, thread_id: int) -> None:
    """Delete an agent thread."""
    _, db = await runtime.init_db(config_path)
    try:
        await db.delete_agent_thread(thread_id)
        print(f"Тред #{thread_id} удалён.")
    finally:
        await db.close()


async def chat_impl(
    config_path: str,
    *,
    prompt: str | None = None,
    thread_id: int | None = None,
    model: str | None = None,
) -> None:
    """Interactive TUI chat, or a one-shot message when ``prompt`` is given."""
    from src.agent.manager import AgentManager

    # chat streams to stdout; move log noise to a file so it doesn't interleave.
    removed_log_handler = runtime.redirect_logging_to_file()
    config, db = await runtime.init_db(config_path)
    auth = pool = mgr = None
    try:
        auth, pool = await runtime.init_pool(config, db)
        mgr = AgentManager(db, config, client_pool=pool)
        await mgr.refresh_settings_cache(preflight=True)
        mgr.initialize()

        if prompt:
            if thread_id:
                resolved_thread_id = thread_id
            else:
                resolved_thread_id = await db.create_agent_thread("Новый тред")
                print(f"(создан тред #{resolved_thread_id})")

            await db.save_agent_message(resolved_thread_id, "user", prompt)

            full_text = ""
            text_started = False
            inline_status = False  # True when \r status is on screen
            async for chunk in mgr.chat_stream(resolved_thread_id, prompt, model=model):
                raw = chunk.removeprefix("data: ").strip()
                try:
                    payload = json.loads(raw)
                except Exception:
                    continue
                event_type = payload.get("type")
                if event_type in ("status", "countdown"):
                    if not text_started:
                        text = payload.get("text", "")
                        print(f"\r⏳ {text:<60}",
                              end="", file=sys.stderr, flush=True)
                        inline_status = True
                    continue
                # Clear inline status before any other output
                if inline_status:
                    print(f"\r{' ' * 64}\r",
                          end="", file=sys.stderr, flush=True)
                    inline_status = False
                if event_type == "tool_start":
                    print(f"🔧 {payload.get('tool', 'tool')}...",
                          file=sys.stderr, flush=True)
                    continue
                if event_type == "tool_end":
                    tool = payload.get("tool", "tool")
                    dur = payload.get("duration", 0)
                    icon = "❌" if payload.get("is_error") else "✅"
                    summary = payload.get("summary", "")
                    line = f"  {icon} {tool} ({dur}s)"
                    if summary:
                        line += f" — {summary}"
                    print(line, file=sys.stderr, flush=True)
                    continue
                if event_type in ("thinking", "tool_result"):
                    continue
                if "text" in payload:
                    if not text_started:
                        print("Агент: ", end="", flush=True)
                        text_started = True
                    print(payload["text"], end="", flush=True)
                    full_text += payload.get("text", "")
                if payload.get("done"):
                    print()
                    break
                if "error" in payload:
                    print(f"\nОшибка: {payload['error']}")
                    if payload.get("details"):
                        print(f"Детали: {payload['details']}", file=sys.stderr)
                    await db.delete_last_agent_exchange(resolved_thread_id)
                    break

            if full_text:
                await db.save_agent_message(resolved_thread_id, "assistant", full_text)
        else:
            # Interactive TUI mode
            from src.cli.commands.agent_tui import AgentTuiApp

            app = AgentTuiApp(db=db, config=config, agent_manager=mgr)
            await app.run_async()
    finally:
        if mgr is not None:
            try:
                await mgr.close_all()
            except Exception:
                logger.debug("Failed to close agent manager", exc_info=True)
        if pool is not None:
            try:
                await pool.disconnect_all()
            except Exception:
                logger.debug("Failed to disconnect pool", exc_info=True)
        if auth is not None:
            try:
                await auth.cleanup()
            except Exception:
                logger.debug("Failed to cleanup auth", exc_info=True)
        try:
            await db.close()
        except Exception:
            logger.debug("Failed to close database", exc_info=True)
        if removed_log_handler is not None:
            runtime.restore_logging(removed_log_handler)


async def thread_rename_impl(config_path: str, *, thread_id: int, title: str) -> None:
    """Rename an agent thread."""
    _, db = await runtime.init_db(config_path)
    try:
        await db.rename_agent_thread(thread_id, title[:100])
        print(f"Тред #{thread_id} переименован: {title[:100]}")
    finally:
        await db.close()


async def thread_stop_impl(config_path: str, *, thread_id: int) -> None:
    """Stop/cancel an ongoing agent response for a thread."""
    from src.agent.manager import AgentManager

    config, db = await runtime.init_db(config_path)
    mgr = None
    try:
        mgr = AgentManager(db, config)
        cancelled = await mgr.cancel_stream(thread_id)
        if cancelled:
            # Only safe to delete once we've actually cancelled the in-process
            # task. A fresh CLI AgentManager has an empty _active_tasks, so a
            # stream running in the web/worker process is NOT cancelled here;
            # deleting the last exchange then races that stream, which could
            # persist an assistant reply with no preceding user message. (#737)
            await db.delete_last_agent_exchange(thread_id)
            print(f"Тред #{thread_id}: генерация остановлена; последний обмен удалён.")
        else:
            print(
                f"Тред #{thread_id}: активной генерации в этом процессе нет "
                "(возможно, выполняется в worker/web). Обмен не удалён, чтобы не "
                "осиротить ответ всё ещё работающего стрима."
            )
    finally:
        if mgr is not None:
            try:
                await mgr.close_all()
            except Exception:
                logger.debug("Failed to close agent manager", exc_info=True)
        await db.close()


async def messages_impl(config_path: str, *, thread_id: int, limit: int | None = None) -> None:
    """Show messages in an agent thread."""
    _, db = await runtime.init_db(config_path)
    try:
        msgs = await db.get_agent_messages(thread_id)
        if limit:
            msgs = msgs[-limit:]
        if not msgs:
            print("Нет сообщений.")
        else:
            for m in msgs:
                role = "user" if m["role"] == "user" else "assistant"
                preview = m["content"][:200].replace("\n", "\\n")
                print(f"  [{role}] [{m['created_at']}] {preview}")
    finally:
        await db.close()


async def context_impl(
    config_path: str,
    *,
    thread_id: int,
    channel_id: int,
    limit: int = 100000,
    topic_id: int | None = None,
) -> None:
    """Inject channel context (messages) into an agent thread."""
    _, db = await runtime.init_db(config_path)
    try:
        thread = await db.get_agent_thread(thread_id)
        if thread is None:
            print(f"Тред #{thread_id} не найден.")
            return

        messages, _ = await db.search_messages(
            query="",
            channel_id=channel_id,
            limit=limit,
            topic_id=topic_id,
        )
        ch = await db.get_channel_by_channel_id(channel_id)
        title = ch.title if ch else str(channel_id)

        topics = await db.get_forum_topics(channel_id)
        topics_map = {t["id"]: t["title"] for t in topics}

        from src.agent.context import format_context

        content = format_context(messages, title, topic_id, topics_map)

        await db.save_agent_message(thread_id=thread_id, role="user", content=content)
        logger.info(
            "Context loaded for thread %d: %d messages, %d chars",
            thread_id,
            len(messages),
            len(content),
        )
        if len(content) > 200_000:
            logger.warning(
                "Large context for thread %d: %d chars (>200K)"
                " — may cause prompt overflow",
                thread_id,
                len(content),
            )
        print(content[:500])
        if len(content) > 500:
            print(f"... ({len(content)} символов всего)")
    finally:
        await db.close()


async def test_escaping_impl(config_path: str) -> None:
    """Test agent handling of special characters (delicate — logic unchanged)."""
    config, db = await runtime.init_db(config_path)
    try:
        await _test_escaping(db, config)
    finally:
        await db.close()


async def test_tools_impl(config_path: str) -> None:
    """Test that agent tool calls produce tool_start/tool_end events (delicate)."""
    config, db = await runtime.init_db(config_path)
    try:
        await _test_tools(db, config)
    finally:
        await db.close()


def run(args: argparse.Namespace) -> None:
    """Thin argparse adapter over the ``*_impl`` bodies (legacy dispatch path).

    The production CLI routes ``agent`` through the Typer ``app`` (#1123); this
    wrapper keeps the argparse leaf audit and command-level tests working. Args are
    read via ``getattr`` defaults so partial test Namespaces stay usable (#1117).
    """
    action = getattr(args, "agent_action", None)
    if action == "threads":
        asyncio.run(threads_impl(args.config))
    elif action == "thread-create":
        asyncio.run(thread_create_impl(args.config, title=getattr(args, "title", None)))
    elif action == "thread-delete":
        asyncio.run(thread_delete_impl(args.config, thread_id=args.thread_id))
    elif action == "chat":
        asyncio.run(
            chat_impl(
                args.config,
                prompt=getattr(args, "prompt", None),
                thread_id=getattr(args, "thread_id", None),
                model=getattr(args, "model", None),
            )
        )
    elif action == "thread-rename":
        asyncio.run(thread_rename_impl(args.config, thread_id=args.thread_id, title=args.title))
    elif action == "thread-stop":
        asyncio.run(thread_stop_impl(args.config, thread_id=args.thread_id))
    elif action == "messages":
        asyncio.run(messages_impl(args.config, thread_id=args.thread_id, limit=getattr(args, "limit", None)))
    elif action == "context":
        asyncio.run(
            context_impl(
                args.config,
                thread_id=args.thread_id,
                channel_id=args.channel_id,
                limit=getattr(args, "limit", 100000),
                topic_id=getattr(args, "topic_id", None),
            )
        )
    elif action == "test-escaping":
        asyncio.run(test_escaping_impl(args.config))
    elif action == "test-tools":
        asyncio.run(test_tools_impl(args.config))


# --------------------------------------------------------------------------- #
# agent → threads / thread-create / thread-delete / chat / thread-rename /
#         thread-stop / messages / context / test-escaping / test-tools
# --------------------------------------------------------------------------- #

agent_app = typer.Typer(no_args_is_help=True, help="Agent chat management")


@agent_app.command("threads")
def agent_threads(ctx: typer.Context) -> None:
    """List agent threads."""
    apply_startup(ctx)
    run_async(threads_impl(ctx.obj.config))


@agent_app.command("thread-create")
def agent_thread_create(
    ctx: typer.Context,
    title: str | None = typer.Option(None, "--title", help="Thread title"),
) -> None:
    """Create new thread."""
    apply_startup(ctx)
    run_async(thread_create_impl(ctx.obj.config, title=title))


@agent_app.command("thread-delete")
def agent_thread_delete(
    ctx: typer.Context,
    thread_id: int = typer.Argument(..., help="Thread ID"),
) -> None:
    """Delete thread."""
    apply_startup(ctx)
    run_async(thread_delete_impl(ctx.obj.config, thread_id=thread_id))


@agent_app.command("chat")
def agent_chat(
    ctx: typer.Context,
    prompt: str | None = typer.Option(
        None, "-p", "--prompt", help="Message text (non-interactive mode)"
    ),
    thread_id: int | None = typer.Option(None, "--thread-id"),
    model: str | None = typer.Option(None, "--model", help="Model name"),
) -> None:
    """Interactive TUI chat or one-shot message (with -p)."""
    apply_startup(ctx)
    run_async(chat_impl(ctx.obj.config, prompt=prompt, thread_id=thread_id, model=model))


@agent_app.command("thread-rename")
def agent_thread_rename(
    ctx: typer.Context,
    thread_id: int = typer.Argument(..., help="Thread ID"),
    title: str = typer.Argument(..., help="New title"),
) -> None:
    """Rename thread."""
    apply_startup(ctx)
    run_async(thread_rename_impl(ctx.obj.config, thread_id=thread_id, title=title))


@agent_app.command("thread-stop")
def agent_thread_stop(
    ctx: typer.Context,
    thread_id: int = typer.Argument(..., help="Thread ID"),
) -> None:
    """Stop/cancel an ongoing agent response for a thread."""
    apply_startup(ctx)
    run_async(thread_stop_impl(ctx.obj.config, thread_id=thread_id))


@agent_app.command("messages")
def agent_messages(
    ctx: typer.Context,
    thread_id: int = typer.Argument(..., help="Thread ID"),
    limit: int | None = typer.Option(None, "--limit", help="Last N messages"),
) -> None:
    """Show thread messages."""
    apply_startup(ctx)
    run_async(messages_impl(ctx.obj.config, thread_id=thread_id, limit=limit))


@agent_app.command("context")
def agent_context(
    ctx: typer.Context,
    thread_id: int = typer.Argument(..., help="Thread ID"),
    channel_id: int = typer.Option(..., "--channel-id"),
    limit: int = typer.Option(100000, "--limit", help="Max messages"),
    topic_id: int | None = typer.Option(None, "--topic-id"),
) -> None:
    """Inject channel context into thread."""
    apply_startup(ctx)
    run_async(
        context_impl(
            ctx.obj.config,
            thread_id=thread_id,
            channel_id=channel_id,
            limit=limit,
            topic_id=topic_id,
        )
    )


@agent_app.command("test-escaping")
def agent_test_escaping(ctx: typer.Context) -> None:
    """Test agent with special characters."""
    apply_startup(ctx)
    run_async(test_escaping_impl(ctx.obj.config))


@agent_app.command("test-tools")
def agent_test_tools(ctx: typer.Context) -> None:
    """Test that agent tool calls produce tool_start/tool_end events."""
    apply_startup(ctx)
    run_async(test_tools_impl(ctx.obj.config))
