from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys

from src.cli import runtime

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


def run(args: argparse.Namespace) -> None:
    async def _run() -> None:
        removed_log_handler = None
        action = args.agent_action
        if action == "chat":
            removed_log_handler = runtime.redirect_logging_to_file()
        config, db = await runtime.init_db(args.config)
        auth = pool = mgr = None
        try:

            if action == "threads":
                threads = await db.get_agent_threads()
                if not threads:
                    print("Нет тредов.")
                else:
                    for t in threads:
                        print(f"[{t['id']}] {t['title']}  ({t['created_at']})")

            elif action == "thread-create":
                title = getattr(args, "title", None) or "Новый тред"
                tid = await db.create_agent_thread(title)
                print(f"Создан тред #{tid}: {title}")

            elif action == "thread-delete":
                await db.delete_agent_thread(args.thread_id)
                print(f"Тред #{args.thread_id} удалён.")

            elif action == "chat":
                from src.agent.manager import AgentManager

                auth, pool = await runtime.init_pool(config, db)
                mgr = AgentManager(db, config, client_pool=pool)
                await mgr.refresh_settings_cache(preflight=True)
                mgr.initialize()

                if args.prompt:
                    if args.thread_id:
                        thread_id = args.thread_id
                    else:
                        thread_id = await db.create_agent_thread("Новый тред")
                        print(f"(создан тред #{thread_id})")

                    await db.save_agent_message(thread_id, "user", args.prompt)

                    model = getattr(args, "model", None)
                    full_text = ""
                    text_started = False
                    inline_status = False  # True when \r status is on screen
                    async for chunk in mgr.chat_stream(thread_id, args.prompt, model=model):
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
                            await db.delete_last_agent_exchange(thread_id)
                            break

                    if full_text:
                        await db.save_agent_message(thread_id, "assistant", full_text)
                else:
                    # Interactive TUI mode
                    from src.cli.commands.agent_tui import AgentTuiApp

                    app = AgentTuiApp(db=db, config=config, agent_manager=mgr)
                    await app.run_async()

            elif action == "thread-rename":
                await db.rename_agent_thread(args.thread_id, args.title[:100])
                print(f"Тред #{args.thread_id} переименован: {args.title[:100]}")

            elif action == "messages":
                msgs = await db.get_agent_messages(args.thread_id)
                if args.limit:
                    msgs = msgs[-args.limit :]
                if not msgs:
                    print("Нет сообщений.")
                else:
                    for m in msgs:
                        role = "user" if m["role"] == "user" else "assistant"
                        preview = m["content"][:200].replace("\n", "\\n")
                        print(f"  [{role}] [{m['created_at']}] {preview}")

            elif action == "context":
                thread = await db.get_agent_thread(args.thread_id)
                if thread is None:
                    print(f"Тред #{args.thread_id} не найден.")
                    return

                messages, _ = await db.search_messages(
                    query="",
                    channel_id=args.channel_id,
                    limit=args.limit,
                    topic_id=args.topic_id,
                )
                ch = await db.get_channel_by_channel_id(args.channel_id)
                title = ch.title if ch else str(args.channel_id)

                topics = await db.get_forum_topics(args.channel_id)
                topics_map = {t["id"]: t["title"] for t in topics}

                from src.agent.context import format_context

                content = format_context(messages, title, args.topic_id, topics_map)

                await db.save_agent_message(thread_id=args.thread_id, role="user", content=content)
                logger.info(
                    "Context loaded for thread %d: %d messages, %d chars",
                    args.thread_id,
                    len(messages),
                    len(content),
                )
                if len(content) > 200_000:
                    logger.warning(
                        "Large context for thread %d: %d chars (>200K)"
                        " — may cause prompt overflow",
                        args.thread_id,
                        len(content),
                    )
                print(content[:500])
                if len(content) > 500:
                    print(f"... ({len(content)} символов всего)")

            elif action == "test-escaping":
                await _test_escaping(db, config)

            elif action == "test-tools":
                await _test_tools(db, config)
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

    asyncio.run(_run())
