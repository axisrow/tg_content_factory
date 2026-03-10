from __future__ import annotations

import argparse
import asyncio
import json
import logging

from src.cli import runtime

logger = logging.getLogger(__name__)

_ESCAPING_CASES = [
    ("xml_tags", "Привет <user>тег</user> и <assistant>тег</assistant>"),
    ("quotes", 'Он сказал "привет" и \'пока\''),
    ("backslashes", "C:\\Users\\test\\file.txt"),
    ("newlines", "строка 1\nстрока 2\n\nстрока 3"),
    ("json_in_text", '{"key": "value", "arr": [1,2,3]}'),
    ("code_block", "```python\nprint('hello')\n```"),
    ("unicode_emoji", "Привет 🎉 мир 🌍 тест ✅"),
    ("special_chars", "a & b < c > d \"e\" 'f'"),
    ("markdown_links", "[ссылка](https://example.com?a=1&b=2)"),
    ("curly_braces", "{{template}} ${variable} %(format)s"),
]


async def _test_escaping(db) -> None:
    from src.agent.manager import AgentManager

    mgr = AgentManager(db)
    if not mgr.available:
        print("ANTHROPIC_API_KEY / CLAUDE_CODE_OAUTH_TOKEN не задан — пропуск.")
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


def run(args: argparse.Namespace) -> None:
    async def _run() -> None:
        config, db = await runtime.init_db(args.config)
        try:
            action = args.agent_action

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

                if args.thread_id:
                    thread_id = args.thread_id
                else:
                    thread_id = await db.create_agent_thread("Новый тред")
                    print(f"(создан тред #{thread_id})")

                await db.save_agent_message(thread_id, "user", args.message)

                mgr = AgentManager(db)
                mgr.initialize()

                model = getattr(args, "model", None)
                print("Агент: ", end="", flush=True)
                full_text = ""
                async for chunk in mgr.chat_stream(thread_id, args.message, model=model):
                    raw = chunk.removeprefix("data: ").strip()
                    try:
                        payload = json.loads(raw)
                    except Exception:
                        continue
                    if "text" in payload:
                        print(payload["text"], end="", flush=True)
                        full_text += payload.get("text", "")
                    if payload.get("done"):
                        print()
                        break
                    if "error" in payload:
                        print(f"\nОшибка: {payload['error']}")
                        break

                if full_text:
                    await db.save_agent_message(thread_id, "assistant", full_text)

            elif action == "thread-rename":
                await db.rename_agent_thread(args.thread_id, args.title[:100])
                print(f"Тред #{args.thread_id} переименован: {args.title[:100]}")

            elif action == "messages":
                msgs = await db.get_agent_messages(args.thread_id)
                if args.limit:
                    msgs = msgs[-args.limit:]
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
                channels = await db.get_channels()
                ch = next(
                    (c for c in channels if c.channel_id == args.channel_id), None
                )
                title = ch.title if ch else str(args.channel_id)

                header = f"[КОНТЕКСТ: {title}"
                if args.topic_id:
                    header += f", тема #{args.topic_id}"
                header += f", {len(messages)} сообщений]"
                lines = [header]
                for m in messages:
                    preview = (m.text or "").replace("\n", " ")[:200]
                    author = m.sender_name or (
                        f"id={m.sender_id}" if m.sender_id else "unknown"
                    )
                    date_str = m.date.strftime("%Y-%m-%d")
                    lines.append(
                        f"- [msg_id={m.message_id}][{date_str}][{author}] {preview}"
                    )
                content = "\n".join(lines)

                await db.save_agent_message(
                    thread_id=args.thread_id, role="user", content=content
                )
                logger.info(
                    "Context loaded for thread %d: %d messages, %d chars",
                    args.thread_id, len(messages), len(content),
                )
                if len(content) > 200_000:
                    logger.warning(
                        "Large context for thread %d: %d chars (>200K)"
                        " — may cause prompt overflow",
                        args.thread_id, len(content),
                    )
                print(content[:500])
                if len(content) > 500:
                    print(f"... ({len(content)} символов всего)")

            elif action == "test-escaping":
                await _test_escaping(db)
        finally:
            await db.close()

    asyncio.run(_run())
