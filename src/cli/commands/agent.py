from __future__ import annotations

import argparse
import asyncio
import json

from src.cli import runtime


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

                print("Агент: ", end="", flush=True)
                full_text = ""
                async for chunk in mgr.chat_stream(thread_id, args.message):
                    raw = chunk.removeprefix("data: ").strip()
                    try:
                        payload = json.loads(raw)
                    except Exception:
                        continue
                    if "text" in payload:
                        print(payload["text"], end="", flush=True)
                        full_text = payload.get("text", full_text)
                    if payload.get("done"):
                        print()
                        break
                    if "error" in payload:
                        print(f"\nОшибка: {payload['error']}")
                        break

                if full_text:
                    await db.save_agent_message(thread_id, "assistant", full_text)
        finally:
            await db.close()

    asyncio.run(_run())
