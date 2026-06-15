from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timezone

from src.cli import runtime


def run(args: argparse.Namespace) -> None:
    async def _run() -> None:
        _config, db = await runtime.init_db(args.config)
        try:
            action = getattr(args, "settings_action", None) or "get"

            if action == "get":
                key = getattr(args, "key", None)
                if key:
                    value = await db.get_setting(key)
                    print(f"{key} = {value or '(not set)'}")
                else:
                    rows = await db.repos.settings.list_all()
                    if not rows:
                        print("No settings found.")
                        return
                    fmt = "{:<50} {}"
                    print(fmt.format("Key", "Value"))
                    print("-" * 80)
                    for k, v in rows:
                        print(fmt.format(k, v[:80] if v else ""))

            elif action == "set":
                await db.set_setting(args.key, args.value)
                print(f"Set {args.key} = {args.value}")

            elif action == "info":
                stats = await db.get_stats()
                print("System information:")
                for key, value in stats.items():
                    print(f"  {key}: {value}")

            elif action == "server-time":
                # CLI counterpart of the get_server_time agent tool — same UTC fields.
                now = datetime.now(timezone.utc)
                print("Текущее время сервера (UTC):")
                print(f"  ISO8601: {now.isoformat()}")
                print(f"  Unix: {int(now.timestamp())}")
                print(f"  Читаемо: {now.strftime('%Y-%m-%d %H:%M:%S UTC')}")

            elif action == "agent":
                # Write the keys the runtime/web actually read — the old
                # agent_backend / agent_default_prompt_template keys were dead
                # (nothing read them), so CLI config had zero effect (audit #838/7).
                from src.agent.prompt_template import AGENT_PROMPT_TEMPLATE_SETTING

                agent_backend_setting = "agent_backend_override"
                updated = []
                backend = getattr(args, "backend", None)
                prompt_template = getattr(args, "prompt_template", None)
                if backend:
                    await db.set_setting(agent_backend_setting, backend)
                    updated.append(f"{agent_backend_setting} = {backend}")
                if prompt_template:
                    await db.set_setting(AGENT_PROMPT_TEMPLATE_SETTING, prompt_template)
                    updated.append(f"{AGENT_PROMPT_TEMPLATE_SETTING} = {prompt_template[:60]}...")
                if updated:
                    for u in updated:
                        print(f"Set {u}")
                else:
                    for key in (agent_backend_setting, AGENT_PROMPT_TEMPLATE_SETTING):
                        val = await db.get_setting(key)
                        print(f"  {key} = {val or '(not set)'}")

            elif action == "filter-criteria":
                mapping = {
                    "min_uniqueness": "filter_min_uniqueness",
                    "min_sub_ratio": "filter_min_subscriber_ratio",
                    "max_cross_dupe": "filter_max_cross_dupe_pct",
                    "min_cyrillic": "filter_min_cyrillic_pct",
                }
                updated = []
                for attr, setting_key in mapping.items():
                    val = getattr(args, attr, None)
                    if val is not None:
                        await db.set_setting(setting_key, str(val))
                        updated.append(f"{setting_key} = {val}")
                if updated:
                    for u in updated:
                        print(f"Set {u}")
                else:
                    for attr, setting_key in mapping.items():
                        val = await db.get_setting(setting_key)
                        print(f"  {setting_key} = {val or '(not set)'}")

            elif action == "reactions":
                from src.services.telegram_command_dispatcher import (
                    REACTION_MIN_INTERVAL_CEILING_SEC,
                    REACTION_MIN_INTERVAL_FLOOR_SEC,
                    REACTION_MIN_INTERVAL_SETTING,
                )

                min_interval = getattr(args, "min_interval", None)
                if min_interval is not None:
                    clamped = int(
                        max(
                            REACTION_MIN_INTERVAL_FLOOR_SEC,
                            min(REACTION_MIN_INTERVAL_CEILING_SEC, min_interval),
                        )
                    )
                    await db.set_setting(REACTION_MIN_INTERVAL_SETTING, str(clamped))
                    print(f"Set {REACTION_MIN_INTERVAL_SETTING} = {clamped}")
                else:
                    val = await db.get_setting(REACTION_MIN_INTERVAL_SETTING)
                    print(f"  {REACTION_MIN_INTERVAL_SETTING} = {val or '(not set)'}")

            elif action == "semantic":
                # Write the embeddings keys the runtime reads — the old
                # semantic_provider/_model/_api_key keys were dead (audit #838/7).
                from src.services.embedding_service import (
                    EMBEDDINGS_API_KEY_SETTING,
                    EMBEDDINGS_MODEL_SETTING,
                    EMBEDDINGS_PROVIDER_SETTING,
                )

                semantic_keys = [
                    ("provider", EMBEDDINGS_PROVIDER_SETTING),
                    ("model", EMBEDDINGS_MODEL_SETTING),
                    ("api_key", EMBEDDINGS_API_KEY_SETTING),
                ]
                updated = []
                for attr, setting_key in semantic_keys:
                    val = getattr(args, attr, None)
                    if val is not None:
                        await db.set_setting(setting_key, val)
                        display = val[:20] + "..." if attr == "api_key" and len(val) > 20 else val
                        updated.append(f"{setting_key} = {display}")
                if updated:
                    for u in updated:
                        print(f"Set {u}")
                else:
                    for _attr, setting_key in semantic_keys:
                        val = await db.get_setting(setting_key)
                        if setting_key == EMBEDDINGS_API_KEY_SETTING and val:
                            val = val[:8] + "..."
                        print(f"  {setting_key} = {val or '(not set)'}")
        finally:
            await db.close()

    asyncio.run(_run())
