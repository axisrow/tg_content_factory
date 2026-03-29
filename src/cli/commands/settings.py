from __future__ import annotations

import argparse
import asyncio

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

            elif action == "agent":
                updated = []
                backend = getattr(args, "backend", None)
                prompt_template = getattr(args, "prompt_template", None)
                if backend:
                    await db.set_setting("agent_backend", backend)
                    updated.append(f"agent_backend = {backend}")
                if prompt_template:
                    await db.set_setting("agent_default_prompt_template", prompt_template)
                    updated.append(f"agent_default_prompt_template = {prompt_template[:60]}...")
                if updated:
                    for u in updated:
                        print(f"Set {u}")
                else:
                    for key in ("agent_backend", "agent_default_prompt_template"):
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

            elif action == "semantic":
                updated = []
                for attr, setting_key in [
                    ("provider", "semantic_provider"),
                    ("model", "semantic_model"),
                    ("api_key", "semantic_api_key"),
                ]:
                    val = getattr(args, attr, None)
                    if val is not None:
                        await db.set_setting(setting_key, val)
                        display = val[:20] + "..." if attr == "api_key" and len(val) > 20 else val
                        updated.append(f"{setting_key} = {display}")
                if updated:
                    for u in updated:
                        print(f"Set {u}")
                else:
                    for setting_key in ("semantic_provider", "semantic_model", "semantic_api_key"):
                        val = await db.get_setting(setting_key)
                        if setting_key == "semantic_api_key" and val:
                            val = val[:8] + "..."
                        print(f"  {setting_key} = {val or '(not set)'}")
        finally:
            await db.close()

    asyncio.run(_run())
