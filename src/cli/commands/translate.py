from __future__ import annotations

import argparse
import asyncio

from src.cli import runtime


def run(args: argparse.Namespace) -> None:
    async def _run() -> None:
        _config, db = await runtime.init_db(args.config)
        try:
            action = getattr(args, "translate_action", None) or "stats"

            if action == "stats":
                stats = await db.repos.messages.get_language_stats()
                if not stats:
                    print("No language data. Run: python -m src.main translate detect")
                    return
                fmt = "{:<10} {:>10}"
                print(fmt.format("Language", "Messages"))
                print("-" * 22)
                total = 0
                for lang, count in stats:
                    print(fmt.format(lang, count))
                    total += count
                print("-" * 22)
                print(fmt.format("Total", total))

            elif action == "detect":
                batch_size = getattr(args, "batch_size", 5000)
                total_updated = 0
                while True:
                    updated = await db.repos.messages.backfill_language_detection(batch_size=batch_size)
                    total_updated += updated
                    if updated < batch_size:
                        break
                    print(f"  ... detected {total_updated} so far")
                print(f"Language detection complete: {total_updated} messages updated.")

            elif action == "run":
                from src.services.provider_service import AgentProviderService
                from src.services.translation_service import TranslationService

                target = getattr(args, "target", "en")
                source_filter_raw = getattr(args, "source_filter", "")
                limit = getattr(args, "limit", 100)
                source_filter = (
                    [s.strip() for s in source_filter_raw.split(",") if s.strip()]
                    if source_filter_raw else None
                )

                provider_name = await db.get_setting("translation_provider")
                model = await db.get_setting("translation_model")

                provider_service = AgentProviderService(db)
                svc = TranslationService(db, provider_service=provider_service)

                msgs = await db.repos.messages.get_untranslated_messages(
                    target=target, source_langs=source_filter, limit=limit
                )
                if not msgs:
                    print("No messages to translate.")
                    return

                print(f"Translating {len(msgs)} messages to {target}...")
                results = await svc.translate_batch(msgs, target, provider_name=provider_name, model=model)
                for msg_id, translated in results:
                    await db.repos.messages.update_translation(msg_id, target, translated)
                print(f"Translated {len(results)}/{len(msgs)} messages.")

        finally:
            await db.close()

    asyncio.run(_run())
