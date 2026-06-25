"""Shared async bodies for the ``translate`` CLI group (epic #959, Wave 2 — #1122).

Migrated off the argparse dispatcher onto the Typer ``app`` (see
``src/cli/typer_commands.py``). Each leaf sub-command is a plain ``async def
*_impl`` here — no local ``asyncio.run`` and no ``argparse.Namespace``.
"""

from __future__ import annotations

import argparse
import asyncio

from src.cli import runtime


async def stats_impl(config_path: str) -> None:
    """Show language distribution across collected messages."""
    _, db = await runtime.init_db(config_path)
    try:
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
    finally:
        await db.close()


async def detect_impl(config_path: str, *, batch_size: int = 5000) -> None:
    """Backfill language detection over untagged messages."""
    _, db = await runtime.init_db(config_path)
    try:
        total_updated = 0
        while True:
            updated = await db.repos.messages.backfill_language_detection(batch_size=batch_size)
            total_updated += updated
            if updated < batch_size:
                break
            print(f"  ... detected {total_updated} so far")
        print(f"Language detection complete: {total_updated} messages updated.")
    finally:
        await db.close()


async def run_impl(
    config_path: str,
    *,
    target: str = "en",
    source_filter: str = "",
    limit: int = 100,
) -> None:
    """Run a translation batch toward *target* for untranslated messages."""
    from src.services.provider_service import build_provider_service
    from src.services.translation_service import TranslationService

    _config, db = await runtime.init_db(config_path)
    try:
        source_langs = (
            [s.strip() for s in source_filter.split(",") if s.strip()]
            if source_filter else None
        )

        provider_name = await db.get_setting("translation_provider")
        model = await db.get_setting("translation_model")

        provider_service = await build_provider_service(db, _config)
        svc = TranslationService(db, provider_service=provider_service)

        msgs = await db.repos.messages.get_untranslated_messages(
            target=target, source_langs=source_langs, limit=limit
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


async def message_impl(config_path: str, *, message_id: int, target: str = "en") -> None:
    """Translate a single message by DB id toward *target*."""
    from src.services.provider_service import build_provider_service
    from src.services.translation_service import TranslationService

    _config, db = await runtime.init_db(config_path)
    try:
        msg = await db.repos.messages.get_by_id(message_id)
        if msg is None:
            print(f"Message id={message_id} not found.")
            return
        text = msg.text or ""
        if not text.strip():
            print("Message has no text to translate.")
            return

        provider_name = await db.get_setting("translation_provider")
        model = await db.get_setting("translation_model")
        provider_service = await build_provider_service(db, _config)
        svc = TranslationService(db, provider_service=provider_service)

        results = await svc.translate_batch([msg], target, provider_name=provider_name, model=model)
        if results:
            _, translated = results[0]
            await db.repos.messages.update_translation(message_id, target, translated)
            print(f"Original:\n  {text[:500]}\n")
            print(f"Translated ({target}):\n  {translated[:500]}")
        else:
            print("Translation failed.")
    finally:
        await db.close()


def run(args: argparse.Namespace) -> None:
    """Thin argparse adapter over the ``*_impl`` bodies (legacy dispatch path).

    The production CLI routes ``translate`` through the Typer ``app`` (#1122);
    this wrapper keeps the argparse leaf audit and command-level tests working.
    """
    action = getattr(args, "translate_action", None) or "stats"
    if action == "stats":
        asyncio.run(stats_impl(args.config))
    elif action == "detect":
        asyncio.run(detect_impl(args.config, batch_size=getattr(args, "batch_size", 5000)))
    elif action == "run":
        asyncio.run(
            run_impl(
                args.config,
                target=getattr(args, "target", "en"),
                source_filter=getattr(args, "source_filter", ""),
                limit=getattr(args, "limit", 100),
            )
        )
    elif action == "message":
        asyncio.run(
            message_impl(args.config, message_id=args.message_id, target=getattr(args, "target", "en"))
        )
