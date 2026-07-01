"""Shared async bodies for the ``settings`` CLI group (epic #959, Wave 3 — #1123).

Migrated off the argparse dispatcher onto the Typer ``app`` (see
``src/cli/typer_commands.py``). Each leaf sub-command is a plain ``async def
*_impl`` here — no local ``asyncio.run`` and no ``argparse.Namespace``. A thin
``run(args)`` adapter is kept for the argparse leaf audit and existing tests.

Several sub-commands are get-or-set: passing a value writes it, omitting all
values prints the current value(s).
"""

from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timezone

import typer

from src.cli import runtime
from src.cli.commands.common import (
    apply_startup,
    run_async,
)


async def get_impl(config_path: str, *, key: str | None = None) -> None:
    """Show one setting (``--key``) or all settings."""
    _, db = await runtime.init_db(config_path)
    try:
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
    finally:
        await db.close()


async def set_impl(config_path: str, *, key: str, value: str) -> None:
    """Set a single setting key to a value."""
    _, db = await runtime.init_db(config_path)
    try:
        await db.set_setting(key, value)
        print(f"Set {key} = {value}")
    finally:
        await db.close()


async def info_impl(config_path: str) -> None:
    """Show system diagnostics."""
    _, db = await runtime.init_db(config_path)
    try:
        stats = await db.get_stats()
        print("System information:")
        for key, value in stats.items():
            print(f"  {key}: {value}")
    finally:
        await db.close()


async def server_time_impl(config_path: str) -> None:
    """Show the current server time (UTC) — CLI counterpart of get_server_time."""
    _, db = await runtime.init_db(config_path)
    try:
        now = datetime.now(timezone.utc)
        print("Текущее время сервера (UTC):")
        print(f"  ISO8601: {now.isoformat()}")
        print(f"  Unix: {int(now.timestamp())}")
        print(f"  Читаемо: {now.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    finally:
        await db.close()


async def agent_impl(
    config_path: str, *, backend: str | None = None, prompt_template: str | None = None
) -> None:
    """Configure (or show) the agent backend override and default prompt template."""
    _, db = await runtime.init_db(config_path)
    try:
        # Write the keys the runtime/web actually read — the old
        # agent_backend / agent_default_prompt_template keys were dead
        # (nothing read them), so CLI config had zero effect (audit #838/7).
        from src.agent.prompt_template import AGENT_PROMPT_TEMPLATE_SETTING

        agent_backend_setting = "agent_backend_override"
        updated = []
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
    finally:
        await db.close()


async def filter_criteria_impl(
    config_path: str,
    *,
    min_uniqueness: float | None = None,
    min_sub_ratio: float | None = None,
    max_cross_dupe: float | None = None,
    min_cyrillic: float | None = None,
) -> None:
    """Configure (or show) the channel-filter thresholds."""
    _, db = await runtime.init_db(config_path)
    try:
        mapping = {
            "min_uniqueness": "filter_min_uniqueness",
            "min_sub_ratio": "filter_min_subscriber_ratio",
            "max_cross_dupe": "filter_max_cross_dupe_pct",
            "min_cyrillic": "filter_min_cyrillic_pct",
        }
        values = {
            "min_uniqueness": min_uniqueness,
            "min_sub_ratio": min_sub_ratio,
            "max_cross_dupe": max_cross_dupe,
            "min_cyrillic": min_cyrillic,
        }
        updated = []
        for attr, setting_key in mapping.items():
            val = values[attr]
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
    finally:
        await db.close()


async def reactions_impl(config_path: str, *, min_interval: int | None = None) -> None:
    """Configure (or show) the per-account reaction cadence (clamped 1–300)."""
    _, db = await runtime.init_db(config_path)
    try:
        from src.services.telegram_command_dispatcher import (
            REACTION_MIN_INTERVAL_CEILING_SEC,
            REACTION_MIN_INTERVAL_FLOOR_SEC,
            REACTION_MIN_INTERVAL_SETTING,
        )

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
    finally:
        await db.close()


async def semantic_impl(
    config_path: str,
    *,
    provider: str | None = None,
    model: str | None = None,
    api_key: str | None = None,
) -> None:
    """Configure (or show) the semantic-search embedding provider/model/key."""
    _, db = await runtime.init_db(config_path)
    try:
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
        values = {"provider": provider, "model": model, "api_key": api_key}
        updated = []
        for attr, setting_key in semantic_keys:
            val = values[attr]
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


def run(args: argparse.Namespace) -> None:
    """Thin argparse adapter over the ``*_impl`` bodies (legacy dispatch path).

    The production CLI routes ``settings`` through the Typer ``app`` (#1123); this
    wrapper keeps the argparse leaf audit and command-level tests working. Args are
    read via ``getattr`` defaults so partial test Namespaces stay usable (#1117).
    """
    action = getattr(args, "settings_action", None) or "get"
    if action == "get":
        asyncio.run(get_impl(args.config, key=getattr(args, "key", None)))
    elif action == "set":
        asyncio.run(set_impl(args.config, key=args.key, value=args.value))
    elif action == "info":
        asyncio.run(info_impl(args.config))
    elif action == "server-time":
        asyncio.run(server_time_impl(args.config))
    elif action == "agent":
        asyncio.run(
            agent_impl(
                args.config,
                backend=getattr(args, "backend", None),
                prompt_template=getattr(args, "prompt_template", None),
            )
        )
    elif action == "filter-criteria":
        asyncio.run(
            filter_criteria_impl(
                args.config,
                min_uniqueness=getattr(args, "min_uniqueness", None),
                min_sub_ratio=getattr(args, "min_sub_ratio", None),
                max_cross_dupe=getattr(args, "max_cross_dupe", None),
                min_cyrillic=getattr(args, "min_cyrillic", None),
            )
        )
    elif action == "reactions":
        asyncio.run(reactions_impl(args.config, min_interval=getattr(args, "min_interval", None)))
    elif action == "semantic":
        asyncio.run(
            semantic_impl(
                args.config,
                provider=getattr(args, "provider", None),
                model=getattr(args, "model", None),
                api_key=getattr(args, "api_key", None),
            )
        )


# --------------------------------------------------------------------------- #
# settings → get / set / info / server-time / agent / filter-criteria
#            / reactions / semantic
# --------------------------------------------------------------------------- #

settings_app = typer.Typer(
    invoke_without_command=True, help="System settings management"
)


@settings_app.callback()
def settings_main(ctx: typer.Context) -> None:
    """Bare ``settings`` (no sub-command) runs ``get`` — argparse parity (#1123 review).

    The legacy dispatcher defaulted ``settings_action`` to ``get`` and listed all
    settings; preserve that on the direct Typer surface, not just the argparse
    bridge. With a sub-command this is a no-op and the sub-command runs normally.
    """
    if ctx.invoked_subcommand is None:
        apply_startup(ctx)
        run_async(get_impl(ctx.obj.config, key=None))


@settings_app.command("get")
def settings_get(
    ctx: typer.Context,
    key: str | None = typer.Option(None, "--key", help="Specific setting key (default: show all)"),
) -> None:
    """Show settings."""
    apply_startup(ctx)
    run_async(get_impl(ctx.obj.config, key=key))


@settings_app.command("set")
def settings_set(
    ctx: typer.Context,
    key: str = typer.Argument(..., help="Setting key"),
    value: str = typer.Argument(..., help="Setting value"),
) -> None:
    """Set a setting value."""
    apply_startup(ctx)
    run_async(set_impl(ctx.obj.config, key=key, value=value))


@settings_app.command("info")
def settings_info(ctx: typer.Context) -> None:
    """Show system diagnostics."""
    apply_startup(ctx)
    run_async(info_impl(ctx.obj.config))


@settings_app.command("server-time")
def settings_server_time(ctx: typer.Context) -> None:
    """Show current server time (UTC)."""
    apply_startup(ctx)
    run_async(server_time_impl(ctx.obj.config))


@settings_app.command("agent")
def settings_agent(
    ctx: typer.Context,
    backend: str | None = typer.Option(
        None, "--backend", help="Agent backend override (auto, claude, deepagents, codex, adk)"
    ),
    prompt_template: str | None = typer.Option(
        None, "--prompt-template", help="Default prompt template"
    ),
) -> None:
    """Configure agent backend and defaults."""
    apply_startup(ctx)
    run_async(
        agent_impl(ctx.obj.config, backend=backend, prompt_template=prompt_template)
    )


@settings_app.command("filter-criteria")
def settings_filter_criteria(
    ctx: typer.Context,
    min_uniqueness: float | None = typer.Option(None, "--min-uniqueness"),
    min_sub_ratio: float | None = typer.Option(None, "--min-sub-ratio"),
    max_cross_dupe: float | None = typer.Option(None, "--max-cross-dupe"),
    min_cyrillic: float | None = typer.Option(None, "--min-cyrillic"),
) -> None:
    """Configure filter thresholds."""
    apply_startup(ctx)
    run_async(
        filter_criteria_impl(
            ctx.obj.config,
            min_uniqueness=min_uniqueness,
            min_sub_ratio=min_sub_ratio,
            max_cross_dupe=max_cross_dupe,
            min_cyrillic=min_cyrillic,
        )
    )


@settings_app.command("reactions")
def settings_reactions(
    ctx: typer.Context,
    min_interval: int | None = typer.Option(
        None,
        "--min-interval",
        help="Minimum seconds between reactions per account (clamped to 1–300; default 30)",
    ),
) -> None:
    """Configure reaction sending cadence."""
    apply_startup(ctx)
    run_async(reactions_impl(ctx.obj.config, min_interval=min_interval))


@settings_app.command("semantic")
def settings_semantic(
    ctx: typer.Context,
    provider: str | None = typer.Option(None, "--provider", help="Embedding provider"),
    model: str | None = typer.Option(None, "--model", help="Embedding model"),
    api_key: str | None = typer.Option(None, "--api-key", help="Embedding API key"),
) -> None:
    """Configure semantic search."""
    apply_startup(ctx)
    run_async(
        semantic_impl(
            ctx.obj.config, provider=provider, model=model, api_key=api_key
        )
    )
