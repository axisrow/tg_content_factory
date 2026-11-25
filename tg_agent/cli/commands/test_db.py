"""``test-db`` CLI group — fixture test database assembly (#1770).

``build`` собирает ``<dir database.path>/test/tg_search_test.db`` из
закоммиченных сидов ``tests/fixtures/test_db_seed/*.jsonl`` — схема
продакшн-овая (``schema.py`` + миграции через ``Database.initialize()``).
Живого сбора здесь нет: обновление сидов — явный экспорт владельца
(#1794), а фолбэк при старте собирает базу сам (``tg_agent.test_db``).
"""

from __future__ import annotations

import typer

from tg_agent.cli.commands.common import apply_startup, cli_error, run_async
from tg_agent.config import load_config
from tg_agent.test_db import build_test_db, seed_dir, test_db_path


async def build_impl(config_path: str) -> None:
    """Assemble the fixture DB from the committed seeds; print path + count."""
    config = load_config(config_path)
    out = test_db_path(config.database.path)
    if out is None:
        cli_error("Error: config database.path is ':memory:' — there is nothing to build")
    try:
        messages = await build_test_db(out)
    except FileNotFoundError as exc:
        cli_error(f"Error: {exc}")
    print(f"Test DB ready: {out} ({messages} messages; seeds: {seed_dir()})")


test_db_app = typer.Typer(no_args_is_help=True, help="Fixture test database (#1770)")


@test_db_app.command("build")
def test_db_build(ctx: typer.Context) -> None:
    """Assemble the fixture test DB from the committed seed files."""
    apply_startup(ctx)
    run_async(build_impl(ctx.obj.config))
