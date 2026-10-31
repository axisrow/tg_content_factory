"""``telegraph`` CLI group — telegra.ph page creation (#1703).

Typer-группа (эпик #959): тонкие ``@command``-обёртки над ``*_impl``.
Аккаунт: ``createPage`` требует ``access_token`` (живой smoke #1703 показал
``ACCESS_TOKEN_INVALID`` на анонимном вызове), поэтому ``login`` один раз
делает ``createAccount`` и кладёт токен в settings
(``telegraph_access_token`` — секрет: enc:v2 через ``set_secret``, в выводе
маскируется как всякая ``*_token`` настройка). В коде токенов нет.
"""

from __future__ import annotations

import json
import sys
from enum import Enum
from pathlib import Path

import typer

from tg_agent.cli import runtime
from tg_agent.cli.commands.common import apply_startup, cli_error, run_async
from tg_agent.services.telegraph_service import (
    ACCESS_TOKEN_SETTING,
    AUTHOR_NAME_MAX,
    AUTHOR_URL_MAX,
    SHORT_NAME_MAX,
    TITLE_MAX,
    create_account,
    create_page,
    markdown_to_telegraph_nodes,
)


class TelegraphFormat(str, Enum):
    """``telegraph create --format`` choices."""

    text = "text"
    json = "json"


def _read_body(file: Path | None) -> str:
    """Read the article body from ``--file`` or stdin (sync — the Typer wrapper
    calls this before ``run_async``, so no blocking IO inside the coroutine)."""
    if file is not None:
        try:
            return Path(file).read_text(encoding="utf-8")
        except OSError as exc:
            cli_error(f"Error: cannot read --file {file}: {exc}")
    return sys.stdin.read()


async def login_impl(
    config_path: str,
    *,
    short_name: str,
    author_name: str | None,
    author_url: str | None,
) -> None:
    """Create a Telegraph account and store its access token in settings."""
    if len(short_name) > SHORT_NAME_MAX:
        cli_error(f"Error: --short-name exceeds {SHORT_NAME_MAX} characters ({len(short_name)})")
    try:
        result = await create_account(short_name, author_name=author_name, author_url=author_url)
    except RuntimeError as exc:
        cli_error(f"Error: {exc}")
    account = result.get("result") or {}
    token = account.get("access_token")
    if not token:
        cli_error(f"Error: Telegraph returned no access_token: {result}")
    _config, db = await runtime.init_db(config_path)
    try:
        await db.repos.settings.set_secret(ACCESS_TOKEN_SETTING, token)
    except RuntimeError as exc:
        cli_error(f"Error: {exc}")
    finally:
        await db.close()
    print(f"Telegraph account created for {account.get('short_name') or short_name!r}.")
    print(f"Access token stored in settings as {ACCESS_TOKEN_SETTING} (masked on display).")


async def create_impl(
    config_path: str,
    *,
    title: str,
    body: str,
    author_name: str | None,
    author_url: str | None,
    output_format: str,
) -> None:
    """Publish a telegra.ph page from a markdown file (or stdin)."""
    if len(title) > TITLE_MAX:
        cli_error(f"Error: --title exceeds {TITLE_MAX} characters ({len(title)})")
    if author_name and len(author_name) > AUTHOR_NAME_MAX:
        cli_error(
            f"Error: --author-name exceeds {AUTHOR_NAME_MAX} characters ({len(author_name)})"
        )
    if author_url and len(author_url) > AUTHOR_URL_MAX:
        cli_error(f"Error: --author-url exceeds {AUTHOR_URL_MAX} characters ({len(author_url)})")
    content = markdown_to_telegraph_nodes(body)
    if not content:
        cli_error("Error: article body is empty (pass --file FILE or pipe markdown to stdin)")

    _config, db = await runtime.init_db(config_path)
    try:
        token = await db.repos.settings.get_secret(ACCESS_TOKEN_SETTING)
    finally:
        await db.close()
    if not token:
        cli_error(
            f"Error: no Telegraph account configured ({ACCESS_TOKEN_SETTING} is not set); "
            "run: telegraph login --short-name NAME"
        )
    try:
        result = await create_page(
            token, title, content, author_name=author_name, author_url=author_url
        )
    except RuntimeError as exc:
        cli_error(f"Error: {exc}")
    if output_format == TelegraphFormat.json.value:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print(result["result"]["url"])


telegraph_app = typer.Typer(no_args_is_help=True, help="Create telegra.ph pages")


@telegraph_app.command("login")
def telegraph_login(
    ctx: typer.Context,
    short_name: str = typer.Option(
        ..., "--short-name", help="Account name (max 32 chars, shown on pages)"
    ),
    author_name: str | None = typer.Option(
        None, "--author-name", help="Author name shown under articles"
    ),
    author_url: str | None = typer.Option(None, "--author-url", help="Author link"),
) -> None:
    """Create a Telegraph account and store its token in settings."""
    apply_startup(ctx)
    run_async(
        login_impl(
            ctx.obj.config,
            short_name=short_name,
            author_name=author_name,
            author_url=author_url,
        )
    )


@telegraph_app.command("create")
def telegraph_create(
    ctx: typer.Context,
    title: str = typer.Option(..., "--title", help="Page title (max 256 chars)"),
    file: Path | None = typer.Option(None, "--file", help="Markdown file; stdin when omitted"),
    author_name: str | None = typer.Option(
        None, "--author-name", help="Author name shown on the page"
    ),
    author_url: str | None = typer.Option(
        None, "--author-url", help="Author link shown on the page"
    ),
    format: TelegraphFormat = typer.Option(
        TelegraphFormat.text, "--format", help="Output format"
    ),
) -> None:
    """Publish a telegra.ph page from markdown; prints the page URL."""
    apply_startup(ctx)
    body = _read_body(file)
    run_async(
        create_impl(
            ctx.obj.config,
            title=title,
            body=body,
            author_name=author_name,
            author_url=author_url,
            output_format=format.value,
        )
    )
