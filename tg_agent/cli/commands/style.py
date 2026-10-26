"""`style` CLI group: local-only per-channel posting-style specs (#1687).

A style spec describes a target channel's posting voice — post skeleton,
nicknames, emoji discipline, digest rules. It is personal operator content and
must never enter the repository (#1584: an accidentally committed design.md had
to be force-pushed out). Specs therefore live in the machine-wide designs
directory — default ``~/.tg-agent/designs/``, ``$TG_AGENT_GLOBAL_DIR`` overrides
— outside any worktree, so ``git status`` stays clean by construction and there
is nothing to gitignore.

* ``style init`` bootstraps ``designs/<channel_key>.md`` from a placeholder
  template (no real voice content) for the agent to fill in with the owner.
* ``style list`` shows the existing specs.
* ``style show`` prints a spec — agents read the channel voice from here
  instead of hardcoding shell paths or inventing formats.

An existing spec is never overwritten without ``--force``: the file IS the
owner's voice, and a silent rewrite would destroy it. The key accepts a raw
username (``@prog_ai``), a t.me link, or a numeric channel id — anything else
(paths, traversal) is refused.
"""

from __future__ import annotations

import re
from pathlib import Path

import typer

from tg_agent.cli.commands.common import apply_startup, cli_error
from tg_agent.config import designs_root

_STYLE_TEMPLATE = """\
# Style spec: {channel_key}

Local-only operator content — NEVER commit this file (#1584). It lives in
`~/.tg-agent/designs/` (or `$TG_AGENT_GLOBAL_DIR/designs/`), outside the
repository; `git status` must stay clean of it.

Fill the sections below together with the owner, in the owner's words.
Delete the sections the channel does not need. Facts and placeholders only
until then — do not invent a voice.

## Voice

<!-- tone, register, what the channel sounds like; what it never does -->

## Post skeleton

<!-- the fixed shape of a post: heading / body / verdict / links order -->

## Nicknames

<!-- established nicknames and spellings; leave empty for none -->

## Emoji policy

<!-- e.g. none at all, or at most one per post; which ones are off-limits -->

## Digest rules

<!-- digest-specific formatting: item template, link handling, length caps;
     leave empty if the channel never carries digests -->
"""

_KEY_RE = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9_.-]*$")


def _spec_path(channel_key: str) -> Path:
    """Sanitize a raw channel identifier into a safe spec file path."""
    key = channel_key.strip()
    key = re.sub(r"^https?://t\.me/", "", key, flags=re.IGNORECASE)
    key = key.lstrip("@").rstrip("/")
    if not _KEY_RE.fullmatch(key):
        cli_error(
            f"ERROR: invalid channel key {channel_key!r}: use a username, a t.me "
            "link or a numeric channel id (no path separators)."
        )
    return designs_root() / f"{key}.md"


def init_impl(channel_key: str, force: bool) -> None:
    """Bootstrap the channel's style spec from the placeholder template."""
    path = _spec_path(channel_key)
    if path.exists() and not force:
        cli_error(
            f"ERROR: {path} already exists; refusing to overwrite the owner's "
            "voice. Pass --force to discard it and start from the template."
        )
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    path.write_text(_STYLE_TEMPLATE.format(channel_key=path.stem), encoding="utf-8")
    print(f"Style spec created: {path}")
    print("Fill it in together with the owner; it stays outside the repository.")


def list_impl() -> None:
    """List existing style specs."""
    root = designs_root()
    specs = sorted(root.glob("*.md")) if root.is_dir() else []
    if not specs:
        print(f"No style specs yet; bootstrap one with: style init --channel <id>  (dir: {root})")
        return
    print(f"Style specs in {root}:")
    for spec in specs:
        print(f"  {spec.stem}")


def show_impl(channel_key: str) -> None:
    """Print the channel's style spec (the agent's source of channel voice)."""
    path = _spec_path(channel_key)
    if not path.exists():
        cli_error(
            f"ERROR: no style spec for {path.stem}: bootstrap it with "
            "`style init --channel <id>`, then fill it in with the owner."
        )
    text = path.read_text(encoding="utf-8")
    if not text.endswith("\n"):
        text += "\n"
    print(f"# style spec: {path}")
    print(text, end="")


# --------------------------------------------------------------------------- #
# style → init / list / show  (local-only per-channel style specs, #1687)
# --------------------------------------------------------------------------- #

style_app = typer.Typer(no_args_is_help=True, help="Local-only per-channel style specs (never committed)")


@style_app.command("init")
def style_init(
    ctx: typer.Context,
    channel_key: str = typer.Option(..., "--channel", help="Channel username, t.me link or numeric id"),
    force: bool = typer.Option(False, "--force", help="Overwrite an existing spec with the template"),
) -> None:
    """Create the channel's style spec template outside the repository."""
    apply_startup(ctx)
    init_impl(channel_key, force)


@style_app.command("list")
def style_list(ctx: typer.Context) -> None:
    """List existing style specs."""
    apply_startup(ctx)
    list_impl()


@style_app.command("show")
def style_show(
    ctx: typer.Context,
    channel_key: str = typer.Option(..., "--channel", help="Channel username, t.me link or numeric id"),
) -> None:
    """Print the channel's style spec."""
    apply_startup(ctx)
    show_impl(channel_key)
