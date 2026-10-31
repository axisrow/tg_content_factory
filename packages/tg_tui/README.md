# tg-tui

The interactive agent-chat TUI of tg-agent (`tg-agent agent chat` without a
prompt), built on [Textual](https://textual.textualize.io/). Split out of the
core in [#1720](https://github.com/axisrow/tg_content_factory/issues/1720)
(wave 2), following the `packages/tg_shared` layout.

**Status: legacy, frozen.** Under the automation-first contract the TUI is a
deprecated human-facing surface: it keeps working, gets no new features. The
core (`tg_agent`) never imports this package — an import-linter contract
enforces it; only the `agent chat` entry point loads it lazily.

## Install

```bash
pip install -e packages/tg_tui
```

Without it, `tg-agent agent chat` (no prompt) exits with code 1 and an install
hint; one-shot `tg-agent agent chat "<prompt>"` and every other command work
without the package.
