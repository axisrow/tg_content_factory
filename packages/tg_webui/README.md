# tg-webui

The legacy web panel of tg-agent (`tg-agent serve`): a FastAPI + Jinja2/HTMX
dashboard over the same services the CLI uses. Split out of the core in
[#1720](https://github.com/axisrow/tg_content_factory/issues/1720) (wave 3),
following the `packages/tg_shared` layout.

**Status: legacy, frozen.** Under the automation-first contract the web panel
is a deprecated human-facing surface: it keeps working, gets no new features.
The core (`tg_agent`) never imports this package — an import-linter contract
enforces it; only the `serve` entry point loads it lazily. The runtime split
is unchanged: `serve` hosts uvicorn plus an embedded worker, `serve --no-worker`
runs only the web side next to a separate `tg-agent worker`.

## Install

```bash
pip install -e packages/tg_webui
```

Without it, `tg-agent serve` exits with code 1 and an install hint; the worker
daemon (`tg-agent restart` / `tg-agent worker`) and every other command work
without the package.
