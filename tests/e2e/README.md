# E2E tests

End-to-end checks that drive a real browser / the full server surface. They are
**opt-in** — skipped in normal CI because they need a live server (and, for the
console smoke check, the Playwright Chromium build).

## Console-error smoke test (issues #792, #1014)

Walks every main web-panel page and asserts that none of them logs a JS error to
the browser console. A manual one-page pass already found 0 errors (#788); this
turns that into a repeatable check across all pages so regressions get caught.

Since #1014 the check drives the **Playwright Python API** (a declared `[dev]`
dependency) directly — `page.goto` + `page.on("console")` / `page.on("pageerror")`
— instead of shelling out to the external `playwright-cli` binary and parsing its
text output. That removes the undeclared external dependency and the brittle CLI
parsing while keeping the same guards (password redaction, the `/login` bounce
guard, and a loud failure on a dead server).

Pages walked (kept in lockstep with `PANEL_PATHS` in `console_smoke.py` and the
`test_panel_paths_match_issue_list` guard): `/`, `/channels`,
`/channels?view=all`, `/channels/filter/manage`, `/channels/renames`, `/search`,
`/search-queries`, `/analytics`, `/analytics/trends`, `/analytics/channels`,
`/analytics/channels/ratings`, `/dashboard`, `/agent`, `/settings`, `/dialogs`,
`/dialogs/photos`, `/pipelines`, `/jobs`, `/moderation`, `/calendar`, `/images`,
`/scheduler`.

The list was extended in #1013 (part of #1011) to cover newer full-page routes —
notably the session features `/jobs` (#965), `/analytics/channels` (#951) and
`/analytics/channels/ratings` (#968/#999). Only GET full-page routes are walked;
this is a console-only smoke (it catches JS errors on load). Some of the newer
pages are lazyload skeletons (#756), so a clean console here does not yet prove
the deferred fragment loaded — that deeper check is tracked separately.

### Setup (once)

The Playwright packages come from the `[dev]` extra; the browser binaries are a
separate one-time download:

```bash
pip install -e ".[dev]"
playwright install --with-deps chromium
```

### Run it by hand (quickest)

```bash
# 1. Start the panel in one terminal (any password you like):
python -m src.main serve --web-pass secret

# 2. In another terminal, walk every page and check the console:
python -m tests.e2e.console_smoke --base-url http://localhost:8080 --web-pass secret
```

It prints a per-page summary (✓ clean / ✗ N errors) and exits non-zero if any
page logged an error. Drop `--web-pass` if the panel runs without a password.
Add `--headed` to watch the run in a visible browser window (default: headless).

**Auth note:** when a password is set, the script logs in through the real
`/login` form (which sets a session cookie) rather than embedding credentials in
the URL — `BasicAuthMiddleware` answers browser navigations with a `303` to
`/login` instead of a `401` challenge, so a creds-in-URL navigation would
silently land on the login page and report a false "all clean". After each
navigation the script also asserts the page did **not** bounce to `/login`, so a
wrong password or broken auth fails loudly instead of passing green. The password
is typed into the form field (never embedded in a URL) and redacted from any
diagnostic/exception text, so `WEB_PASS` never leaks into a log or a failure.

### Run it via pytest

```bash
RUN_E2E_CONSOLE_SMOKE=1 \
E2E_BASE_URL=http://localhost:8080 \
WEB_PASS=secret \
pytest tests/e2e/test_console_smoke.py -m e2e
```

Without `RUN_E2E_CONSOLE_SMOKE=1` the test is skipped, so it never breaks the
default `pytest` run. Once the gate is open the test only skips for one
intentional case — Playwright / its Chromium build is not installed; every other
failure (dead server, wrong password, hung navigation) fails loudly.

### Layout

- `console_smoke.py` — reusable logic (page list, Playwright browser walk, console
  collection, redaction/auth guards) plus a `__main__` CLI entry point.
- `test_console_smoke.py` — opt-in pytest wrapper (live server + Chromium required).
- `test_console_smoke_parsing.py` — pure-logic unit tests for the helpers
  (redaction, the `/login` bounce guard, the dead-server guard, console-error
  collection); these run in normal CI against fakes — no browser, no server.
