# E2E tests

End-to-end checks that drive a real browser / the full server surface. They are
**opt-in** — skipped in normal CI because they need a live server (and the
Playwright Chromium build).

## Console-error smoke test (issue #792)

Walks every main web-panel page and asserts that none of them logs a JS error to
the browser console. A manual one-page pass already found 0 errors (#788); this
turns that into a repeatable check across all pages so regressions get caught.

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

### Run it by hand (quickest)

```bash
# 1. Start the panel in one terminal (any password you like):
python -m src.main serve --web-pass secret

# 2. In another terminal, walk every page and check the console:
python -m tests.e2e.console_smoke --base-url http://localhost:8080 --web-pass secret
```

It prints a per-page summary (✓ clean / ✗ N errors) and exits non-zero if any
page logged an error. Drop `--web-pass` if the panel runs without a password.

Add `--settle N` (or `E2E_SETTLE=N`) to wait N seconds after each page load
before reading the console — useful when a page logs errors asynchronously a
moment after load (the default reads the console immediately).

**Auth note:** when a password is set, the script logs in through the real
`/login` form (which sets a session cookie) rather than embedding credentials in
the URL — `BasicAuthMiddleware` answers browser navigations with a `303` to
`/login` instead of a `401` challenge, so a creds-in-URL navigation would
silently land on the login page and report a false "all clean". After each
navigation the script also asserts the page did **not** bounce to `/login`, so a
wrong password or broken auth fails loudly instead of passing green.

### Run it via pytest

```bash
RUN_E2E_CONSOLE_SMOKE=1 \
E2E_BASE_URL=http://localhost:8080 \
WEB_PASS=secret \
pytest tests/e2e/test_console_smoke.py -m e2e
```

Without `RUN_E2E_CONSOLE_SMOKE=1` the test is skipped, so it never breaks the
default `pytest` run.

### Layout

- `console_smoke.py` — reusable logic driven by the Playwright Python API (page
  list, `browser_session`/`login`/`check_page`) plus a `__main__` CLI entry point.
  The functional checks below reuse these helpers.
- `test_console_smoke.py` — opt-in pytest wrapper (live server required).
- `test_console_smoke_parsing.py` — pure-logic unit tests for the helpers; these
  run in normal CI (no browser, no server).

## Functional HTMX checks (issue #1015)

The console smoke only proves a page logs no JS errors. For lazyload pages (#756:
an empty skeleton + `hx-trigger="load"`), a *failed* fragment request still renders
a clean skeleton, so the page passes "green" while its content never loaded. The
functional checks assert the **behaviour**, not just the absence of errors:

1. **Lazyload filled** — after load, the fragment container replaced its skeleton
   (the `loading()` hourglass icon is gone) AND the fragment GET returned 200. Pages:
   `/jobs`, `/analytics/channels`, `/moderation`, `/dashboard`.
2. **`/rate` swap (#999)** — clicking "Запустить судью" on `/analytics/channels/ratings`
   HTMX-swaps the verdict fragment into `#rate-result`. Safe without secrets: with no
   LLM provider configured it returns the error fragment (`.alert-warning`) — a valid
   swap that never calls a real LLM.
3. **Collect OOB swap** — clicking a channel's desktop collect button OOB-swaps BOTH
   the desktop and mobile buttons into the disabled "queued" state. Checking both is
   the point: a single-target check would miss an OOB desync.

Mechanism: driven by the Playwright Python API (the same engine `console_smoke`
migrated to in #1014). The fragment's HTTP status is read directly off the network
event via `page.expect_response`, and DOM conditions are awaited with
`page.wait_for_function` — direct assertions, no log parsing.

Empty-DB note: an empty DB legitimately renders an empty-state body (e.g. `/jobs` →
"Нет фоновых задач."), which is a *valid filled container*, not a stuck skeleton — so
"filled" is keyed on the skeleton icon disappearing, not on a specific table. Two
pages need seeded state to fully exercise: `/dashboard` 303-redirects to `/settings`
without an account (reported as a failure), and the OOB collect check needs ≥1 channel
(otherwise the test skips with a clear reason). A *broken* `/channels` lazyload (fragment
≠200 or stuck skeleton) is reported as a failure, never a skip — so it can't hide.

### Run it

These are **local-only** — like the console smoke, they are NOT wired into CI. Run
against a live server, reusing the console-smoke gate env:

```bash
python -m src.main serve --web-pass secret          # in another terminal

RUN_E2E_CONSOLE_SMOKE=1 E2E_BASE_URL=http://localhost:8080 WEB_PASS=secret \
    pytest tests/e2e/test_htmx_functional.py -m e2e
```

For the full picture (incl. OOB collect + dashboard), seed a channel and an account
into the configured DB first, e.g. via `python -m src.main channel add ...` and a
connected account.

- `htmx_functional.py` — the functional checks (specs, waits, OOB/swap logic).
- `test_htmx_functional.py` — opt-in pytest wrapper (live server required).
- `test_htmx_functional_parsing.py` — pure-logic unit tests for the check
  orchestration (result models, redirect/empty-DB guards, summary); run in normal CI.
