# E2E tests

End-to-end checks that drive a real browser / the full server surface. They are
**opt-in** — skipped in normal CI because they need a live server (and, for the
console smoke check, the `playwright-cli` binary).

## Console-error smoke test (issue #792)

Walks every main web-panel page and asserts that none of them logs a JS error to
the browser console. A manual one-page pass already found 0 errors (#788); this
turns that into a repeatable check across all pages so regressions get caught.

Pages walked: `/`, `/channels`, `/channels?view=all`, `/channels/filter/manage`,
`/search`, `/analytics`, `/analytics/trends`, `/dashboard`, `/agent`,
`/settings`, `/dialogs`, `/pipelines`.

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

- `console_smoke.py` — reusable logic (page list, `playwright-cli` calls, parsing)
  plus a `__main__` CLI entry point.
- `test_console_smoke.py` — opt-in pytest wrapper (live server required).
- `test_console_smoke_parsing.py` — pure-logic unit tests for the helpers; these
  run in normal CI (no browser, no server).
