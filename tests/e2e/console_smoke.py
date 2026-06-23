"""Console-error smoke check for the web panel, driven by ``pytest-playwright``.

Why a standalone module (issue #792): a manual ``playwright-cli console`` pass on
one page already found 0 errors (#788), but regressions are inevitable. This
walks **every** main panel page in a real browser and asserts that none of them
logs a JS error to the console. The check is opt-in (a live server must be
running) and intentionally lives next to ``tests/e2e/test_collection_flow.py``.

Design notes (issue #1014 — migrated off the external ``playwright-cli`` binary):

- We drive the **Playwright Python API** (a declared ``[dev]`` dependency; the
  browser is installed locally with ``playwright install --with-deps chromium``)
  instead of shelling out to the ``playwright-cli`` binary. This removes the
  "undeclared external dependency" and the brittle text-parsing of the CLI's
  stdout: console errors are read directly off ``page.on("console")`` /
  ``page.on("pageerror")`` and the landed path off ``page.url`` — no regex over
  CLI output. This check is run **locally** (gate ``RUN_E2E_CONSOLE_SMOKE=1``
  against a server started with ``python -m src.main serve``); it is not wired
  into CI.
- Console capture: Playwright does NOT buffer console events across navigations
  the way the CLI did, so we attach a listener once per page and ``goto`` with
  that listener live, collecting only that page's messages. ``pageerror`` catches
  uncaught JS exceptions (which Chromium also logs to the console, but the
  dedicated event is the robust source).
- Auth: ``BasicAuthMiddleware`` only sends a ``401`` Basic challenge for
  non-HTML requests; a browser navigation (``Accept: text/html``) instead gets a
  ``303`` redirect to ``/login``, and Chromium never replays URL-embedded
  credentials on a ``303``. So creds-in-URL would silently land every page on the
  public ``/login`` form and report "all clean" without testing anything. We
  therefore log in through the real form once (POST ``/login`` → session cookie),
  which authenticates every later navigation, and after each ``goto`` we assert
  the page did NOT bounce back to ``/login`` so a broken auth fails loudly rather
  than passing green. With no password configured the panel is open and we skip
  the login step. The password is typed into the form field (``page.fill``) and
  never embedded in a navigation URL; Playwright never echoes the value back, but
  we still redact it from any diagnostic/exception text as a belt-and-braces
  guard so ``WEB_PASS`` can never leak into a log or a failure message.
- Dead server: ``page.goto`` raises a Playwright error (``ERR_CONNECTION_REFUSED``,
  a timeout, …) which we wrap in :class:`ConsoleSmokeError`. A mute/misconfigured
  server therefore fails loudly instead of walking every page and reporting "0
  errors" (the silent false-negative the gated test must never produce) — the
  Python-API equivalent of the old "exit 0 + ``### Error``" guard.
"""

from __future__ import annotations

import argparse
import os
import sys
from contextlib import contextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Generator
from urllib.parse import urlsplit

if TYPE_CHECKING:
    from playwright.sync_api import Browser, ConsoleMessage, Page
    from playwright.sync_api import Error as PlaywrightError

# The main panel pages to walk. Originally mirrored the list in issue #792;
# extended in #1013 to cover newer full-page routes (incl. the session features
# /jobs #965, /analytics/channels #951, /analytics/channels/ratings #968/#999).
# Only GET full-page routes belong here — no POST endpoints, no /fragments/*
# routes, and no {id}-parametric pages. Some routes are declared as @get("/")
# under a non-empty router prefix, so their canonical URL has a trailing slash
# (e.g. /moderation/, /calendar/); we list them without the slash to match the
# existing convention — a browser navigation follows the 307 redirect and the
# bounce-to-/login guard in check_page still holds. Keep this in lockstep with
# test_panel_paths_match_issue_list (tests/e2e/test_console_smoke_parsing.py)
# and the "Pages walked" list in tests/e2e/README.md.
PANEL_PATHS: tuple[str, ...] = (
    "/",
    "/channels",
    "/channels?view=all",
    "/channels/filter/manage",
    "/channels/renames",
    "/search",
    "/search-queries",
    "/analytics",
    "/analytics/trends",
    "/analytics/channels",
    "/analytics/channels/ratings",
    "/dashboard",
    "/agent",
    "/settings",
    "/dialogs",
    "/dialogs/photos",
    "/pipelines",
    "/jobs",
    "/moderation",
    "/calendar",
    "/images",
    "/scheduler",
)

LOGIN_PATH = "/login"
# CSS selector for the panel's login form field (src/web/templates/web_login.html).
_PASSWORD_FIELD = "#password"
_REDACTED = "***"
# Per-action navigation/console timeout (ms). A hung navigation against a wedged
# server must fail the gated run loudly rather than block forever — pytest's own
# 120s deadlock guard is the outer backstop.
_DEFAULT_TIMEOUT_MS = 30_000


class ConsoleSmokeError(RuntimeError):
    """Raised when a Playwright operation fails (dead server, bad navigation, …).

    This is the Python-API equivalent of the old CLI's "exit 0 + ``### Error``"
    guard: it turns an operational failure (connection refused, navigation
    timeout, missing login field, …) into a loud error so a dead/misconfigured
    server can never walk every page and report "0 errors" — a silent
    false-negative the gated test must never produce.
    """


class RedirectedToLoginError(RuntimeError):
    """Raised when a page bounced to ``/login`` — i.e. the session is not authenticated.

    This turns a silent false-negative (walking the public login form 12× and
    reporting "all clean") into a loud failure.
    """


@dataclass(frozen=True)
class PageResult:
    """Outcome of visiting a single page."""

    path: str
    error_count: int
    errors: list[str]

    @property
    def clean(self) -> bool:
        return self.error_count == 0


def _redact(text: str, secrets: tuple[str, ...]) -> str:
    """Replace each non-empty secret in ``text`` with ``***``.

    The Playwright Python API never puts the password on a process argv (unlike
    the old ``playwright-cli``), but we still scrub it from any diagnostic or
    exception text as a belt-and-braces guard so ``WEB_PASS`` can never leak into
    a log or a failure message via a caller that interpolates the error.
    """
    for secret in secrets:
        if secret:
            text = text.replace(secret, _REDACTED)
    return text


def _path_of(url: str) -> str:
    """Return just the ``location.pathname`` of a full URL (no scheme/host/query)."""
    return urlsplit(url).path


def _is_login_path(path: str) -> bool:
    """True if ``path`` is the login page (with or without a trailing slash)."""
    return path.rstrip("/") == LOGIN_PATH


def _console_error_text(msg: "ConsoleMessage") -> str:
    """Render a console error message in the old ``[ERROR] ...`` reporting shape.

    The location (URL:line) mirrors the ``@ file:line`` suffix the CLI printed,
    so ``format_summary`` output stays familiar across the migration.
    """
    loc = msg.location or {}
    where = loc.get("url", "") if isinstance(loc, dict) else ""
    line = loc.get("lineNumber", 0) if isinstance(loc, dict) else 0
    suffix = f" @ {where}:{line}" if where else ""
    return f"[ERROR] {msg.text}{suffix}"


def _attach_error_listeners(page: "Page", sink: list[str]) -> None:
    """Collect console *errors* and uncaught page errors into ``sink``.

    Mirrors the old ``console error`` filter: only ``console.error`` calls and
    uncaught JS exceptions count — warnings/info/log are ignored.
    """

    def _on_console(msg: "ConsoleMessage") -> None:
        if msg.type == "error":
            sink.append(_console_error_text(msg))

    def _on_pageerror(exc: "PlaywrightError") -> None:
        # Uncaught JS exception (e.g. a thrown Error). ``exc.message`` is the
        # error text; Chromium also surfaces these on the console, but the
        # dedicated event is the robust source.
        sink.append(f"[ERROR] {exc.message}")

    page.on("console", _on_console)
    page.on("pageerror", _on_pageerror)


@contextmanager
def browser_session(*, headless: bool = True) -> Generator["Browser", None, None]:
    """Launch a headless Chromium and always close it, even on error.

    A failed run must not leave a zombie browser process behind, mirroring the
    old ``finally: close`` contract. Import is local so merely importing this
    module (e.g. for the pure-logic unit tests) does not require Playwright. A
    missing Chromium build (``playwright install`` not run) raises a Playwright
    error at launch, which we wrap in :class:`ConsoleSmokeError` so the caller
    sees the same operational-failure type as a navigation error.
    """
    from playwright.sync_api import Error as PlaywrightError
    from playwright.sync_api import sync_playwright

    with sync_playwright() as pw:
        try:
            browser = pw.chromium.launch(headless=headless)
        except PlaywrightError as exc:
            raise ConsoleSmokeError(f"failed to launch Chromium: {exc.message}") from None
        try:
            yield browser
        finally:
            browser.close()


def _goto(page: "Page", url: str, *, secrets: tuple[str, ...] = ()) -> None:
    """Navigate, converting any Playwright error into a redacted ConsoleSmokeError.

    A dead/misconfigured server raises here (``ERR_CONNECTION_REFUSED``, a
    navigation timeout, …) rather than silently walking on — the Python-API
    equivalent of the old "exit 0 + ``### Error``" guard.
    """
    from playwright.sync_api import Error as PlaywrightError

    try:
        page.goto(url, wait_until="load")
    except PlaywrightError as exc:
        raise ConsoleSmokeError(_redact(f"navigation to {url} failed: {exc.message}", secrets)) from None


def login(page: "Page", base_url: str, password: str) -> None:
    """Authenticate the browser session via the real ``/login`` form.

    Navigates to ``/login``, fills the password field and submits; the panel
    responds with a session cookie that authenticates every later navigation.
    Raises :class:`RedirectedToLoginError` if the form did not authenticate
    (still on ``/login`` afterwards). The password is typed into the field and
    never embedded in a URL; it is redacted from any error text as a guard.
    """
    from playwright.sync_api import Error as PlaywrightError

    base = base_url.rstrip("/")
    secrets = (password,)
    _goto(page, f"{base}{LOGIN_PATH}", secrets=secrets)
    try:
        page.fill(_PASSWORD_FIELD, password)
        # Submitting reloads to the target page; wait for that navigation so the
        # session cookie is set before we read the landed path.
        with page.expect_navigation(wait_until="load"):
            page.press(_PASSWORD_FIELD, "Enter")
    except PlaywrightError as exc:
        raise ConsoleSmokeError(_redact(f"login form interaction failed: {exc.message}", secrets)) from None
    if _is_login_path(_path_of(page.url)):
        raise RedirectedToLoginError("login failed: still on /login after submitting the password (wrong WEB_PASS?)")


def check_page(page: "Page", base_url: str, path: str) -> PageResult:
    """Navigate to one page and read back its console errors.

    Raises :class:`RedirectedToLoginError` if the navigation bounced to ``/login``
    (an unauthenticated session) — otherwise a broken auth would silently report
    the clean login form as a clean panel page.
    """
    base = base_url.rstrip("/")
    errors: list[str] = []
    _attach_error_listeners(page, errors)
    _goto(page, f"{base}{path}")
    landed = _path_of(page.url)
    if _is_login_path(landed) and not _is_login_path(path):
        raise RedirectedToLoginError(f"navigation to {path!r} bounced to {landed!r}: session is not authenticated")
    return PageResult(path=path, error_count=len(errors), errors=list(errors))


def run_smoke(
    base_url: str,
    password: str | None = None,
    paths: tuple[str, ...] = PANEL_PATHS,
    *,
    headless: bool = True,
    timeout_ms: int = _DEFAULT_TIMEOUT_MS,
) -> list[PageResult]:
    """Open a browser, (log in if needed,) walk every panel page, return results.

    The browser is always closed, even on error (see :func:`browser_session`),
    so a failed run does not leave a zombie process behind. A fresh page is used
    per panel path so each page's console listener only sees its own messages.
    """
    base = base_url.rstrip("/")
    with browser_session(headless=headless) as browser:
        context = browser.new_context()
        context.set_default_timeout(timeout_ms)
        context.set_default_navigation_timeout(timeout_ms)
        try:
            if password:
                login(context.new_page(), base, password)
            return [check_page(context.new_page(), base, path) for path in paths]
        finally:
            context.close()


def format_summary(results: list[PageResult]) -> str:
    """Render a human-readable summary: which pages are clean, which have errors."""
    lines: list[str] = []
    clean = [r for r in results if r.clean]
    dirty = [r for r in results if not r.clean]
    width = max((len(r.path) for r in results), default=0)
    for r in results:
        mark = "✓" if r.clean else "✗"
        suffix = "clean" if r.clean else f"{r.error_count} error(s)"
        lines.append(f"  {mark} {r.path.ljust(width)}  {suffix}")
        for err in r.errors:
            lines.append(f"      {err}")
    lines.append("")
    lines.append(f"SUMMARY: {len(clean)}/{len(results)} clean, {len(dirty)} with errors")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    """Run the smoke check from the command line against a live server.

    Usage::

        python -m src.main serve --web-pass PASS   # in another terminal
        python -m tests.e2e.console_smoke --base-url http://localhost:8080 --web-pass PASS

    The password also reads from ``WEB_PASS`` if ``--web-pass`` is omitted.
    Exit code is 0 when every page is clean, 1 otherwise.
    """
    parser = argparse.ArgumentParser(description="Walk every web-panel page and check for JS console errors.")
    parser.add_argument(
        "--base-url",
        default=os.environ.get("E2E_BASE_URL", "http://localhost:8080"),
        help="Base URL of the running web panel (default: %(default)s).",
    )
    parser.add_argument(
        "--web-pass",
        default=os.environ.get("WEB_PASS"),
        help="Panel password (defaults to the WEB_PASS env var; omit if the panel is open).",
    )
    parser.add_argument(
        "--headed",
        action="store_true",
        help="Run with a visible browser window (default: headless).",
    )
    args = parser.parse_args(argv)
    # Basic sanity on the base URL so a typo fails clearly rather than mid-walk.
    if not urlsplit(args.base_url).scheme:
        parser.error(f"--base-url must include a scheme, got {args.base_url!r}")

    results = run_smoke(args.base_url, args.web_pass, headless=not args.headed)
    print(format_summary(results))
    return 0 if all(r.clean for r in results) else 1


if __name__ == "__main__":
    sys.exit(main())
