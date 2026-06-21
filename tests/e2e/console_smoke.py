"""Console-error smoke check for the web panel, driven by ``playwright-cli``.

Why a standalone module (issue #792): a manual ``playwright-cli console`` pass on
one page already found 0 errors (#788), but regressions are inevitable. This
walks **every** main panel page in a real browser and asserts that none of them
logs a JS error to the console. The check is opt-in (a live server must be
running) and intentionally lives next to ``tests/e2e/test_collection_flow.py``.

Design notes:

- We shell out to the already-installed ``playwright-cli`` binary rather than
  pulling in ``pytest-playwright`` + uvicorn-in-a-thread fixtures. The CLI keeps
  a persistent browser session between invocations, so the flow is just
  ``open`` → (login once) → ``goto`` (per page) → ``console error`` → ``close``.
- ``playwright-cli`` clears its console buffer on navigation, so after a
  ``goto`` the buffer holds only that page's messages — exactly what we want.
- Auth: ``BasicAuthMiddleware`` only sends a ``401`` Basic challenge for
  non-HTML requests; a browser navigation (``Accept: text/html``) instead gets a
  ``303`` redirect to ``/login``, and Chromium never replays URL-embedded
  credentials on a ``303``. So creds-in-URL would silently land every page on the
  public ``/login`` form and report "all clean" without testing anything. We
  therefore log in through the real form once (POST ``/login`` → session cookie),
  which authenticates every later navigation, and after each ``goto`` we assert
  the page did NOT bounce back to ``/login`` so a broken auth fails loudly rather
  than passing green. With no password configured the panel is open and we skip
  the login step. The password is typed into the form field (never embedded in a
  navigation URL); ``playwright-cli`` diagnostics are redacted so it never leaks
  into log/exception text (it can still be visible in local process args while
  the ``fill`` runs — an inherent limit of any CLI that takes the value as argv).
"""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from urllib.parse import urlsplit

# The main panel pages to walk, mirroring the list in issue #792. Paths are
# relative to the base URL; the leading "/" page is the dashboard.
PANEL_PATHS: tuple[str, ...] = (
    "/",
    "/channels",
    "/channels?view=all",
    "/channels/filter/manage",
    "/search",
    "/analytics",
    "/analytics/trends",
    "/dashboard",
    "/agent",
    "/settings",
    "/dialogs",
    "/pipelines",
)

# "Total messages: 4 (Errors: 2, Warnings: 1)" — the summary line that
# ``playwright-cli console`` always prints. We read the error count from it
# rather than counting "[ERROR]" lines, so the parse is robust even when the
# CLI truncates or reformats the message bodies.
_ERROR_COUNT_RE = re.compile(r"Errors:\s*(\d+)", re.IGNORECASE)

PLAYWRIGHT_CLI = "playwright-cli"
LOGIN_PATH = "/login"
# CSS selector for the panel's login form field (src/web/templates/web_login.html).
_PASSWORD_FIELD = "#password"
_REDACTED = "***"
# ``playwright-cli`` reports operational failures (connection refused, element not
# found, failed eval, …) by printing a ``### Error`` block to stdout while still
# exiting 0 — so a non-zero exit code alone misses them. We treat this marker as a
# failure too, otherwise a dead/misconfigured server would walk every page and
# report "0 errors" (a silent false-negative the gated test must never produce).
_CLI_ERROR_MARKER = "### Error"


class PlaywrightCliError(RuntimeError):
    """Raised when a ``playwright-cli`` invocation fails.

    Covers both a non-zero exit and the exit-0 ``### Error`` stdout block the CLI
    emits for operational failures (connection refused, missing element, …).
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
    """Replace each non-empty secret in ``text`` with ``***``."""
    for secret in secrets:
        if secret:
            text = text.replace(secret, _REDACTED)
    return text


def _run_cli(*args: str, session: str | None = None, secrets: tuple[str, ...] = ()) -> str:
    """Run ``playwright-cli`` and return its stdout; raise on failure.

    Failure is either a non-zero exit OR an exit-0 ``### Error`` stdout block (the
    CLI reports operational errors that way — see ``_CLI_ERROR_MARKER``).

    ``secrets`` are redacted from EVERYTHING this function returns or raises (the
    echoed command, captured stdout/stderr, the success-path return value, and a
    timeout diagnostic), so a password passed to ``fill`` — which the CLI echoes
    back verbatim on success (``…fill('<pw>')``) — never leaks into the returned
    text, logs, or exceptions. NOTE: ``subprocess.TimeoutExpired`` is converted to
    ``PlaywrightCliError`` here because its own ``str()`` embeds the raw argv
    (cleartext password); letting it propagate would leak the secret via any caller
    that interpolates the exception (e.g. the gated pytest fixture).
    """
    cmd = [PLAYWRIGHT_CLI]
    if session:
        cmd.append(f"-s={session}")
    cmd.extend(args)
    cmd_shown = _redact(" ".join(cmd), secrets)
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    except subprocess.TimeoutExpired as exc:
        out = _redact(exc.stdout or "", secrets) if isinstance(exc.stdout, str) else ""
        err = _redact(exc.stderr or "", secrets) if isinstance(exc.stderr, str) else ""
        raise PlaywrightCliError(f"`{cmd_shown}` timed out after {exc.timeout}s:\n{out}\n{err}") from None
    stdout = _redact(proc.stdout, secrets)
    if proc.returncode != 0 or _CLI_ERROR_MARKER in proc.stdout:
        stderr = _redact(proc.stderr, secrets)
        raise PlaywrightCliError(f"`{cmd_shown}` failed (exit {proc.returncode}):\n{stdout}\n{stderr}")
    return stdout


def current_path(*, session: str | None = None) -> str:
    """Return the browser's current ``location.pathname`` (no query/host)."""
    output = _run_cli("eval", "() => location.pathname", session=session)
    # ``eval`` prints the JSON result on its own line, e.g. `"/settings/"`.
    match = re.search(r'"([^"]*)"', output)
    if match is None:
        raise PlaywrightCliError(f"could not read location.pathname from:\n{output}")
    return match.group(1)


def _is_login_path(path: str) -> bool:
    """True if ``path`` is the login page (with or without a trailing slash)."""
    return path.rstrip("/") == LOGIN_PATH


def login(base_url: str, password: str, *, session: str | None = None) -> None:
    """Authenticate the browser session via the real ``/login`` form.

    Navigates to ``/login``, fills the password field and submits; the panel
    responds with a session cookie that authenticates every later navigation.
    Raises if the form did not authenticate (still on ``/login`` afterwards).
    """
    base = base_url.rstrip("/")
    secrets = (password,)
    _run_cli("goto", f"{base}{LOGIN_PATH}", session=session, secrets=secrets)
    _run_cli("fill", _PASSWORD_FIELD, password, "--submit", session=session, secrets=secrets)
    if _is_login_path(current_path(session=session)):
        raise RedirectedToLoginError("login failed: still on /login after submitting the password (wrong WEB_PASS?)")


def parse_error_count(console_output: str) -> int:
    """Extract the error count from ``playwright-cli console`` output."""
    match = _ERROR_COUNT_RE.search(console_output)
    if match is None:
        raise PlaywrightCliError(f"could not find an error count in console output:\n{console_output}")
    return int(match.group(1))


def _error_lines(console_output: str) -> list[str]:
    """Collect the ``[ERROR] ...`` lines, for human-readable reporting."""
    return [line.strip() for line in console_output.splitlines() if line.lstrip().startswith("[ERROR]")]


def check_page(base_url: str, path: str, *, session: str | None = None, settle: float = 0.0) -> PageResult:
    """Navigate to one page and read back its console errors.

    Raises :class:`RedirectedToLoginError` if the navigation bounced to ``/login``
    (an unauthenticated session) — otherwise a broken auth would silently report
    the clean login form as a clean panel page. ``settle`` optionally sleeps after
    navigation so console errors emitted shortly after load (async/deferred) are
    still captured before the one-shot console read.
    """
    base = base_url.rstrip("/")
    _run_cli("goto", f"{base}{path}", session=session)
    landed = current_path(session=session)
    if _is_login_path(landed) and not _is_login_path(path):
        raise RedirectedToLoginError(f"navigation to {path!r} bounced to {landed!r}: session is not authenticated")
    if settle > 0:
        _run_cli("eval", f"() => new Promise(r => setTimeout(r, {int(settle * 1000)}))", session=session)
    output = _run_cli("console", "error", session=session)
    return PageResult(path=path, error_count=parse_error_count(output), errors=_error_lines(output))


def run_smoke(
    base_url: str,
    password: str | None = None,
    paths: tuple[str, ...] = PANEL_PATHS,
    *,
    session: str | None = None,
    settle: float = 0.0,
) -> list[PageResult]:
    """Open a browser, (log in if needed,) walk every panel page, return results.

    The browser session is always closed, even on error, so a failed run does
    not leave a zombie ``playwright-cli`` process behind.
    """
    base = base_url.rstrip("/")
    _run_cli("open", base, session=session)
    try:
        if password:
            login(base, password, session=session)
        return [check_page(base, path, session=session, settle=settle) for path in paths]
    finally:
        try:
            _run_cli("close", session=session)
        except PlaywrightCliError:
            pass


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
        "--settle",
        type=float,
        default=float(os.environ.get("E2E_SETTLE", "0") or "0"),
        help="Seconds to wait after each page load before reading the console "
        "(catches async errors fired shortly after load; default: %(default)s).",
    )
    args = parser.parse_args(argv)
    # Basic sanity on the base URL so a typo fails clearly rather than mid-walk.
    if not urlsplit(args.base_url).scheme:
        parser.error(f"--base-url must include a scheme, got {args.base_url!r}")

    results = run_smoke(args.base_url, args.web_pass, settle=args.settle)
    print(format_summary(results))
    return 0 if all(r.clean for r in results) else 1


if __name__ == "__main__":
    sys.exit(main())
