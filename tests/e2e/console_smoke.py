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
  ``open`` → ``goto`` (per page) → ``console error`` → ``close``.
- ``playwright-cli`` clears its console buffer on navigation, so after a
  ``goto`` the buffer holds only that page's messages — exactly what we want.
- Auth: ``BasicAuthMiddleware`` accepts ``Authorization: Basic`` on any request
  and sets a session cookie, so embedding ``admin:<pass>`` straight into the URL
  (``http://admin:PASS@host/``) authenticates every navigation with no separate
  login step. With no password configured the panel is open and we navigate as-is.
"""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from urllib.parse import quote, urlsplit, urlunsplit

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
PANEL_USERNAME = "admin"


class PlaywrightCliError(RuntimeError):
    """Raised when a ``playwright-cli`` invocation fails outright (non-zero exit)."""


@dataclass(frozen=True)
class PageResult:
    """Outcome of visiting a single page."""

    path: str
    error_count: int
    errors: list[str]

    @property
    def clean(self) -> bool:
        return self.error_count == 0


def _run_cli(*args: str, session: str | None = None) -> str:
    """Run ``playwright-cli`` and return stdout; raise on non-zero exit."""
    cmd = [PLAYWRIGHT_CLI]
    if session:
        cmd.append(f"-s={session}")
    cmd.extend(args)
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    if proc.returncode != 0:
        raise PlaywrightCliError(f"`{' '.join(cmd)}` exited {proc.returncode}:\n{proc.stdout}\n{proc.stderr}")
    return proc.stdout


def authed_base_url(base_url: str, password: str | None) -> str:
    """Embed ``admin:<password>`` into ``base_url`` for Basic auth.

    Returns ``base_url`` unchanged when no password is given (open panel). The
    password is percent-encoded so special characters survive the URL userinfo.
    """
    if not password:
        return base_url.rstrip("/")
    parts = urlsplit(base_url)
    userinfo = f"{quote(PANEL_USERNAME, safe='')}:{quote(password, safe='')}"
    netloc = f"{userinfo}@{parts.hostname}"
    if parts.port is not None:
        netloc = f"{netloc}:{parts.port}"
    return urlunsplit((parts.scheme, netloc, parts.path, parts.query, parts.fragment)).rstrip("/")


def parse_error_count(console_output: str) -> int:
    """Extract the error count from ``playwright-cli console`` output."""
    match = _ERROR_COUNT_RE.search(console_output)
    if match is None:
        raise PlaywrightCliError(f"could not find an error count in console output:\n{console_output}")
    return int(match.group(1))


def _error_lines(console_output: str) -> list[str]:
    """Collect the ``[ERROR] ...`` lines, for human-readable reporting."""
    return [line.strip() for line in console_output.splitlines() if line.lstrip().startswith("[ERROR]")]


def check_page(base_with_auth: str, path: str, *, session: str | None = None) -> PageResult:
    """Navigate to one page and read back its console errors."""
    url = f"{base_with_auth}{path}"
    _run_cli("goto", url, session=session)
    output = _run_cli("console", "error", session=session)
    return PageResult(path=path, error_count=parse_error_count(output), errors=_error_lines(output))


def run_smoke(
    base_url: str,
    password: str | None = None,
    paths: tuple[str, ...] = PANEL_PATHS,
    *,
    session: str | None = None,
) -> list[PageResult]:
    """Open a browser, walk every panel page, and return per-page results.

    The browser session is always closed, even on error, so a failed run does
    not leave a zombie ``playwright-cli`` process behind.
    """
    base_with_auth = authed_base_url(base_url, password)
    # Open against the auth'd root so the first navigation is already authenticated.
    _run_cli("open", base_with_auth or base_url, session=session)
    try:
        return [check_page(base_with_auth, path, session=session) for path in paths]
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
    args = parser.parse_args(argv)

    results = run_smoke(args.base_url, args.web_pass)
    print(format_summary(results))
    return 0 if all(r.clean for r in results) else 1


if __name__ == "__main__":
    sys.exit(main())
