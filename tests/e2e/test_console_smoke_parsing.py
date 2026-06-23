"""Pure-logic tests for the console-smoke helpers (issues #792, #1014).

These run in normal CI (no browser, no live server) and cover the
redaction / auth-guard / dead-server / console-collection logic in
``tests/e2e/console_smoke.py``. The actual browser-walking is exercised by the
opt-in ``test_console_smoke.py``.

Since #1014 the module drives the Playwright Python API instead of shelling out
to ``playwright-cli``, so the old text-parsing tests (CLI stdout regex, argv
redaction, ``### Error`` marker) are gone — but the *behaviours* they protected
are preserved here against fake ``Page`` / ``ConsoleMessage`` objects:

- secret redaction (``WEB_PASS`` never in diagnostics/exceptions),
- the ``/login`` bounce guard (``RedirectedToLoginError`` — the false-negative fix),
- the dead-server guard (``ConsoleSmokeError`` from a failed navigation — the
  Python-API equivalent of the old "exit 0 + ``### Error``" guard),
- console *error* collection (only ``console.error`` + uncaught exceptions count).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

import pytest

from tests.e2e import console_smoke
from tests.e2e.console_smoke import ConsoleSmokeError, PageResult, RedirectedToLoginError

if TYPE_CHECKING:
    from playwright.sync_api import ConsoleMessage, Page


# --- fakes mirroring the Playwright sync API surface we use ------------------
#
# The console_smoke functions are typed against the real Playwright ``Page`` /
# ``ConsoleMessage`` classes. Our fakes are structurally compatible but not
# subclasses, so we ``cast`` them at the call site — these helpers keep the casts
# in one place and the tests readable. The fakes never touch a real browser.


def _as_page(page: _FakePage) -> "Page":
    return cast("Page", page)


def _as_msg(msg: _FakeConsoleMessage) -> "ConsoleMessage":
    return cast("ConsoleMessage", msg)


class _FakeConsoleMessage:
    """Stand-in for ``playwright.sync_api.ConsoleMessage``."""

    def __init__(self, type_: str, text: str, location: dict | None = None) -> None:
        self.type = type_
        self.text = text
        self.location = location or {}


class _FakePageError(Exception):
    """Stand-in for a ``pageerror`` payload (carries a ``.message``)."""

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message


class _NavCtx:
    """No-op stand-in for Playwright's ``expect_navigation`` context manager."""

    def __enter__(self) -> None:
        return None

    def __exit__(self, *_exc: object) -> None:
        return None


class _FakePage:
    """Minimal fake ``Page`` recording listeners and serving a scripted ``url``.

    ``goto`` can be told to raise (dead-server simulation), to land on a fixed URL
    regardless of the target (server-redirect simulation, e.g. a bounce to
    ``/login``), or — by default — to land on exactly the requested URL.
    ``fill`` / ``press`` / ``expect_navigation`` are no-op stand-ins so the login
    flow can run without a browser.
    """

    def __init__(
        self,
        *,
        url: str = "http://host/",
        goto_raises: Exception | None = None,
        lands_on: str | None = None,
    ) -> None:
        self.url = url
        self._goto_raises = goto_raises
        self._lands_on = lands_on
        self._console_handlers: list = []
        self._pageerror_handlers: list = []
        self.filled: dict[str, str] = {}

    def on(self, event: str, handler) -> None:  # noqa: ANN001 - test fake
        if event == "console":
            self._console_handlers.append(handler)
        elif event == "pageerror":
            self._pageerror_handlers.append(handler)

    def goto(self, url: str, **_kwargs: object) -> None:
        if self._goto_raises is not None:
            raise self._goto_raises
        # ``lands_on`` models a server-side redirect: the browser ends up there
        # no matter where we asked to go (e.g. an unauthenticated bounce to /login).
        self.url = self._lands_on if self._lands_on is not None else url

    def fill(self, selector: str, value: str) -> None:
        self.filled[selector] = value

    def press(self, selector: str, key: str) -> None:  # noqa: D401 - test fake
        pass

    def expect_navigation(self, **_kwargs: object) -> "_NavCtx":
        # The login flow only needs the ``with`` block to run the body; the
        # navigation itself is driven by the press() side-effect in the tests.
        return _NavCtx()

    # --- helpers used only by the tests to drive the captured listeners ----

    def emit_console(self, msg: _FakeConsoleMessage) -> None:
        for handler in self._console_handlers:
            handler(msg)

    def emit_pageerror(self, exc: _FakePageError) -> None:
        for handler in self._pageerror_handlers:
            handler(exc)


# --- secret redaction (no WEB_PASS in diagnostics) --------------------------


def test_redact_replaces_secret() -> None:
    assert console_smoke._redact("login with hunter2 now", ("hunter2",)) == "login with *** now"


def test_redact_ignores_empty_secret() -> None:
    # An empty/None-ish secret must not turn the whole string into "***".
    assert console_smoke._redact("nothing to hide", ("",)) == "nothing to hide"


def test_redact_handles_multiple_secrets() -> None:
    assert console_smoke._redact("a=p1 b=p2", ("p1", "p2")) == "a=*** b=***"


# --- login-path detection / path extraction ---------------------------------


def test_is_login_path() -> None:
    assert console_smoke._is_login_path("/login") is True
    assert console_smoke._is_login_path("/login/") is True
    assert console_smoke._is_login_path("/settings") is False
    assert console_smoke._is_login_path("/") is False


def test_path_of_strips_host_and_query() -> None:
    assert console_smoke._path_of("http://host:8080/channels?view=all") == "/channels"
    assert console_smoke._path_of("http://host/settings/") == "/settings/"
    assert console_smoke._path_of("http://host") == ""


# --- console error collection (only errors + uncaught exceptions count) -----


def test_console_error_text_includes_location() -> None:
    msg = _FakeConsoleMessage("error", "boom", {"url": "http://host/app.js", "lineNumber": 12})
    assert console_smoke._console_error_text(_as_msg(msg)) == "[ERROR] boom @ http://host/app.js:12"


def test_console_error_text_without_location() -> None:
    msg = _FakeConsoleMessage("error", "boom", {})
    assert console_smoke._console_error_text(_as_msg(msg)) == "[ERROR] boom"


def test_attach_listeners_collects_only_errors() -> None:
    page = _FakePage()
    sink: list[str] = []
    console_smoke._attach_error_listeners(_as_page(page), sink)
    page.emit_console(_FakeConsoleMessage("log", "just a log"))
    page.emit_console(_FakeConsoleMessage("warning", "just a warning"))
    page.emit_console(_FakeConsoleMessage("error", "boom1", {"url": "u", "lineNumber": 1}))
    page.emit_pageerror(_FakePageError("uncaught boom2"))
    assert sink == ["[ERROR] boom1 @ u:1", "[ERROR] uncaught boom2"]


# --- check_page auth guard (the critical false-negative fix) ----------------


def test_check_page_raises_when_redirected_to_login() -> None:
    # Simulate an unauthenticated session: navigating to /settings bounces to /login.
    page = _FakePage(lands_on="http://host/login")
    with pytest.raises(RedirectedToLoginError):
        console_smoke.check_page(_as_page(page), "http://host", "/settings")


def test_check_page_ok_when_landed_on_target() -> None:
    page = _FakePage(url="http://host/")
    result = console_smoke.check_page(_as_page(page), "http://host", "/settings")
    assert result.clean is True
    assert result.path == "/settings"
    assert result.error_count == 0


def test_check_page_login_page_itself_is_allowed_to_land_on_login() -> None:
    # The login page itself is allowed to "land on /login" (path == /login).
    page = _FakePage(url="http://host/login")
    assert console_smoke.check_page(_as_page(page), "http://host", "/login").clean is True


def test_check_page_collects_console_errors() -> None:
    # A page that fires a console error should report it (not raise).
    class _ErroringPage(_FakePage):
        def goto(self, url: str, **kwargs: object) -> None:
            super().goto(url, **kwargs)
            # Errors arrive during/after navigation while the listener is live.
            self.emit_console(_FakeConsoleMessage("error", "boom", {"url": "u", "lineNumber": 3}))

    page = _ErroringPage(url="http://host/settings")
    result = console_smoke.check_page(_as_page(page), "http://host", "/settings")
    assert result.clean is False
    assert result.error_count == 1
    assert result.errors == ["[ERROR] boom @ u:3"]


# --- dead-server guard (Python-API equivalent of "exit 0 + ### Error") ------


def test_check_page_raises_console_smoke_error_on_dead_server() -> None:
    # CRITICAL: a navigation failure (connection refused / timeout) must raise so
    # a dead server can't walk every page and report "0 errors".
    from playwright.sync_api import Error as PlaywrightError

    page = _FakePage(goto_raises=PlaywrightError("net::ERR_CONNECTION_REFUSED"))
    with pytest.raises(ConsoleSmokeError):
        console_smoke.check_page(_as_page(page), "http://host", "/settings")


def test_goto_redacts_secret_in_error() -> None:
    # CRITICAL: a navigation error during login must not leak the password into
    # the raised ConsoleSmokeError (belt-and-braces, even though the API never
    # puts the secret on the message).
    from playwright.sync_api import Error as PlaywrightError

    page = _FakePage(goto_raises=PlaywrightError("failed near hunter2 boundary"))
    with pytest.raises(ConsoleSmokeError) as excinfo:
        console_smoke._goto(_as_page(page), "http://host/login", secrets=("hunter2",))
    assert "hunter2" not in str(excinfo.value)
    assert "***" in str(excinfo.value)


# --- login flow -------------------------------------------------------------


def test_login_fills_password_and_authenticates() -> None:
    # After a successful login the page is NOT on /login and the field was filled.
    page = _FakePage(url="http://host/login")

    # Submitting "navigates" away from /login — emulate by flipping url on press.
    def _press(selector: str, key: str) -> None:
        page.url = "http://host/"

    page.press = _press  # type: ignore[method-assign]
    console_smoke.login(_as_page(page), "http://host", "hunter2")
    assert page.filled["#password"] == "hunter2"
    assert console_smoke._path_of(page.url) == "/"


def test_login_raises_on_wrong_password() -> None:
    # Wrong password: the form re-renders and we stay on /login → loud failure.
    page = _FakePage(url="http://host/login")
    with pytest.raises(RedirectedToLoginError):
        console_smoke.login(_as_page(page), "http://host", "wrong-pass")


# --- result model / summary -------------------------------------------------


def test_page_result_clean_flag() -> None:
    assert PageResult("/", 0, []).clean is True
    assert PageResult("/x", 1, ["[ERROR] boom @ :0"]).clean is False


def test_format_summary_reports_counts_and_errors() -> None:
    results = [
        PageResult("/", 0, []),
        PageResult("/analytics", 2, ["[ERROR] boom1 @ :0", "[ERROR] boom2 @ :0"]),
    ]
    summary = console_smoke.format_summary(results)
    assert "SUMMARY: 1/2 clean, 1 with errors" in summary
    assert "✗ /analytics" in summary
    assert "✓ /" in summary
    assert "[ERROR] boom1 @ :0" in summary


def test_panel_paths_match_issue_list() -> None:
    # Guard: keep the walked set aligned with the curated page list. Originally
    # the #792 set; extended in #1013 with newer full-page routes. Change this
    # tuple and PANEL_PATHS (console_smoke.py) in the same edit, or CI goes red.
    assert console_smoke.PANEL_PATHS == (
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
