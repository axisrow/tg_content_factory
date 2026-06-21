"""Pure-logic tests for the console-smoke helpers (issue #792).

These run in normal CI (no browser, no live server) and cover the parsing /
redaction / auth-guard logic in ``tests/e2e/console_smoke.py``. The actual
browser-walking is exercised by the opt-in ``test_console_smoke.py``.
"""

from __future__ import annotations

import pytest

from tests.e2e import console_smoke
from tests.e2e.console_smoke import PageResult, RedirectedToLoginError

# Real ``playwright-cli console error`` output captured from a live run.
_OUTPUT_WITH_ERRORS = (
    "### Result\n"
    "Total messages: 4 (Errors: 2, Warnings: 1)\n"
    'Returning 2 messages for level "error"\n'
    "\n"
    "[ERROR] boom1 @ :0\n"
    "[ERROR] boom2 @ :0\n"
)
_OUTPUT_CLEAN = "### Result\nTotal messages: 0 (Errors: 0, Warnings: 0)\n"


# --- error-count parsing ----------------------------------------------------


def test_parse_error_count_with_errors() -> None:
    assert console_smoke.parse_error_count(_OUTPUT_WITH_ERRORS) == 2


def test_parse_error_count_clean() -> None:
    assert console_smoke.parse_error_count(_OUTPUT_CLEAN) == 0


def test_parse_error_count_missing_raises() -> None:
    with pytest.raises(console_smoke.PlaywrightCliError):
        console_smoke.parse_error_count("garbage with no summary line")


def test_error_lines_extracted() -> None:
    assert console_smoke._error_lines(_OUTPUT_WITH_ERRORS) == [
        "[ERROR] boom1 @ :0",
        "[ERROR] boom2 @ :0",
    ]


# --- secret redaction (no WEB_PASS in diagnostics) --------------------------


def test_redact_replaces_secret() -> None:
    assert console_smoke._redact("fill #password hunter2 --submit", ("hunter2",)) == "fill #password *** --submit"


def test_redact_ignores_empty_secret() -> None:
    # An empty/None-ish secret must not turn the whole string into "***".
    assert console_smoke._redact("nothing to hide", ("",)) == "nothing to hide"


def test_redact_handles_multiple_secrets() -> None:
    assert console_smoke._redact("a=p1 b=p2", ("p1", "p2")) == "a=*** b=***"


def _fake_proc(returncode: int, stdout: str, stderr: str = ""):
    class _Proc:
        pass

    p = _Proc()
    p.returncode = returncode  # type: ignore[attr-defined]
    p.stdout = stdout  # type: ignore[attr-defined]
    p.stderr = stderr  # type: ignore[attr-defined]
    return p


def test_run_cli_redacts_secret_on_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    # A failing playwright-cli call must not echo the password into the exception.
    monkeypatch.setattr(
        console_smoke.subprocess, "run", lambda *a, **k: _fake_proc(1, "boom with hunter2 in output", "stderr hunter2")
    )
    with pytest.raises(console_smoke.PlaywrightCliError) as excinfo:
        console_smoke._run_cli("fill", "#password", "hunter2", secrets=("hunter2",))
    assert "hunter2" not in str(excinfo.value)
    assert "***" in str(excinfo.value)


def test_run_cli_redacts_secret_on_success(monkeypatch: pytest.MonkeyPatch) -> None:
    # CRITICAL: `fill --submit` echoes the cleartext password in its SUCCESS stdout
    # (`...fill('hunter2')`), exit 0. _run_cli must redact the returned value too.
    monkeypatch.setattr(
        console_smoke.subprocess,
        "run",
        lambda *a, **k: _fake_proc(0, "### Ran Playwright code\nawait page.locator('#password').fill('hunter2');\n"),
    )
    out = console_smoke._run_cli("fill", "#password", "hunter2", "--submit", secrets=("hunter2",))
    assert "hunter2" not in out
    assert "***" in out


def test_run_cli_raises_on_error_marker_despite_exit_zero(monkeypatch: pytest.MonkeyPatch) -> None:
    # CRITICAL: playwright-cli prints `### Error` and exits 0 for a dead server /
    # missing element / failed eval. _run_cli must treat that as a failure, else
    # the gated smoke test passes green against a dead server.
    monkeypatch.setattr(
        console_smoke.subprocess,
        "run",
        lambda *a, **k: _fake_proc(0, "### Error\nError: net::ERR_CONNECTION_REFUSED at http://x/\n"),
    )
    with pytest.raises(console_smoke.PlaywrightCliError):
        console_smoke._run_cli("goto", "http://x/")


def test_run_cli_console_output_not_mistaken_for_error(monkeypatch: pytest.MonkeyPatch) -> None:
    # A normal `console error` result contains `[ERROR]` lines but NOT the literal
    # `### Error` marker, so it must NOT be treated as a CLI failure.
    monkeypatch.setattr(
        console_smoke.subprocess,
        "run",
        lambda *a, **k: _fake_proc(0, "### Result\nTotal messages: 1 (Errors: 1, Warnings: 0)\n\n[ERROR] boom @ :0\n"),
    )
    out = console_smoke._run_cli("console", "error")
    assert console_smoke.parse_error_count(out) == 1


def test_run_cli_redacts_secret_on_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    # CRITICAL: subprocess.TimeoutExpired's str() embeds the raw argv (the
    # cleartext password). _run_cli must convert it to a redacted PlaywrightCliError
    # so a hung `fill` can't leak WEB_PASS via any caller that interpolates it.
    def _raise_timeout(*a: object, **k: object):
        raise console_smoke.subprocess.TimeoutExpired(
            cmd=["playwright-cli", "fill", "#password", "hunter2", "--submit"], timeout=120
        )

    monkeypatch.setattr(console_smoke.subprocess, "run", _raise_timeout)
    with pytest.raises(console_smoke.PlaywrightCliError) as excinfo:
        console_smoke._run_cli("fill", "#password", "hunter2", "--submit", secrets=("hunter2",))
    assert "hunter2" not in str(excinfo.value)
    assert "***" in str(excinfo.value)
    # The original TimeoutExpired (whose str() leaks the password argv) must be
    # suppressed from the chained traceback via `raise ... from None`, so it cannot
    # resurface through a "During handling…" context line. (We assert the chaining
    # flags rather than scanning the formatted traceback, which here would show the
    # test's own source line containing the literal "hunter2".)
    assert excinfo.value.__cause__ is None
    assert excinfo.value.__suppress_context__ is True


# --- login-path detection / current_path ------------------------------------


def test_is_login_path() -> None:
    assert console_smoke._is_login_path("/login") is True
    assert console_smoke._is_login_path("/login/") is True
    assert console_smoke._is_login_path("/settings") is False
    assert console_smoke._is_login_path("/") is False


def test_current_path_extracts_pathname(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(console_smoke, "_run_cli", lambda *a, **k: '### Result\n"/settings/"\n')
    assert console_smoke.current_path() == "/settings/"


def test_current_path_missing_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(console_smoke, "_run_cli", lambda *a, **k: "### Result\nno-json-here\n")
    with pytest.raises(console_smoke.PlaywrightCliError):
        console_smoke.current_path()


# --- check_page auth guard (the critical false-negative fix) ----------------


def test_check_page_raises_when_redirected_to_login(monkeypatch: pytest.MonkeyPatch) -> None:
    # Simulate an unauthenticated session: navigating to /settings lands on /login.
    monkeypatch.setattr(console_smoke, "_run_cli", lambda *a, **k: "")
    monkeypatch.setattr(console_smoke, "current_path", lambda **k: "/login")
    with pytest.raises(RedirectedToLoginError):
        console_smoke.check_page("http://host", "/settings")


def test_check_page_ok_when_landed_on_target(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, ...]] = []

    def fake_run(*args: str, **kwargs: object) -> str:
        calls.append(args)
        if args[:1] == ("console",):
            return _OUTPUT_CLEAN
        return ""

    monkeypatch.setattr(console_smoke, "_run_cli", fake_run)
    monkeypatch.setattr(console_smoke, "current_path", lambda **k: "/settings/")
    result = console_smoke.check_page("http://host", "/settings")
    assert result.clean is True
    assert result.path == "/settings"
    # The login page itself is allowed to "land on /login" (path == /login).
    monkeypatch.setattr(console_smoke, "current_path", lambda **k: "/login")
    assert console_smoke.check_page("http://host", "/login").clean is True


def test_login_raises_on_wrong_password(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(console_smoke, "_run_cli", lambda *a, **k: "")
    monkeypatch.setattr(console_smoke, "current_path", lambda **k: "/login")
    with pytest.raises(RedirectedToLoginError):
        console_smoke.login("http://host", "wrong-pass")


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
    # Guard: keep the walked set aligned with the pages enumerated in issue #792.
    assert console_smoke.PANEL_PATHS == (
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
