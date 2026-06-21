"""Pure-logic tests for the console-smoke helpers (issue #792).

These run in normal CI (no browser, no live server) and cover the parsing /
URL-building helpers in ``tests/e2e/console_smoke.py``. The browser-walking
itself is exercised by the opt-in ``test_console_smoke.py``.
"""

from __future__ import annotations

import pytest

from tests.e2e import console_smoke
from tests.e2e.console_smoke import PageResult

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


def test_authed_base_url_no_password_strips_trailing_slash() -> None:
    assert console_smoke.authed_base_url("http://localhost:8000/", None) == "http://localhost:8000"


def test_authed_base_url_embeds_credentials() -> None:
    assert console_smoke.authed_base_url("http://localhost:8000", "secret") == "http://admin:secret@localhost:8000"


def test_authed_base_url_preserves_non_default_port() -> None:
    assert console_smoke.authed_base_url("http://127.0.0.1:9999", "p") == "http://admin:p@127.0.0.1:9999"


def test_authed_base_url_percent_encodes_special_chars() -> None:
    # A password with ":" / "@" / "/" must not break the userinfo segment.
    result = console_smoke.authed_base_url("http://localhost:8000", "a:b@c/d")
    assert result == "http://admin:a%3Ab%40c%2Fd@localhost:8000"


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
