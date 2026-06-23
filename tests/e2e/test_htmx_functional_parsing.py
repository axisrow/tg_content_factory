"""Pure-logic tests for the HTMX functional e2e helpers (issue #1015).

These run in normal CI (no browser, no live server) and cover the parts of
``tests/e2e/htmx_functional.py`` that need no Playwright: the result-model ``ok``
flags (including the Codex-review empty-DB-vs-broken-lazyload distinction), the spec
coverage guard, and the summary rendering. The actual browser-driving (navigation,
``page.expect_response``, ``wait_for_function``) is exercised by the opt-in
``test_htmx_functional.py`` against a live server.
"""

from __future__ import annotations

from tests.e2e import htmx_functional
from tests.e2e.htmx_functional import (
    CollectOobResult,
    LazyloadResult,
    RateSwapResult,
)

# --- result model ``ok`` flags ----------------------------------------------


def test_lazyload_result_ok_requires_all_three() -> None:
    assert LazyloadResult("/jobs", filled=True, marker_present=True, fragment_status=200).ok is True
    # Any one failing makes it not-ok — a 200-rendered skeleton or a failed fragment
    # must NOT read as filled (the issue's core "valid 200 ≠ filled" pitfall).
    assert LazyloadResult("/jobs", filled=False, marker_present=True, fragment_status=200).ok is False
    assert LazyloadResult("/jobs", filled=True, marker_present=False, fragment_status=200).ok is False
    assert LazyloadResult("/jobs", filled=True, marker_present=True, fragment_status=500).ok is False
    assert LazyloadResult("/jobs", filled=True, marker_present=True, fragment_status=None).ok is False


def test_rate_swap_result_ok_requires_swap_and_200() -> None:
    assert RateSwapResult(swapped=True, kind="verdict", post_status=200).ok is True
    # The no-provider / no-posts path returns the error fragment — still a valid swap.
    assert RateSwapResult(swapped=True, kind="error", post_status=200).ok is True
    # No swap, or a non-200 POST, is not ok.
    assert RateSwapResult(swapped=False, kind="empty", post_status=200).ok is False
    assert RateSwapResult(swapped=True, kind="verdict", post_status=500).ok is False
    assert RateSwapResult(swapped=True, kind="verdict", post_status=None).ok is False


def test_collect_oob_result_ok_requires_both_sides() -> None:
    assert CollectOobResult(channel_pk=1, desktop_disabled=True, mobile_disabled=True).ok is True
    # A desync (only one side swapped) must NOT pass — that's the whole point of OOB.
    assert CollectOobResult(channel_pk=1, desktop_disabled=True, mobile_disabled=False).ok is False
    assert CollectOobResult(channel_pk=1, desktop_disabled=False, mobile_disabled=True).ok is False


def test_collect_oob_broken_lazyload_is_not_a_skip() -> None:
    # Codex review: a broken /channels lazyload must FAIL, not skip. Only a genuine empty DB
    # (no_channels=True) is skip-worthy. A load failure leaves no_channels=False and ok=False.
    load_fail = CollectOobResult(
        channel_pk=None, desktop_disabled=False, mobile_disabled=False, no_channels=False, detail="lazyload failed"
    )
    assert load_fail.no_channels is False
    assert load_fail.ok is False  # → test asserts and FAILS, not skips

    empty_db = CollectOobResult(
        channel_pk=None, desktop_disabled=False, mobile_disabled=False, no_channels=True, detail="no channels"
    )
    assert empty_db.no_channels is True  # → test skips
    # Default: a result built without the flag is treated as a load failure, never an
    # accidental skip (fail-closed).
    assert CollectOobResult(channel_pk=None, desktop_disabled=False, mobile_disabled=False).no_channels is False


# --- spec coverage / summary -------------------------------------------------


def test_lazyload_specs_cover_issue_pages() -> None:
    # Guard: the four pages enumerated in issue #1015's P3 lazyload list.
    assert {s.path for s in htmx_functional.LAZYLOAD_SPECS} == {
        "/jobs",
        "/analytics/channels",
        "/moderation",
        "/dashboard",
    }


def test_format_summary_marks_failures() -> None:
    results = htmx_functional.HtmxCheckResults(
        lazyload=[
            LazyloadResult("/jobs", filled=True, marker_present=True, fragment_status=200),
            LazyloadResult("/dashboard", filled=False, marker_present=False, fragment_status=None, detail="redirected"),
        ],
        rate=RateSwapResult(swapped=True, kind="error", post_status=200),
        collect=CollectOobResult(channel_pk=1, desktop_disabled=True, mobile_disabled=False, detail="desync"),
    )
    summary = htmx_functional.format_summary(results)
    assert "✓ /jobs" in summary
    assert "✗ /dashboard" in summary
    assert "redirected" in summary
    assert "Rate swap (#999): ✓" in summary
    assert "Collect OOB: ✗" in summary
    assert "desync" in summary
