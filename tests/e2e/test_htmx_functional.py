"""Opt-in functional HTMX e2e (issues #1015, #1014) — local-only, never wired into CI.

These assert HTMX **functionality** (lazyload containers actually fill, the /rate
swap produces a verdict fragment, collect buttons OOB-swap both desktop+mobile),
not just the absence of console errors that ``test_console_smoke.py`` covers. They
need a live web server (``python -m src.main serve``) plus the Playwright Chromium
build (``playwright install --with-deps chromium``), so they are skipped by default.
Enable them with::

    RUN_E2E_CONSOLE_SMOKE=1 E2E_BASE_URL=http://localhost:8080 WEB_PASS=secret \\
        pytest tests/e2e/test_htmx_functional.py -m e2e

The gate env is shared with the console smoke (#792) so one switch turns on every
browser-driven e2e against a live server. The browser-walking logic lives in
``tests/e2e/htmx_functional.py`` (driven by the Playwright Python API, the same engine
``console_smoke`` migrated to in #1014) so it can also be run by hand.

Once the gate is open, an infrastructure failure (dead server, wrong password, hung
navigation) must FAIL — never skip — because the user explicitly asked for the check,
so silence cannot read as "all good". We only skip for the two intentional cases:
the gate is closed (handled by ``pytestmark``) or Playwright / its Chromium build is
not installed. ``console_smoke`` redacts ``WEB_PASS`` from every error it raises, so a
failing/hung run can never leak the secret into a failure message.

The one nuance is the OOB collect check: it needs ≥1 channel in the DB (per-row
collect buttons have no empty-state fallback). On a clean DB it returns
``no_channels=True`` and the test skips with a clear reason rather than failing —
seed a channel to actually exercise it.
"""

from __future__ import annotations

import os

import pytest

from tests.e2e import console_smoke, htmx_functional

_GATE_ENV = "RUN_E2E_CONSOLE_SMOKE"
_TRUE_TOKENS = frozenset({"1", "true", "yes", "on"})


def _gate_open() -> bool:
    return os.environ.get(_GATE_ENV, "").strip().lower() in _TRUE_TOKENS


pytestmark = [
    pytest.mark.e2e,
    pytest.mark.skipif(
        not _gate_open(),
        reason=f"opt-in HTMX functional e2e; set {_GATE_ENV}=1 against a live server to run",
    ),
]


@pytest.fixture(scope="module")
def htmx_results() -> htmx_functional.HtmxCheckResults:
    """Open the browser once, run every HTMX functional check, share the results.

    Skips ONLY when Playwright (or its bundled Chromium) is not installed — an
    intentional "not installed" case. Once the gate is open, any live-run failure
    (a :class:`~tests.e2e.console_smoke.ConsoleSmokeError` from a dead server / hung
    navigation, or a :class:`~tests.e2e.console_smoke.RedirectedToLoginError` from a
    broken auth) is re-raised as an ``AssertionError`` so the test fails loudly
    instead of masquerading as a skip. The message only ever surfaces the base URL —
    ``console_smoke`` redacts the password from its errors — so the secret cannot leak.
    """
    try:
        import playwright.sync_api  # noqa: F401
    except ImportError:
        pytest.skip("playwright not installed; `pip install -e .[dev]` to enable the HTMX functional e2e")

    base_url = os.environ.get("E2E_BASE_URL", "http://localhost:8080")
    password = os.environ.get("WEB_PASS") or None
    try:
        return htmx_functional.run_htmx_checks(base_url, password)
    except console_smoke.RedirectedToLoginError as exc:
        raise AssertionError(f"HTMX functional e2e failed against {base_url}: {exc}") from exc
    except console_smoke.ConsoleSmokeError as exc:
        # A missing Chromium build raises ConsoleSmokeError at launch — treat that one
        # operational failure as an intentional "not installed" skip; every other
        # ConsoleSmokeError (dead server, hung nav) is a loud failure.
        if "Executable doesn't exist" in str(exc) or "playwright install" in str(exc):
            pytest.skip("playwright Chromium not installed; run `playwright install --with-deps chromium`")
        raise AssertionError(f"HTMX functional e2e failed against {base_url}: {exc}") from exc


def test_lazyload_containers_filled(htmx_results: htmx_functional.HtmxCheckResults) -> None:
    """Every lazyload page: container filled (not skeleton) AND fragment returned 200."""
    summary = htmx_functional.format_summary(htmx_results)
    bad = [r for r in htmx_results.lazyload if not r.ok]
    assert not bad, f"lazyload pages that didn't fill / fragment !=200:\n{summary}"


def test_all_lazyload_specs_checked(htmx_results: htmx_functional.HtmxCheckResults) -> None:
    """Guard against silently skipping a lazyload page (e.g. an early redirect loop)."""
    walked = {r.path for r in htmx_results.lazyload}
    assert walked == {s.path for s in htmx_functional.LAZYLOAD_SPECS}, (
        f"expected to check {{s.path for s in LAZYLOAD_SPECS}}, checked {walked}"
    )


def test_rate_swap_produces_verdict_fragment(htmx_results: htmx_functional.HtmxCheckResults) -> None:
    """#999: clicking 'Запустить судью' HTMX-swaps a verdict (.card) or error (.alert) fragment."""
    rate = htmx_results.rate
    assert rate.ok, (
        f"/rate swap did not produce a verdict fragment: kind={rate.kind} post={rate.post_status} {rate.detail}"
    )


def test_collect_buttons_oob_swap_both(htmx_results: htmx_functional.HtmxCheckResults) -> None:
    """OOB: clicking the desktop collect button disables BOTH the desktop and mobile buttons."""
    collect = htmx_results.collect
    # Skip ONLY for a genuine empty DB (table lazyloaded fine, no rows). A broken /channels
    # lazyload sets no_channels=False and must FAIL here — skipping it would hide the exact
    # regression this module exists to catch (Codex review).
    if collect.no_channels:
        pytest.skip(f"OOB collect needs ≥1 channel in the DB; {collect.detail}")
    assert collect.ok, (
        f"OOB swap incomplete (channel pk={collect.channel_pk}): "
        f"desktop={collect.desktop_disabled} mobile={collect.mobile_disabled} {collect.detail}"
    )
