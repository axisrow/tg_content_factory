"""Opt-in e2e: assert no panel page logs a JS console error (issues #792, #1014).

This test is **opt-in** and skipped by default — it needs a live web server
(``python -m src.main serve``) plus the Playwright browser binaries
(``playwright install --with-deps chromium``), neither of which exists in the
standard CI run. Enable it with::

    RUN_E2E_CONSOLE_SMOKE=1 E2E_BASE_URL=http://localhost:8080 WEB_PASS=secret \
        pytest tests/e2e/test_console_smoke.py -m e2e

The actual browser-walking logic lives in ``tests/e2e/console_smoke.py`` so it
can also be run by hand: ``python -m tests.e2e.console_smoke --base-url ... --web-pass ...``.

Once the gate is open, an infrastructure failure (dead server, wrong password,
hung navigation, missing browser binary at runtime) must FAIL the test, not
silently pass — the user explicitly asked for the check, so silence cannot read
as "all clean". We therefore only skip for the two intentional cases: the gate
is closed (handled by ``pytestmark``) or Playwright / its Chromium build is
simply not installed.
"""

from __future__ import annotations

import os

import pytest

from tests.e2e import console_smoke

_GATE_ENV = "RUN_E2E_CONSOLE_SMOKE"
_TRUE_TOKENS = frozenset({"1", "true", "yes", "on"})


def _gate_open() -> bool:
    return os.environ.get(_GATE_ENV, "").strip().lower() in _TRUE_TOKENS


pytestmark = [
    pytest.mark.e2e,
    pytest.mark.skipif(
        not _gate_open(),
        reason=f"opt-in console smoke test; set {_GATE_ENV}=1 against a live server to run",
    ),
]


@pytest.fixture(scope="module")
def smoke_results() -> list[console_smoke.PageResult]:
    """Walk every panel page once and share the results across the test(s).

    Skips ONLY when Playwright (or its bundled Chromium) is not installed — an
    intentional "not installed" case. Once the gate is open, any live-run failure
    (a :class:`~tests.e2e.console_smoke.ConsoleSmokeError` from a dead server /
    hung navigation, or a :class:`~tests.e2e.console_smoke.RedirectedToLoginError`
    from a broken auth) is re-raised as an ``AssertionError`` so the test fails
    loudly instead of masquerading as a skip/pass.

    The failure message never interpolates the password: ``console_smoke``
    redacts ``WEB_PASS`` from every error it raises, and we only ever surface the
    base URL here — so a failing/hung run can never leak the secret.
    """
    try:
        import playwright.sync_api  # noqa: F401
    except ImportError:
        pytest.skip("playwright not installed; `pip install -e .[dev]` to enable the console smoke test")

    base_url = os.environ.get("E2E_BASE_URL", "http://localhost:8080")
    password = os.environ.get("WEB_PASS") or None
    try:
        return console_smoke.run_smoke(base_url, password)
    except console_smoke.RedirectedToLoginError as exc:
        raise AssertionError(f"console smoke run failed against {base_url}: {exc}") from exc
    except console_smoke.ConsoleSmokeError as exc:
        # A missing Chromium build raises ConsoleSmokeError at launch — treat that
        # one operational failure as an intentional "not installed" skip; every
        # other ConsoleSmokeError (dead server, hung nav) is a loud failure.
        if "Executable doesn't exist" in str(exc) or "playwright install" in str(exc):
            pytest.skip("playwright Chromium not installed; run `playwright install --with-deps chromium`")
        raise AssertionError(f"console smoke run failed against {base_url}: {exc}") from exc


def test_no_console_errors_on_any_page(smoke_results: list[console_smoke.PageResult]) -> None:
    """Every walked page must report zero console errors."""
    # Surface the full per-page summary so a failure shows which pages broke.
    summary = console_smoke.format_summary(smoke_results)
    dirty = [r for r in smoke_results if not r.clean]
    assert not dirty, f"pages with console errors:\n{summary}"


def test_all_expected_paths_were_walked(smoke_results: list[console_smoke.PageResult]) -> None:
    """Guard against silently skipping pages (e.g. an early redirect loop)."""
    walked = {r.path for r in smoke_results}
    assert walked == set(
        console_smoke.PANEL_PATHS
    ), f"expected to walk {set(console_smoke.PANEL_PATHS)}, walked {walked}"
