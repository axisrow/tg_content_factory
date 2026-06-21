"""Opt-in e2e: assert no panel page logs a JS console error (issue #792).

This test is **opt-in** and skipped by default — it needs a live web server
(``python -m src.main serve``) plus the installed ``playwright-cli`` binary,
neither of which exists in the standard CI run. Enable it with::

    RUN_E2E_CONSOLE_SMOKE=1 E2E_BASE_URL=http://localhost:8080 WEB_PASS=secret \
        pytest tests/e2e/test_console_smoke.py -m e2e

The actual browser-walking logic lives in ``tests/e2e/console_smoke.py`` so it
can also be run by hand: ``python -m tests.e2e.console_smoke --base-url ... --web-pass ...``.
"""

from __future__ import annotations

import os
import shutil

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
    """Walk every panel page once and share the results across the test(s)."""
    if shutil.which(console_smoke.PLAYWRIGHT_CLI) is None:
        pytest.skip(f"{console_smoke.PLAYWRIGHT_CLI} binary not found on PATH")
    base_url = os.environ.get("E2E_BASE_URL", "http://localhost:8080")
    password = os.environ.get("WEB_PASS") or None
    try:
        return console_smoke.run_smoke(base_url, password)
    except console_smoke.PlaywrightCliError as exc:  # pragma: no cover - depends on live env
        pytest.skip(f"could not reach the panel via playwright-cli: {exc}")


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
