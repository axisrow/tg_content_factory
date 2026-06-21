"""Opt-in e2e: assert no panel page logs a JS console error (issue #792).

This test is **opt-in** and skipped by default — it needs a live web server
(``python -m src.main serve``) plus the installed ``playwright-cli`` binary,
neither of which exists in the standard CI run. Enable it with::

    RUN_E2E_CONSOLE_SMOKE=1 E2E_BASE_URL=http://localhost:8080 WEB_PASS=secret \
        pytest tests/e2e/test_console_smoke.py -m e2e

The actual browser-walking logic lives in ``tests/e2e/console_smoke.py`` so it
can also be run by hand: ``python -m tests.e2e.console_smoke --base-url ... --web-pass ...``.

Once the gate is open, an infrastructure failure (dead server, wrong password,
broken ``playwright-cli`` invocation, hung navigation) must FAIL the test, not
skip it — the user explicitly asked for the check, so silence cannot read as
"all clean". We therefore only skip for the two intentional cases: the gate is
closed (handled by ``pytestmark``) or the binary is simply not installed.
"""

from __future__ import annotations

import os
import shutil
import subprocess

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

    Skips ONLY when the ``playwright-cli`` binary is missing (an intentional
    "not installed" case). Once the gate is open, any live-run failure
    (``PlaywrightCliError``, a redirect-to-login) is re-raised so the test fails
    loudly instead of masquerading as a skip/pass.

    ``_run_cli`` converts a ``subprocess.TimeoutExpired`` into a redacted
    ``PlaywrightCliError``; we still catch the raw ``TimeoutExpired`` here as a
    belt-and-braces guard but deliberately do NOT interpolate the exception (its
    ``str()`` embeds the raw argv, i.e. the cleartext password) — so a hung run
    can never leak ``WEB_PASS`` into the failure message.
    """
    if shutil.which(console_smoke.PLAYWRIGHT_CLI) is None:
        pytest.skip(f"{console_smoke.PLAYWRIGHT_CLI} binary not found on PATH")
    base_url = os.environ.get("E2E_BASE_URL", "http://localhost:8080")
    password = os.environ.get("WEB_PASS") or None
    settle = float(os.environ.get("E2E_SETTLE", "0") or "0")
    try:
        return console_smoke.run_smoke(base_url, password, settle=settle)
    except (console_smoke.PlaywrightCliError, console_smoke.RedirectedToLoginError) as exc:
        raise AssertionError(f"console smoke run failed against {base_url}: {exc}") from exc
    except subprocess.TimeoutExpired:
        # Never interpolate the exception — its str() leaks the password argv.
        raise AssertionError(f"console smoke run timed out against {base_url}") from None


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
