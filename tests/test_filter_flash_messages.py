"""Flash-error coverage for hard-delete-all/selected routes.

The route layer redirects with error codes plus purged/skipped/expected
query parameters. The browser-side flash renderer in app.js looks the code
up in FLASH_ERRORS and then appends the count breakdown so the admin sees
exactly what survived. Codex round 10 caught that none of the new error
codes were registered, so the alert banner showed a raw code (or nothing)
and the count breakdown was discarded.
"""
from __future__ import annotations

from pathlib import Path

from src.web.template_globals import FLASH_ERRORS, FLASH_MESSAGES


def test_filter_success_codes_registered():
    """Every success code emitted by the filter routes must be in
    FLASH_MESSAGES so the alert banner renders a human-readable message
    instead of silently swallowing the redirect (review feedback on #568)."""
    required = {
        "filter_reset",
        "filter_reset_selected",
        "filter_applied",
        "filter_toggled",
    }
    missing = required - set(FLASH_MESSAGES.keys())
    assert not missing, f"FLASH_MESSAGES is missing: {sorted(missing)}"


def test_hard_delete_error_codes_registered():
    """Every error code emitted by the filter routes must be in FLASH_ERRORS
    so the browser renders a human-readable message, not the raw code."""
    required = {
        "dev_mode_required_for_hard_delete",
        "no_filtered_channels",
        "hard_delete_confirm_required",
        "hard_delete_set_changed",
        "hard_delete_partial",
    }
    missing = required - set(FLASH_ERRORS.keys())
    assert not missing, f"FLASH_ERRORS is missing: {sorted(missing)}"
    # Sanity-check the partial-failure message mentions enough context.
    partial = FLASH_ERRORS["hard_delete_partial"]
    assert "частично" in partial.lower() or "часть" in partial.lower(), partial


def test_app_js_renders_hard_delete_partial_breakdown():
    """app.js must surface purged/skipped/expected counts next to the
    hard_delete_partial message — otherwise the count breakdown set by the
    route gets dropped on the way to the user."""
    app_js = Path(__file__).resolve().parent.parent / "src" / "web" / "static" / "js" / "app.js"
    src = app_js.read_text(encoding="utf-8")
    assert "hard_delete_partial" in src, "app.js must special-case hard_delete_partial"
    # Pulls all three counts from the URL.
    for param in ("purged", "skipped", "expected"):
        assert f'params.get("{param}")' in src, (
            f"app.js must read {param} out of the URL to show the breakdown"
        )
    # Counts are cleaned up from the URL alongside msg/error/warning so they
    # don't linger as stale state on the next render.
    for param in ("purged", "skipped", "expected"):
        assert f'params.delete("{param}")' in src, (
            f"app.js must drop {param} from the URL after rendering"
        )
