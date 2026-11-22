"""Telethon TL-schema coverage canary (#1549 family).

Two contracts pinned against the **installed** Telethon (never a stub):

1. The four TL constructors the Telegram server sent during the 2026-09-30
   schema-drift incident (unknown to Telethon 1.45 layer 229, still unknown to
   1.46 layer 233) must stay absent from the installed registry. The strict
   xfail is the canary: while Telethon does not know them the test silently
   xfails; the day a Telethon release covers them it XPASSes and **fails the
   suite** — forcing the conversation about removing the SchemaDriftError
   workaround (``tg_agent/telegram/schema_drift.py``). Check a candidate wheel
   the same way before bumping: grep the ids in ``tl/alltlobjects.py`` — the
   RTD changelog lags (it never gained a 1.46 entry).
2. The workaround's subclass contract: ``SchemaDriftError`` is built on the
   real ``TypeNotFoundError`` of whatever Telethon is installed, so every
   isinstance-based classifier keeps working across Telethon upgrades.
"""

import pytest
from telethon.errors import TypeNotFoundError
from telethon.tl.alltlobjects import tlobjects

from tg_agent.telegram.schema_drift import SchemaDriftError, translate_schema_drift

# Constructor IDs captured from live collection failures, 2026-09-30…10-13
# (see memory project-typenotfound-telethon-layer): they are the published
# layer-225 schema's channel / user / replyInlineMarkup / pageBlockBlockquote.
DRIFTED_CONSTRUCTOR_IDS = (0x1C32B11C, 0x31774388, 0x48A30254, 0x263D7C26)


@pytest.mark.xfail(
    strict=True,
    reason=(
        "A Telethon release finally covers the drifted constructors — remove the "
        "SchemaDriftError workaround (schema_drift.py, backends.py translation, "
        "unknown_tl_type_note) in the same PR; keep tests that assert real "
        "TypeNotFoundError classification"
    ),
)
def test_telethon_covers_drifted_constructors() -> None:
    """Canary: passes only when Telethon's registry knows all four drifted ids."""
    for constructor_id in DRIFTED_CONSTRUCTOR_IDS:
        assert constructor_id in tlobjects, (
            f"constructor 0x{constructor_id:08x} is still unknown to the installed "
            "Telethon — the schema-drift workaround must stay"
        )


def test_schema_drift_error_tracks_installed_type_not_found_error() -> None:
    """SchemaDriftError subclasses the installed TypeNotFoundError and translates it."""
    original = TypeNotFoundError(0x1C32B11C, b"\x1c\xb12\x1c")

    assert isinstance(SchemaDriftError(original), TypeNotFoundError)

    translated = translate_schema_drift(original, operation="collect")
    assert isinstance(translated, SchemaDriftError)
    assert translated.invalid_constructor_id == 0x1C32B11C
    assert "0x1c32b11c" in str(translated)
    # Idempotent: an already-translated error passes through unchanged.
    assert translate_schema_drift(translated) is translated
