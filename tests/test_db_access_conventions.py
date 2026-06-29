"""Structural guards for the database access boundary (#660).

`db.repos.<repo>.<method>()` is the canonical low-level DB access path. The
facade (`src/database/facade.py`) and bundles (`src/database/bundles.py`) expose
many mechanical pass-throughs that are kept as compatibility shims — but they
must not grow. These ratchet tests fail when the pass-through surface increases,
nudging new code toward `db.repos` (and focused services) instead.

The baselines are deliberately downward-only: when you delete a shim and migrate
its callers, lower the matching baseline so the ratchet keeps tightening.
"""

from __future__ import annotations

import re
from pathlib import Path

SRC_DIR = Path(__file__).resolve().parent.parent / "src"
FACADE = SRC_DIR / "database" / "facade.py"
BUNDLES = SRC_DIR / "database" / "bundles.py"

# Current size of the pass-through surface. These are CAPS, not exact counts:
# the guard prevents growth. Lower them whenever shims are removed.
FACADE_PASSTHROUGH_BASELINE = 88
# +2 for PhotoLoaderBundle.count_due_items / count_items_by_batch_status (#1152):
# PhotoTaskService / JobsReadModel consume the bundle, not db.repos, so the
# progress-count reads must be reachable through the bundle surface.
BUNDLE_METHOD_BASELINE = 138

# Every facade pass-through opens with `self._require()`; counting it is a
# robust, signature-shape-independent proxy for the pass-through surface.
_REQUIRE_RE = re.compile(r"self\._require\(\)")
# Bundle delegating methods, excluding the `from_database` constructors and dunders.
_BUNDLE_METHOD_RE = re.compile(r"^    (?:async )?def (?!from_database\b|__)\w+", re.MULTILINE)


def test_facade_passthrough_surface_does_not_grow():
    count = len(_REQUIRE_RE.findall(FACADE.read_text()))
    assert count <= FACADE_PASSTHROUGH_BASELINE, (
        f"facade.py pass-through count grew to {count} (baseline "
        f"{FACADE_PASSTHROUGH_BASELINE}).\n"
        "Do not add new `self._require(); return await self._<repo>...` pass-throughs — "
        "call db.repos.<repo>.<method>() directly. If you removed shims, lower the baseline."
    )


def test_bundle_method_surface_does_not_grow():
    count = len(_BUNDLE_METHOD_RE.findall(BUNDLES.read_text()))
    assert count <= BUNDLE_METHOD_BASELINE, (
        f"bundles.py method count grew to {count} (baseline {BUNDLE_METHOD_BASELINE}).\n"
        "Prefer db.repos or a focused service over a new bundle pass-through. "
        "If you removed methods, lower the baseline."
    )
