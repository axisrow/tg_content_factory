"""End-to-end regression for the cross-file fixture leak (#1005).

Symptom: running a few route-test files together with a foreign-package file
*interleaved* between them — without ``--dist=loadfile`` to shard files across
workers — failed every test in the re-entered route file with
``fixture 'route_client' not found``, even though each file passes in isolation.

Root cause: pytest collects one ``Package`` collector per directory in the order
its args first appear. Interleaved sibling-package args
(``tests/routes/a.py tests/b.py tests/routes/c.py``) make it enter, leave, then
re-enter ``tests/routes`` — and on re-entry that package's ``conftest.py``
fixtures are not re-bound to the new nodes. CI dodged this because
``--dist=loadfile`` keeps a package's files on one worker; a plain local mixed
run did not.

Fix: ``pytest_collection`` in the root ``conftest.py`` groups same-directory
args so each package is collected exactly once. This test reproduces the exact
issue invocation in a subprocess and asserts it now collects + runs cleanly.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[1]

# The exact interleaving from the issue: a foreign-package file (notifier, in
# tests/) splitting two tests/routes/ files. This ordering is what triggered the
# 26 setup errors before the fix.
_INTERLEAVED_ARGS = [
    "tests/routes/test_agent_lazyload.py",
    "tests/test_notifier_delivery_paths.py",
    "tests/routes/test_analytics_routes_channel_trends.py",
]


@pytest.mark.timeout(90)
def test_interleaved_packages_collect_without_fixture_leak() -> None:
    """The issue repro must run green now that pytest_collection de-interleaves.

    Run in a subprocess so the real collection tree is rebuilt with the
    interleaved arg order — the leak lives in collection, not in this process.
    No ``-p no:randomly``: pytest_collection regroups *before* collection, so the
    tree is built correctly regardless of any later item shuffling, and the test
    must hold whether or not pytest-randomly is installed.
    """
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            *_INTERLEAVED_ARGS,
            "-m",
            "not aiosqlite_serial",
            # Strip the project's --dist=loadfile so the leak isn't masked by
            # file-sharding — this is precisely the unguarded local path.
            "-o",
            "addopts=",
            "-q",
        ],
        cwd=_REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=80,
    )
    combined = result.stdout + result.stderr
    assert "fixture 'route_client' not found" not in combined, combined
    # Before the fix the summary line read "47 passed, 26 errors in ...".
    assert "error" not in combined, combined
    assert result.returncode == 0, combined
