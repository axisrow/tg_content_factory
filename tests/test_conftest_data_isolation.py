"""#1880: the suite runs with TG_AGENT_DATA_DIR outside the checkout.

The root conftest points data_root() at a per-process temp dir before any test
resolves paths, so pytest writes its logs, DBs and session files into temp —
never into the runtime data/ a daemon may be using in the same checkout. The
DB path itself comes from config (data/tg_search.db relative to cwd), which
tests isolate via chdir/tmp_path on their own."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

from tg_agent.paths import DATA_DIR_ENV, data_root


def test_data_root_points_outside_the_checkout_during_the_run():
    repo_data = Path(__file__).resolve().parent.parent / "data"
    override = os.environ.get(DATA_DIR_ENV)
    assert override, "the root conftest must set TG_AGENT_DATA_DIR"
    override_path = Path(override)
    assert override_path.is_absolute()
    assert override_path != repo_data, "tests must not write into the checkout's data/"
    assert data_root() == override_path
    # A per-process temp dir: xdist workers stay out of each other's files.
    assert override_path.parent == Path(tempfile.gettempdir())
