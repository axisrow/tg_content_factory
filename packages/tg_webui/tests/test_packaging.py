"""tg-webui wheel ships the panel's templates and static assets (#1720 wave 3)."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from zipfile import ZipFile

import pytest

PACKAGE_ROOT = Path(__file__).resolve().parent.parent
SOURCE = PACKAGE_ROOT / "src" / "tg_webui"


@pytest.mark.integration
@pytest.mark.mutmut_skip
def test_wheel_ships_templates_and_static(tmp_path: Path) -> None:
    """Declared package-data must actually make it into the tg-webui wheel."""
    subprocess.run(
        [
            sys.executable, "-m", "pip", "wheel", "--no-deps", "--no-build-isolation", "--no-index",
            "-w", str(tmp_path), str(PACKAGE_ROOT),
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=110,
    )
    wheels = list(tmp_path.glob("tg_webui-*.whl"))
    assert len(wheels) == 1, f"expected exactly one tg_webui wheel, got {wheels}"

    with ZipFile(wheels[0]) as zf:
        names = set(zf.namelist())

    assert "tg_webui/app.py" in names
    expected = {
        "tg_webui/" + path.relative_to(SOURCE).as_posix()
        for pattern in ("templates/**/*.html", "static/**/*.css", "static/**/*.js")
        for path in SOURCE.glob(pattern)
    }
    assert expected, "no templates/static found in the package source"
    missing = expected - names
    assert not missing, f"templates/static missing from the tg-webui wheel: {sorted(missing)}"
