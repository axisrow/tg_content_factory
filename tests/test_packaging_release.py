from __future__ import annotations

import subprocess
import sys
import tomllib
from pathlib import Path
from zipfile import ZipFile

import pytest

ROOT = Path(__file__).resolve().parent.parent


def test_packaging_config_keeps_tg_agent_namespace_for_console_script() -> None:
    data = tomllib.loads(Path("pyproject.toml").read_text())

    assert data["project"]["name"] == "tg-agent"
    assert "tg-search" not in data["project"]["scripts"]
    assert data["project"]["scripts"]["tg-agent"] == "tg_agent.main:main"
    assert data["tool"]["setuptools"]["packages"]["find"]["where"] == ["."]
    assert data["tool"]["setuptools"]["packages"]["find"]["include"] == ["tg_agent*"]
    assert data["tool"]["setuptools"]["package-data"]["tg_agent"] == [
        "web/templates/*.html",
        "web/templates/**/*.html",
        "web/static/*.css",
        "web/static/*.js",
        "web/static/js/*.js",
        "cli/commands/*.tcss",
        "database/seeds/*.csv",
    ]


def _build_wheel(tmp_path: Path) -> Path:
    """Build a wheel offline with the declared backend installed by the dev extra."""
    subprocess.run(
        [
            sys.executable, "-m", "pip", "wheel", "--no-deps", "--no-build-isolation", "--no-index",
            "-w", str(tmp_path), str(ROOT),
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=110,
    )
    wheels = list(tmp_path.glob("tg_agent-*.whl"))
    assert len(wheels) == 1, f"expected exactly one tg_agent wheel, got {wheels}"
    return wheels[0]


@pytest.mark.integration
@pytest.mark.mutmut_skip
def test_wheel_ships_tg_agent_package_not_src(tmp_path: Path) -> None:
    """#1448 regression guard: the wheel ships the tg_agent package, not src.

    The distribution is named tg-agent; shipping a top-level ``src`` package made
    the import name collide with any other project's ``src`` and produced the
    ``tg-agent --version`` -> ``src 0.2.3`` confusion. Fails if the packaged
    import name ever drifts back to something generic.

    Explicit level/mutation markers: this spawns ``pip wheel`` out-of-process
    using the locally installed build backend — outside the unit contract and
    excluded from mutation runs (mutmut_skip semantics).
    """
    wheel = _build_wheel(tmp_path)

    with ZipFile(wheel) as zf:
        names = zf.namelist()
        dist_info_meta = [n for n in names if n.endswith(".dist-info/METADATA")]
        assert len(dist_info_meta) == 1, f"dist-info METADATA not found in {names[:5]}"
        metadata = zf.read(dist_info_meta[0]).decode("utf-8")
        entry_points = zf.read(
            dist_info_meta[0].replace("METADATA", "entry_points.txt")
        ).decode("utf-8")

    assert "tg_agent/__init__.py" in names
    # ponytail: the literal below names the OLD package on purpose — do not sweep it
    assert not any(n == "src" or n.startswith("src/") for n in names), (
        "wheel must not ship a top-level src package (#1448)"
    )
    # Declared package-data must actually make it into the wheel.
    assert any(
        n.startswith("tg_agent/web/templates/") and n.endswith(".html") for n in names
    ), "web templates missing from wheel — package-data mapping broke"
    source_templates = {
        path.relative_to(ROOT).as_posix()
        for path in (ROOT / "tg_agent" / "web" / "templates").rglob("*.html")
    }
    missing_templates = source_templates - set(names)
    assert not missing_templates, f"nested web templates missing from wheel: {sorted(missing_templates)}"
    assert "Name: tg-agent" in metadata
    assert "tg-agent = tg_agent.main:main" in entry_points
