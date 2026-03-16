from __future__ import annotations

import tomllib
from pathlib import Path


def test_packaging_config_keeps_src_namespace_for_console_script() -> None:
    data = tomllib.loads(Path("pyproject.toml").read_text())

    assert data["project"]["name"] == "tg-agent"
    assert "tg-search" not in data["project"]["scripts"]
    assert data["project"]["scripts"]["tg-agent"] == "src.main:main"
    assert data["tool"]["setuptools"]["packages"]["find"]["where"] == ["."]
    assert data["tool"]["setuptools"]["packages"]["find"]["include"] == ["src*"]
    assert data["tool"]["setuptools"]["package-data"]["src"] == [
        "web/templates/*.html",
        "web/static/*.css",
    ]
