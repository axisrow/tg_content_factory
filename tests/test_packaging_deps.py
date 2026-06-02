"""Guard the dependency split between runtime and dev tooling (#633 bug #32)."""

from __future__ import annotations

import re
import tomllib
from pathlib import Path

_PYPROJECT = Path(__file__).resolve().parent.parent / "pyproject.toml"

# Pure dev/test tooling that must never ship as a runtime dependency.
_DEV_ONLY = {"pytest", "pytest-timeout", "pytest-xdist", "ruff", "pytest-cov"}
_DIST_NAME_RE = re.compile(r"^\s*([A-Za-z0-9][A-Za-z0-9._-]*)")


def _dep_names(requirements: list[str]) -> set[str]:
    """Extract bare package names from PEP 508 requirement strings."""
    names: set[str] = set()
    for req in requirements:
        match = _DIST_NAME_RE.match(req)
        assert match is not None, f"could not parse requirement name from {req!r}"
        names.add(re.sub(r"[-_.]+", "-", match.group(1)).lower())
    return names


def _load_pyproject() -> dict:
    with _PYPROJECT.open("rb") as fh:
        return tomllib.load(fh)


def test_dep_names_extracts_pep508_distribution_names():
    requirements = [
        "pytest!=9.0.0",
        "ruff===0.15.14",
        "pytest_timeout[testing]>=2.4.0; python_version >= '3.11'",
        "pytest.xdist @ https://example.invalid/pytest-xdist.tar.gz",
    ]

    assert _dep_names(requirements) == {"pytest", "ruff", "pytest-timeout", "pytest-xdist"}


def test_dev_tooling_not_in_runtime_dependencies():
    data = _load_pyproject()
    runtime = _dep_names(data["project"]["dependencies"])
    leaked = _DEV_ONLY & runtime
    assert not leaked, f"dev tooling leaked into runtime dependencies: {sorted(leaked)}"


def test_dev_tooling_present_in_dev_extra():
    data = _load_pyproject()
    dev = _dep_names(data["project"]["optional-dependencies"]["dev"])
    missing = _DEV_ONLY - dev
    assert not missing, f"dev tooling missing from [dev] extra: {sorted(missing)}"


def test_httpx_stays_a_runtime_dependency():
    # httpx is imported at runtime (src/agent/tools/images.py), so it must remain
    # a main dependency, not be moved into the dev extra.
    data = _load_pyproject()
    runtime = _dep_names(data["project"]["dependencies"])
    assert "httpx" in runtime
