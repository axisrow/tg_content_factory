"""Scaffold contract tests for the tg-shared package (#1420, epic #1416).

The package lives at ``packages/tg_shared/`` inside this monorepo until the
owner splits it into its own repository. These tests pin the scaffold contract:

- the package imports standalone (no factory code needed to use it);
- the package never imports ``tg_agent`` / legacy ``src`` (automatic AST check);
- the storage Protocol exists and the factory ``Database`` satisfies it;
- the default single-messenger JSON store round-trips and survives a reopen;
- packaging markers: ``py.typed``, CHANGELOG with the first version, LICENSE.
"""

from __future__ import annotations

import ast
import subprocess
import sys
import tomllib
from datetime import UTC, datetime
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SHARED_ROOT = REPO_ROOT / "packages" / "tg_shared"
SHARED_SRC = SHARED_ROOT / "src"

# mutmut_skip: scans packages/tg_shared on disk — the tree is not part of the
# mutants sandbox (mutmut copies only tg_agent/ + tests/).
@pytest.mark.mutmut_skip
def test_shared_package_tree_and_packaging_markers() -> None:
    """#1420 AC: py.typed, changelog with the released versions, license present."""
    assert (SHARED_SRC / "tg_shared" / "__init__.py").is_file()
    assert (SHARED_SRC / "tg_shared" / "py.typed").is_file()
    changelog = (SHARED_ROOT / "CHANGELOG.md").read_text(encoding="utf-8")
    assert "0.1.0" in changelog
    assert "0.2.0" in changelog  # wave A module migration (#1421)
    assert (SHARED_ROOT / "LICENSE").is_file()


@pytest.mark.mutmut_skip
def test_shared_package_imports_standalone(tmp_path) -> None:
    """Import with only the shared source tree available and factory imports blocked."""
    metadata = tomllib.loads((SHARED_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    probe = """
import importlib.abc
import sys

class BlockFactoryImports(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split(".")[0] in {"tg_agent", "src"}:
            raise ImportError(f"factory dependency: {fullname}")

sys.meta_path.insert(0, BlockFactoryImports())
sys.path.insert(0, sys.argv[1])
import tg_shared
assert tg_shared.__version__ == sys.argv[2]
assert tg_shared.SharedAccount is not None
assert tg_shared.SharedAccountStore is not None
assert tg_shared.JsonFileStore is not None
"""
    result = subprocess.run(
        [sys.executable, "-I", "-c", probe, str(SHARED_SRC), metadata["project"]["version"]],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_shared_package_never_imports_factory() -> None:
    """#1420 AC: the package must not import factory code — checked automatically."""
    banned_roots = {"tg_agent", "src"}
    offenders: list[str] = []
    for path in sorted(SHARED_SRC.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                found = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom):
                found = [node.module or ""]
            else:
                continue
            offenders.extend(
                f"{path.relative_to(REPO_ROOT)}: {name}"
                for name in found
                if name.split(".")[0] in banned_roots
            )
    assert offenders == []


def test_shared_account_is_minimal() -> None:
    from tg_shared import SharedAccount

    account = SharedAccount(phone="+15550000000", session_string="fake-session-string")
    assert account.is_active is True
    assert account.flood_wait_until is None


def test_store_protocol_is_runtime_checkable() -> None:
    from tg_shared import JsonFileStore, SharedAccountStore

    assert isinstance(JsonFileStore("unused-path.json"), SharedAccountStore)


async def test_factory_database_satisfies_store_protocol(db) -> None:
    """The existing factory Database conforms trivially — the point of #1420.

    The Protocol deliberately mirrors the facade's existing method names
    (``get_accounts`` / ``update_account_flood`` / ``get_setting`` /
    ``set_setting``), so no adapter is needed on the factory side.
    """
    from tg_shared import SharedAccountStore

    assert isinstance(db, SharedAccountStore)
    await db.set_setting("tg_shared_scaffold_probe", "ok")
    assert await db.get_setting("tg_shared_scaffold_probe") == "ok"
    # Flood-write half of the contract runs against the real schema (no-op on
    # a phone that has no row, same as the factory's own UPDATE).
    await db.update_account_flood("+15550000000", None)


async def test_json_store_roundtrip_survives_reopen(tmp_path) -> None:
    """Default single-messenger store: no factory involved, persists across restart."""
    from tg_shared import JsonFileStore, SharedAccount

    path = tmp_path / "store.json"
    store = JsonFileStore(path)
    await store.add_account(SharedAccount(phone="+15550000000", session_string="fake-session-string"))
    await store.update_account_flood("+15550000000", datetime(2026, 10, 21, 12, 0, tzinfo=UTC))
    await store.set_setting("resolve_backoff_by_phone", "2026-10-21T00:00:00+00:00")

    reopened = JsonFileStore(path)
    accounts = await reopened.get_accounts()
    assert accounts[0].phone == "+15550000000"
    assert accounts[0].flood_wait_until == datetime(2026, 10, 21, 12, 0, tzinfo=UTC)
    assert await reopened.get_setting("resolve_backoff_by_phone") == "2026-10-21T00:00:00+00:00"


async def test_json_store_active_only_and_unknown_phone_noop(tmp_path) -> None:
    from tg_shared import JsonFileStore, SharedAccount

    store = JsonFileStore(tmp_path / "store.json")
    assert await store.get_accounts(active_only=True) == []

    await store.add_account(SharedAccount(phone="+15550000000", session_string="fake-a"))
    await store.add_account(SharedAccount(phone="+15550000001", session_string="fake-b", is_active=False))
    assert len(await store.get_accounts()) == 2
    assert [a.phone for a in await store.get_accounts(active_only=True)] == ["+15550000000"]

    # Unknown phone is a no-op (same semantics as the factory's UPDATE ... WHERE phone).
    await store.update_account_flood("+15559999999", datetime(2026, 10, 21, 12, 0, tzinfo=UTC))
    assert len(await store.get_accounts()) == 2


# mutmut_skip: scans packages/tg_shared on disk — not part of the mutants sandbox.
@pytest.mark.mutmut_skip
@pytest.mark.parametrize(
    "missing",
    ["__init__.py", "py.typed", "models.py", "storage.py", "json_store.py"],
)
def test_package_module_files_exist(missing: str) -> None:
    assert (SHARED_SRC / "tg_shared" / missing).is_file()
