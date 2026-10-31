"""Guard the documented CLI / Web / Agent parity contract."""

from __future__ import annotations

import ast
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _actual_agent_tools() -> set[str]:
    names: set[str] = set()
    for path in (ROOT / "tg_agent/agent/tools").glob("*.py"):
        if path.name.startswith("_") or path.name == "__init__.py":
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            for decorator in node.decorator_list:
                if (
                    isinstance(decorator, ast.Call)
                    and isinstance(decorator.func, ast.Name)
                    and decorator.func.id == "tool"
                    and decorator.args
                    and isinstance(decorator.args[0], ast.Constant)
                    and isinstance(decorator.args[0].value, str)
                ):
                    names.add(decorator.args[0].value)
    return names


def _parity_rows() -> list[tuple[str, str, list[str]]]:
    rows: list[tuple[str, str, list[str]]] = []
    for line in (ROOT / "docs/reference/parity.md").read_text(encoding="utf-8").splitlines():
        if not line.startswith("|") or line.startswith("|---") or "Операция" in line:
            continue
        cells = [cell.strip() for cell in line.strip("|").split("|")]
        if len(cells) < 4:
            continue
        agent_tools = re.findall(r"`([^`]+)`", cells[3])
        if agent_tools:
            rows.append((cells[0], cells[1], agent_tools))
    return rows


def _route_exists(actual_routes: set[tuple[str, str]], method: str, documented_path: str) -> bool:
    return any(
        actual_method == method and _paths_match(actual_path, documented_path)
        for actual_method, actual_path in actual_routes
    )


def _paths_match(actual_path: str, documented_path: str) -> bool:
    actual_parts = actual_path.strip("/").split("/") if actual_path.strip("/") else []
    documented_parts = documented_path.strip("/").split("/") if documented_path.strip("/") else []
    if len(actual_parts) != len(documented_parts):
        return False
    # A segment matches only when it is literally equal, or BOTH sides are path
    # params. Treating a single param segment as a wildcard (e.g. documented
    # {id} vs actual static "notifications") would let an unrelated static
    # sibling satisfy a removed dynamic route, false-greening the guard.
    return all(
        actual == documented or (_is_path_param(actual) and _is_path_param(documented))
        for actual, documented in zip(actual_parts, documented_parts, strict=True)
    )


def _is_path_param(part: str) -> bool:
    return part.startswith("{") and part.endswith("}")


def test_all_agent_tools_are_documented_in_parity_table():
    documented = {tool for _, _, tools in _parity_rows() for tool in tools}
    assert _actual_agent_tools() - documented == set()


def test_documented_agent_operations_have_cli_entrypoint():
    missing_cli = [
        (operation, tools)
        for operation, cli, tools in _parity_rows()
        if cli in {"", "—"}
    ]
    assert missing_cli == []


def test_path_param_does_not_match_unrelated_static_sibling():
    """A documented dynamic route must NOT be satisfied by an unrelated static
    sibling of the same shape. If POST /settings/{account_id}/delete regresses,
    the guard must fail rather than false-green against POST
    /settings/notifications/delete (param-vs-static segment is not a match)."""
    documented_dynamic = "/settings/{id}/delete"
    static_sibling_only = {("POST", "/settings/notifications/delete")}

    # The real dynamic endpoint is gone -> the guard must report it missing.
    assert not _route_exists(static_sibling_only, "POST", documented_dynamic)

    # A genuine param-to-param route still matches (different param name is fine).
    real_dynamic = {("POST", "/settings/{account_id}/delete")}
    assert _route_exists(real_dynamic, "POST", documented_dynamic)
