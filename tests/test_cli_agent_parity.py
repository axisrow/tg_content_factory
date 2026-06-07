"""Guard the documented CLI / Web / Agent parity contract."""

from __future__ import annotations

import ast
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _actual_agent_tools() -> set[str]:
    names: set[str] = set()
    for path in (ROOT / "src/agent/tools").glob("*.py"):
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


def _actual_web_routes() -> set[tuple[str, str]]:
    from src.web.app import create_app

    routes: set[tuple[str, str]] = set()
    for route in create_app().routes:
        path = getattr(route, "path", "")
        methods = getattr(route, "methods", set()) or set()
        for method in methods:
            if method not in {"HEAD", "OPTIONS"}:
                routes.add((method, path))
    return routes


def _documented_web_routes() -> list[tuple[str, str, str]]:
    routes: list[tuple[str, str, str]] = []
    for line in (ROOT / "docs/reference/parity.md").read_text(encoding="utf-8").splitlines():
        if not line.startswith("|") or line.startswith("|---") or "Операция" in line:
            continue
        cells = [cell.strip() for cell in line.strip("|").split("|")]
        if len(cells) < 4:
            continue
        operation = cells[0]
        web_cell = cells[2]
        if "исключение" in web_cell:
            continue
        for endpoint in re.findall(r"`([^`]+)`", web_cell):
            for method, path in _expand_web_endpoint(endpoint):
                if "*" not in path:
                    routes.append((operation, method, path.split("?", 1)[0]))
    return routes


def _expand_web_endpoint(endpoint: str) -> list[tuple[str, str]]:
    if endpoint.startswith("GET/POST "):
        path = endpoint.removeprefix("GET/POST ").strip()
        return [("GET", path), ("POST", path)]
    match = re.match(r"^(GET|POST|DELETE|PUT|PATCH)\s+(.+)$", endpoint)
    if match is None:
        return []
    return [(match.group(1), match.group(2).strip())]


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
    return all(
        actual == documented or _is_path_param(actual) or _is_path_param(documented)
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


def test_documented_web_endpoints_exist_in_app_routes():
    actual_routes = _actual_web_routes()
    missing_routes = [
        (operation, f"{method} {path}")
        for operation, method, path in _documented_web_routes()
        if not _route_exists(actual_routes, method, path)
    ]
    assert missing_routes == []
