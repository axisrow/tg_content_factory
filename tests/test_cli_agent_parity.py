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
