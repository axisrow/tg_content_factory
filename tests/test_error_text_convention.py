"""A stored failure always carries a reason (#1968).

An exception raised without a message — ``asyncio.TimeoutError()``, a bare
``RuntimeError()`` — has an empty ``str()``. Every failure path that stores the
exception's text goes through :func:`tg_agent.utils.errors.error_text`, and the
convention test below names any new site that stores a raw ``str(exc)``.
"""
from __future__ import annotations

import ast
import asyncio
import pathlib

from tg_agent.utils.errors import error_text

_PACKAGE = pathlib.Path(__file__).resolve().parents[1] / "tg_agent"
_REASON_KEYWORDS = {"error", "error_text", "fallback_error"}


def test_error_text_is_the_message():
    assert error_text(ValueError("bad id")) == "bad id"


def test_error_text_names_the_class_when_the_exception_has_no_message():
    assert error_text(asyncio.TimeoutError()) == "TimeoutError"
    assert error_text(RuntimeError("   ")) == "RuntimeError"


def test_error_text_is_capped():
    assert len(error_text(ValueError("x" * 900))) == 500


def _is_bare_str(node: ast.AST) -> bool:
    """``str(exc)`` — alone, sliced, or as an operand of ``note or str(exc)``."""
    if isinstance(node, ast.Subscript):
        node = node.value
    if isinstance(node, ast.BoolOp):
        return any(_is_bare_str(value) for value in node.values)
    return (
        isinstance(node, ast.Call)
        and getattr(node.func, "id", None) == "str"
        and len(node.args) == 1
        and isinstance(node.args[0], ast.Name)
    )


def test_no_failure_path_stores_a_bare_str_of_the_exception():
    offenders = []
    for path in sorted(_PACKAGE.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            for keyword in node.keywords:
                if keyword.arg in _REASON_KEYWORDS and _is_bare_str(keyword.value):
                    offenders.append(f"{path.relative_to(_PACKAGE.parent)}:{node.lineno}")
    assert offenders == []
