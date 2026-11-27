"""A stored failure always carries a reason (#1968).

An exception raised without a message — ``asyncio.TimeoutError()``, a bare
``RuntimeError()`` — has an empty ``str()``. Every failure path that stores the
exception's text goes through :func:`tg_agent.utils.errors.error_text`, and the
convention test below holds the rule a linter can check: inside an
``except ... as exc`` handler, ``exc`` reaches a reason name only through
``error_text(exc)`` — not as ``str(exc)`` however wrapped or sliced, not
interpolated into an f-string, and not through a local assigned either of
those.

A reason name is a reason keyword (``error=``, ``error_text=``,
``fallback_error=``, and the result fields ``unavailable=``/``username_error=``
— #1982) or a dict-literal key of the same names (an event payload such as
``{"error": str(exc)}`` — #1982).

Not seen, by design: a reason passed positionally (``update_item(id, FAILED,
text, owner)`` — pass ``error=``), and a helper that takes the exception as a
parameter.
"""
from __future__ import annotations

import ast
import asyncio
import pathlib
import textwrap
from typing import cast

import pytest

from tg_agent.utils.errors import error_text

_PACKAGE = pathlib.Path(__file__).resolve().parents[1] / "tg_agent"
_REASON_KEYWORDS = {"error", "error_text", "fallback_error", "unavailable", "username_error"}


def test_error_text_is_the_message():
    assert error_text(ValueError("bad id")) == "bad id"


def test_error_text_names_the_class_when_the_exception_has_no_message():
    assert error_text(asyncio.TimeoutError()) == "TimeoutError"
    assert error_text(RuntimeError("   ")) == "RuntimeError"


def test_error_text_is_capped():
    assert len(error_text(ValueError("x" * 900))) == 500


def _is_str_of(node: ast.AST, name: str) -> bool:
    """``str(name)``."""
    return (
        isinstance(node, ast.Call)
        and getattr(node.func, "id", None) == "str"
        and len(node.args) == 1
        and isinstance(node.args[0], ast.Name)
        and node.args[0].id == name
    )


def _is_name(node: ast.AST, name: str) -> bool:
    return isinstance(node, ast.Name) and node.id == name


def _interpolates(node: ast.AST, name: str) -> bool:
    """An f-string with ``{name}`` or ``{str(name)}`` in it."""
    return isinstance(node, ast.JoinedStr) and any(
        isinstance(part, ast.FormattedValue)
        and (_is_name(part.value, name) or _is_str_of(part.value, name))
        for part in node.values
    )


def _takes_text_raw(expr: ast.AST, name: str) -> bool:
    """``expr`` turns ``name`` into text without :func:`error_text`:
    ``str(name)`` anywhere in it, or an f-string interpolating it."""
    return any(_is_str_of(node, name) or _interpolates(node, name) for node in ast.walk(expr))


def _raw_reason_lines(handler: ast.ExceptHandler) -> list[int]:
    """Lines of ``handler`` where its bound exception reaches a reason name raw."""
    name = handler.name
    if name is None:
        return []
    # ponytail: a local is raw if any assignment in the handler makes it so — statement order,
    # annotated and walrus assignments are not tracked
    tainted = {
        target.id
        for node in ast.walk(handler)
        if isinstance(node, ast.Assign) and _takes_text_raw(node.value, name)
        for target in node.targets
        if isinstance(target, ast.Name)
    }
    return [
        node.lineno
        for node in ast.walk(handler)
        if (
            isinstance(node, ast.Call)
            and any(
                keyword.arg in _REASON_KEYWORDS
                and (
                    _takes_text_raw(keyword.value, name)
                    or (isinstance(keyword.value, ast.Name) and keyword.value.id in tainted)
                )
                for keyword in node.keywords
            )
        )
        or (
            isinstance(node, ast.Dict)
            and any(
                isinstance(key, ast.Constant)
                and key.value in _REASON_KEYWORDS
                and (
                    _takes_text_raw(value, name)
                    or (isinstance(value, ast.Name) and value.id in tainted)
                )
                for key, value in zip(node.keys, node.values, strict=False)  # ponytail: AST builds them in lockstep
            )
        )
    ]


def test_exception_reaches_a_reason_keyword_only_through_error_text():
    offenders = []
    for path in sorted(_PACKAGE.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.ExceptHandler):
                offenders += [
                    f"{path.relative_to(_PACKAGE.parent)}:{line}" for line in _raw_reason_lines(node)
                ]
    assert offenders == []


def _handler(body: str) -> ast.ExceptHandler:
    """The ``except Exception as exc`` handler whose body is ``body``."""
    source = textwrap.dedent("""\
        try:
            work()
        except Exception as exc:
        """) + textwrap.indent(textwrap.dedent(body), " " * 4)
    return cast(ast.Try, ast.parse(source).body[0]).handlers[0]


_RED = {
    "bare": "update(1, error=str(exc))",
    "sliced": "update(1, error=str(exc)[:500])",
    "or-note": "update(1, error=note or str(exc)[:500])",
    "stripped": "update(1, error=str(exc).strip())",
    "f-string": "update(1, error=f'partial send failure: {exc}')",
    "local": "error = str(exc)\nupdate(1, error=error)",
    "local-branch": (
        "error = describe(exc) if isinstance(exc, ValueError) else str(exc)\nupdate(1, error=error)"
    ),
    "dict-key": "report({'event': 'error', 'error': str(exc)})",
    "dict-key-local": "error = str(exc)\nreport({'error': error})",
    "result-field": "return ReactionUsersResult([], unavailable=str(exc))",
    "result-field-local": "username_error = str(exc)\nreturn Result(username_error=username_error)",
}


@pytest.mark.parametrize("body", _RED.values(), ids=_RED.keys())
def test_raw_reason_lines_names_each_raw_form(body):
    handler = _handler(body)
    assert _raw_reason_lines(handler) == [handler.lineno + body.count("\n") + 1]


_GREEN = {
    "helper": "update(1, error=error_text(exc))",
    "helper-in-f-string": "update(1, error=f'partial send failure: {error_text(exc)}')",
    "local-from-helper": "reason = error_text(exc)\nupdate(1, error=reason)",
    "other-name-in-f-string": "update(1, error=f'channel {channel_id} not found')",
    "not-a-reason-keyword": "update(1, note=f'failed: {exc}')",
    "raw-text-not-stored": "message = str(exc)\nlog(message)",
    "dict-key-helper": "report({'event': 'error', 'error': error_text(exc)})",
    "dict-key-other-value": "report({'status': 'error', 'error': 'gone'})",
    "result-field-helper": "return ReactionUsersResult([], unavailable=error_text(exc))",
}


@pytest.mark.parametrize("body", _GREEN.values(), ids=_GREEN.keys())
def test_raw_reason_lines_passes_the_guarded_spelling(body):
    assert _raw_reason_lines(_handler(body)) == []
