"""A refusal is not raised where a broad `except` catches it (#1974).

`cli_error()` raises `typer.Exit` — an `Exception` subclass. Raised inside a
`try` whose handlers include `except Exception` (or a bare `except`, or
`BaseException`) and no handler that lets `typer.Exit` through, it is caught
there and reported a second time as a generic ``Error …: 1`` next to the real
reason. The convention is the older one (`messages_read_impl`, #1486): such a
`try` carries ``except typer.Exit: raise`` before the broad handler — or the
refusal is raised after the handlers. This test names any site that has neither.
"""
from __future__ import annotations

import ast
import pathlib
from textwrap import dedent

_CLI_ROOT = pathlib.Path(__file__).resolve().parents[1] / "tg_agent" / "cli"


def _is_broad(handler: ast.ExceptHandler) -> bool:
    if handler.type is None:
        return True
    names = [node.id for node in ast.walk(handler.type) if isinstance(node, ast.Name)]
    return any(name in {"Exception", "BaseException"} for name in names)


def _lets_exit_through(node: ast.Try) -> bool:
    return any(
        handler.type is not None and "Exit" in ast.unparse(handler.type)
        for handler in node.handlers
    )


def _is_refusal(node: ast.AST) -> bool:
    if isinstance(node, ast.Call):
        return getattr(node.func, "id", None) == "cli_error"
    if isinstance(node, ast.Raise) and isinstance(node.exc, ast.Call):
        return getattr(node.exc.func, "attr", None) == "Exit"
    return False


def _unguarded_tries(tree: ast.AST):
    for node in ast.walk(tree):
        if not isinstance(node, ast.Try):
            continue
        if any(_is_broad(handler) for handler in node.handlers) and not _lets_exit_through(node):
            yield node


def _swallowed_refusals(source: str) -> list[int]:
    tree = ast.parse(source)
    return [
        inner.lineno
        for guarded in _unguarded_tries(tree)
        for inner in ast.walk(ast.Module(body=guarded.body, type_ignores=[]))
        if _is_refusal(inner)
    ]


def test_no_refusal_is_raised_inside_a_try_with_a_broad_handler():
    offenders = []
    for path in sorted(_CLI_ROOT.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for guarded in _unguarded_tries(tree):
            for inner in ast.walk(ast.Module(body=guarded.body, type_ignores=[])):
                if _is_refusal(inner):
                    offenders.append(f"{path.relative_to(_CLI_ROOT.parent.parent)}:{inner.lineno}")
    assert offenders == []


def test_the_scan_sees_a_swallowed_refusal():
    """The guard is not vacuous: each shape below is a double report in the making."""
    swallowed = [
        # raised in the guarded body
        """
        def f():
            try:
                cli_error('x')
            except Exception as exc:
                cli_error(str(exc))
        """,
        # raised in a nested handler — the outer broad one catches it again
        """
        def g():
            try:
                try:
                    boom()
                except ValueError:
                    cli_error('x')
            except Exception:
                pass
        """,
        # raised in a nested try's finally
        """
        def h():
            try:
                try:
                    boom()
                finally:
                    cli_error('x')
            except Exception:
                pass
        """,
        # a bare except
        """
        def i():
            try:
                raise typer.Exit(1)
            except:
                pass
        """,
    ]
    for index, source in enumerate(swallowed):
        assert _swallowed_refusals(dedent(source)), f"shape {index} was not seen"


def test_the_scan_passes_the_two_allowed_shapes():
    allowed = [
        # the older convention: Exit passes through before the broad handler
        """
        def f():
            try:
                cli_error('x')
            except typer.Exit:
                raise
            except Exception as exc:
                cli_error(str(exc))
        """,
        # the refusal after the handlers
        """
        def g():
            try:
                result = work()
            except Exception as exc:
                cli_error(str(exc))
            if result is None:
                cli_error('x')
        """,
    ]
    for index, source in enumerate(allowed):
        assert _swallowed_refusals(dedent(source)) == [], f"shape {index} was flagged"
