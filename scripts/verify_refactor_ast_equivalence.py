#!/usr/bin/env python3
"""AST guard for the #1131 batch-1 complexity refactor.

The script compares the original rank-E functions from a git base ref with the
current extracted helper composition. It intentionally checks AST shapes, not
runtime output, so it can catch accidental rewrites in refactor-only changes.
"""

from __future__ import annotations

import argparse
import ast
import copy
import difflib
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class AstMismatch(AssertionError):
    pass


class ReplaceLoads(ast.NodeTransformer):
    def __init__(self, replacements: dict[str, ast.expr]) -> None:
        self._replacements = replacements

    def visit_Name(self, node: ast.Name) -> ast.AST:
        if isinstance(node.ctx, ast.Load) and node.id in self._replacements:
            replacement = copy.deepcopy(self._replacements[node.id])
            return ast.copy_location(replacement, node)
        return node


def _parse_expr(source: str) -> ast.expr:
    return ast.parse(source, mode="eval").body


def _base_source(base_ref: str, path: str) -> str:
    return subprocess.check_output(["git", "show", f"{base_ref}:{path}"], cwd=ROOT, text=True)


def _current_source(path: str) -> str:
    return (ROOT / path).read_text()


def _tree(source: str) -> ast.Module:
    return ast.parse(source)


def _function(tree: ast.AST, name: str, *, class_name: str | None = None) -> ast.FunctionDef | ast.AsyncFunctionDef:
    scope = tree
    if class_name is not None:
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef) and node.name == class_name:
                scope = node
                break
        else:
            raise AstMismatch(f"class {class_name!r} not found")

    for node in ast.iter_child_nodes(scope):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name:
            return node
    raise AstMismatch(f"function {name!r} not found")


def _dump(node: ast.AST | list[ast.stmt]) -> str:
    if isinstance(node, list):
        node = ast.Module(body=node, type_ignores=[])
    ast.fix_missing_locations(node)
    return ast.dump(node, include_attributes=False, indent=2)


def _unparse(node: ast.AST | list[ast.stmt]) -> str:
    if isinstance(node, list):
        node = ast.Module(body=node, type_ignores=[])
    ast.fix_missing_locations(node)
    return ast.unparse(node)


def _assert_same(label: str, left: ast.AST | list[ast.stmt], right: ast.AST | list[ast.stmt]) -> None:
    left_dump = _dump(left)
    right_dump = _dump(right)
    if left_dump == right_dump:
        print(f"OK {label}")
        return
    diff = "\n".join(
        difflib.unified_diff(
            _unparse(left).splitlines(),
            _unparse(right).splitlines(),
            fromfile=f"{label}: expected",
            tofile=f"{label}: actual",
            lineterm="",
        )
    )
    raise AstMismatch(f"{label} AST mismatch\n{diff}")


def _clone_statements(statements: list[ast.stmt]) -> list[ast.stmt]:
    return copy.deepcopy(statements)


def _body_without_return(func: ast.FunctionDef | ast.AsyncFunctionDef) -> list[ast.stmt]:
    body = _clone_statements(func.body)
    if body and isinstance(body[-1], ast.Return):
        return body[:-1]
    return body


def _replace_loads_in_statements(statements: list[ast.stmt], replacements: dict[str, ast.expr]) -> list[ast.stmt]:
    cloned = _clone_statements(statements)
    return [ReplaceLoads(replacements).visit(stmt) for stmt in cloned]  # type: ignore[list-item]


def _return_to_assign(
    func: ast.FunctionDef | ast.AsyncFunctionDef,
    target: str,
    replacements: dict[str, ast.expr] | None = None,
) -> list[ast.stmt]:
    statements = _clone_statements(func.body)
    converted: list[ast.stmt] = []
    for stmt in statements:
        if isinstance(stmt, ast.Return):
            converted.append(ast.Assign(targets=[ast.Name(id=target, ctx=ast.Store())], value=stmt.value))
        else:
            converted.append(stmt)
    if replacements:
        converted = [ReplaceLoads(replacements).visit(stmt) for stmt in converted]  # type: ignore[list-item]
    return converted


def _return_value(func: ast.FunctionDef | ast.AsyncFunctionDef) -> ast.expr:
    final = func.body[-1]
    if not isinstance(final, ast.Return) or final.value is None:
        raise AstMismatch(f"{func.name} does not end with a value return")
    return copy.deepcopy(final.value)


def _runtime_snapshot_payload(stmt: ast.stmt) -> ast.expr:
    if not isinstance(stmt, ast.Expr) or not isinstance(stmt.value, ast.Await):
        raise AstMismatch("expected awaited upsert_snapshot expression")
    call = stmt.value.value
    if not isinstance(call, ast.Call) or not call.args:
        raise AstMismatch("expected upsert_snapshot call")
    snapshot = call.args[0]
    if not isinstance(snapshot, ast.Call):
        raise AstMismatch("expected RuntimeSnapshot call")
    for keyword in snapshot.keywords:
        if keyword.arg == "payload":
            return copy.deepcopy(keyword.value)
    raise AstMismatch("RuntimeSnapshot payload keyword not found")


def _runtime_snapshot_type(stmt: ast.stmt) -> str:
    if not isinstance(stmt, ast.Expr) or not isinstance(stmt.value, ast.Await):
        raise AstMismatch("expected awaited upsert_snapshot expression")
    call = stmt.value.value
    if not isinstance(call, ast.Call) or not call.args:
        raise AstMismatch("expected upsert_snapshot call")
    snapshot = call.args[0]
    if not isinstance(snapshot, ast.Call):
        raise AstMismatch("expected RuntimeSnapshot call")
    for keyword in snapshot.keywords:
        if keyword.arg == "snapshot_type" and isinstance(keyword.value, ast.Constant):
            return str(keyword.value.value)
    raise AstMismatch("RuntimeSnapshot snapshot_type keyword not found")


def _append_call_arg(stmt: ast.stmt) -> ast.expr:
    if not isinstance(stmt, ast.Expr) or not isinstance(stmt.value, ast.Call):
        raise AstMismatch("expected append expression")
    call = stmt.value
    if not call.args:
        raise AstMismatch("expected append call argument")
    return copy.deepcopy(call.args[0])


def _assert_current_calls(label: str, func: ast.FunctionDef | ast.AsyncFunctionDef, expected: list[str]) -> None:
    found: list[str] = []
    for stmt in func.body:
        expr: ast.AST | None = None
        if isinstance(stmt, ast.Expr):
            expr = stmt.value.value if isinstance(stmt.value, ast.Await) else stmt.value
        elif isinstance(stmt, ast.Assign):
            expr = stmt.value.value if isinstance(stmt.value, ast.Await) else stmt.value
        elif isinstance(stmt, ast.Return):
            expr = stmt.value.value if isinstance(stmt.value, ast.Await) else stmt.value
        if isinstance(expr, ast.Call):
            called = expr.func
            if isinstance(called, ast.Name) and called.id.startswith("_"):
                found.append(called.id)
            elif isinstance(called, ast.Attribute) and called.attr.startswith("_"):
                found.append(called.attr)
    if found != expected:
        raise AstMismatch(f"{label} helper call order changed: expected {expected}, got {found}")
    print(f"OK {label} helper call order")


def verify_worker(base_ref: str) -> None:
    base = _function(_tree(_base_source(base_ref, "src/runtime/worker.py")), "_publish_snapshots")
    current_tree = _tree(_current_source("src/runtime/worker.py"))
    current = _function(current_tree, "_publish_snapshots")
    _assert_current_calls(
        "worker _publish_snapshots",
        current,
        [
            "_load_active_accounts",
            "_resolve_available_phones",
            "_resolve_pool_warming",
            "_resolve_backoffs",
            "_publish_worker_heartbeat_snapshot",
            "_publish_accounts_status_snapshot",
            "_publish_pool_counters_snapshot",
            "_publish_collector_status_snapshot",
            "_publish_scheduler_status_snapshot",
            "_publish_scheduler_jobs_snapshot",
            "_publish_collection_queue_status_snapshot",
            "_publish_notification_target_status_snapshot",
        ],
    )

    _assert_same("worker kept setup", base.body[0:2], current.body[0:2])
    _assert_same("worker accounts load", base.body[2:4], _body_without_return(_function(current_tree, "_load_active_accounts")))
    _assert_same(
        "worker available phones",
        base.body[4:7],
        _body_without_return(_function(current_tree, "_resolve_available_phones")),
    )
    _assert_same(
        "worker pool warming",
        base.body[7:11],
        _return_to_assign(
            _function(current_tree, "_resolve_pool_warming"),
            "is_warming",
            {"pool": _parse_expr("container.pool")},
        ),
    )
    _assert_same(
        "worker resolve backoffs",
        base.body[11:14],
        _replace_loads_in_statements(
            _body_without_return(_function(current_tree, "_resolve_backoffs")),
            {"pool": _parse_expr("container.pool")},
        ),
    )
    _assert_same("worker heartbeat snapshot", [base.body[14]], _function(current_tree, "_publish_worker_heartbeat_snapshot").body)
    _assert_same("worker accounts snapshot", [base.body[15]], _function(current_tree, "_publish_accounts_status_snapshot").body)

    pool_payload_func = _function(current_tree, "_pool_counters_payload")
    _assert_same("worker pool counter locals", base.body[17:21], pool_payload_func.body[:-1])
    _assert_same("worker pool counter payload", _runtime_snapshot_payload(base.body[21]), _return_value(pool_payload_func))
    _assert_same("worker collector snapshot", [base.body[22]], _function(current_tree, "_publish_collector_status_snapshot").body)
    _assert_same("worker scheduler status snapshot", [base.body[23]], _function(current_tree, "_publish_scheduler_status_snapshot").body)
    _assert_same("worker scheduler jobs snapshot", base.body[24:27], _function(current_tree, "_publish_scheduler_jobs_snapshot").body)

    queue_payload_func = _function(current_tree, "_collection_queue_status_payload")
    _assert_same("worker queue locals", base.body[27:31], queue_payload_func.body[:-1])
    _assert_same("worker queue payload", _runtime_snapshot_payload(base.body[31]), _return_value(queue_payload_func))

    notification_payload_func = _function(current_tree, "_notification_target_status_payload")
    _assert_same("worker notification locals", base.body[32:35], notification_payload_func.body[:-1])
    _assert_same("worker notification payload", _runtime_snapshot_payload(base.body[35]), _return_value(notification_payload_func))

    notification_snapshot = _function(current_tree, "_publish_notification_target_status_snapshot").body[0]
    if _runtime_snapshot_type(notification_snapshot) != "notification_target_status":
        raise AstMismatch("worker notification snapshot_type changed")
    print("OK worker notification snapshot_type")


def verify_analyzer(base_ref: str) -> None:
    base = _function(_tree(_base_source(base_ref, "src/filters/analyzer.py")), "_build_report", class_name="ChannelAnalyzer")
    current_tree = _tree(_current_source("src/filters/analyzer.py"))
    current = _function(current_tree, "_build_report", class_name="ChannelAnalyzer")
    _assert_current_calls(
        "analyzer _build_report",
        current,
        [
            "_fetch_channels_for_report",
            "_fetch_analysis_maps",
            "_load_min_subscribers_filter",
            "_build_channel_results",
            "_filter_report_from_results",
        ],
    )

    class_name = "ChannelAnalyzer"
    _assert_same("analyzer timer setup", [base.body[0]], [current.body[0]])
    _assert_same(
        "analyzer channel fetch",
        base.body[1:3],
        _replace_loads_in_statements(
            _body_without_return(_function(current_tree, "_fetch_channels_for_report", class_name=class_name)),
            {"started_at": _parse_expr("t0")},
        ),
    )
    _assert_same("analyzer empty report branch", [base.body[3]], [current.body[2]])
    _assert_same(
        "analyzer map fetch",
        [base.body[4]],
        _body_without_return(_function(current_tree, "_fetch_analysis_maps", class_name=class_name)),
    )
    _assert_same("analyzer min subscribers", [base.body[5]], [current.body[4]])

    original_loop = base.body[7]
    if not isinstance(original_loop, ast.For):
        raise AstMismatch("expected original analyzer result loop")
    loop_body = original_loop.body
    result_func = _function(current_tree, "_build_channel_result", class_name=class_name)
    _assert_same("analyzer result locals", loop_body[0:3], result_func.body[0:3])
    _assert_same(
        "analyzer uniqueness block",
        loop_body[3:7],
        _body_without_return(_function(current_tree, "_append_uniqueness_flag", class_name=class_name)),
    )
    _assert_same("analyzer subscriber count lookup", [loop_body[9]], [result_func.body[4]])
    _assert_same(
        "analyzer subscriber block",
        loop_body[7:9] + loop_body[10:14],
        _body_without_return(_function(current_tree, "_append_subscriber_flags", class_name=class_name)),
    )
    _assert_same(
        "analyzer cross-dupe block",
        loop_body[14:18],
        _body_without_return(_function(current_tree, "_append_cross_dupe_flag", class_name=class_name)),
    )
    _assert_same(
        "analyzer cyrillic block",
        loop_body[18:22],
        _body_without_return(_function(current_tree, "_append_cyrillic_flag", class_name=class_name)),
    )
    _assert_same(
        "analyzer chat-noise block",
        loop_body[22:27],
        _body_without_return(_function(current_tree, "_append_chat_noise_flag", class_name=class_name)),
    )
    _assert_same(
        "analyzer suspicious username block",
        loop_body[27:29],
        _function(current_tree, "_append_suspicious_username_flag", class_name=class_name).body,
    )
    _assert_same("analyzer result constructor", _append_call_arg(loop_body[29]), _return_value(result_func))
    _assert_same(
        "analyzer final report",
        base.body[8:11],
        _replace_loads_in_statements(
            _function(current_tree, "_filter_report_from_results", class_name=class_name).body,
            {"started_at": _parse_expr("t0")},
        ),
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-ref", default="origin/main")
    args = parser.parse_args()

    verify_worker(args.base_ref)
    verify_analyzer(args.base_ref)
    print("AST equivalence checks passed")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except AstMismatch as exc:
        print(exc, file=sys.stderr)
        raise SystemExit(1)
