"""NodeContext.record_error / get_errors API — issue #463 observability."""
from __future__ import annotations

from src.services.pipeline_nodes.base import NodeContext


def test_record_error_stores_structured_entry():
    ctx = NodeContext()
    ctx.record_error(node_id="react_1", code="no_available_client", detail="all flooded")
    errors = ctx.get_errors()
    assert len(errors) == 1
    e = errors[0]
    assert e["node_id"] == "react_1"
    assert e["code"] == "no_available_client"
    assert e["detail"] == "all flooded"
    assert "retry_after" not in e


def test_multiple_errors_preserve_order():
    ctx = NodeContext()
    ctx.record_error(node_id="a", code="x", detail="first")
    ctx.record_error(node_id="b", code="y", detail="second", retry_after=42)
    errs = ctx.get_errors()
    assert [e["node_id"] for e in errs] == ["a", "b"]
    assert errs[1]["retry_after"] == 42


def test_get_errors_empty_by_default():
    ctx = NodeContext()
    assert ctx.get_errors() == []


def test_get_errors_returns_copy_not_reference():
    """Callers must not be able to mutate internal state by editing the list."""
    ctx = NodeContext()
    ctx.record_error(node_id="a", code="x", detail="y")
    errs = ctx.get_errors()
    errs.append({"node_id": "fake"})
    assert len(ctx.get_errors()) == 1
