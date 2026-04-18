"""Tests for node_dsl parser."""
from __future__ import annotations

import pytest

from src.cli.node_dsl import (
    NodeSpec,
    NodeSpecError,
    _coerce_bare,
    _parse_relaxed_array,
    _split_kv_pairs,
    generate_node_id,
    parse_node_spec,
)
from src.models import PipelineNodeType


# --- generate_node_id ---


def test_generate_node_id():
    assert generate_node_id(PipelineNodeType.LLM_GENERATE, 0) == "llm_generate_0"
    assert generate_node_id(PipelineNodeType.PUBLISH, 3) == "publish_3"


# --- parse_node_spec ---


def test_parse_node_spec_type_only():
    spec = parse_node_spec("llm_generate")
    assert spec.type == PipelineNodeType.LLM_GENERATE
    assert spec.config == {}
    assert spec.id is None


def test_parse_node_spec_with_config():
    spec = parse_node_spec("publish:target=channel1")
    assert spec.type == PipelineNodeType.PUBLISH
    assert spec.config["target"] == "channel1"


def test_parse_node_spec_with_explicit_id():
    spec = parse_node_spec("llm_generate:id=my_node,prompt=hello")
    assert spec.id == "my_node"
    assert spec.config["prompt"] == "hello"


def test_parse_node_spec_empty_raises():
    with pytest.raises(NodeSpecError, match="empty"):
        parse_node_spec("")


def test_parse_node_spec_whitespace_raises():
    with pytest.raises(NodeSpecError, match="empty"):
        parse_node_spec("   ")


def test_parse_node_spec_unknown_type():
    with pytest.raises(NodeSpecError, match="Unknown"):
        parse_node_spec("nonexistent_type")


# --- _coerce_bare ---


def test_coerce_bare_bool():
    assert _coerce_bare("true") is True
    assert _coerce_bare("false") is False


def test_coerce_bare_int():
    assert _coerce_bare("42") == 42
    assert isinstance(_coerce_bare("42"), int)


def test_coerce_bare_float():
    assert _coerce_bare("3.14") == 3.14
    assert isinstance(_coerce_bare("3.14"), float)


def test_coerce_bare_string():
    assert _coerce_bare("hello") == "hello"


# --- _split_kv_pairs ---


def test_split_kv_pairs_simple():
    result = _split_kv_pairs("key=value")
    assert result == [("key", "value")]


def test_split_kv_pairs_multiple():
    result = _split_kv_pairs("a=1,b=2")
    assert len(result) == 2


def test_split_kv_pairs_quoted():
    result = _split_kv_pairs('key="hello world"')
    assert result == [("key", "hello world")]


def test_split_kv_pairs_empty():
    assert _split_kv_pairs("") == []


def test_split_kv_pairs_bare_key():
    result = _split_kv_pairs("bare_key,other=val")
    assert len(result) == 1
    assert result[0][0] == "other"


# --- _parse_relaxed_array ---


def test_parse_relaxed_array():
    assert _parse_relaxed_array("[a, b, 3]") == ["a", "b", 3]


def test_parse_relaxed_array_empty():
    assert _parse_relaxed_array("[]") == []


# --- parse_node_spec with JSON array ---


def test_parse_node_spec_json_array():
    spec = parse_node_spec('source:channels=["ch1","ch2"]')
    assert spec.config["channels"] == ["ch1", "ch2"]


def test_parse_node_spec_json_object():
    spec = parse_node_spec('llm_generate:params={"temperature": 0.7}')
    assert spec.config["params"] == {"temperature": 0.7}


def test_parse_node_spec_quoted_value():
    spec = parse_node_spec('publish:caption="Hello World"')
    assert spec.config["caption"] == "Hello World"
