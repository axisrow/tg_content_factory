"""Unit tests for the mini-DSL node spec parser (src/cli/node_dsl.py)."""
from __future__ import annotations

import pytest

from src.models import PipelineNodeType


def _make_spec(raw: str):
    from src.cli.node_dsl import parse_node_spec

    return parse_node_spec(raw)


class TestParseSimple:
    def test_type_no_config(self):
        spec = _make_spec("delay")
        assert spec.type == PipelineNodeType.DELAY
        assert spec.config == {}
        assert spec.id is None

    def test_type_empty_config_after_colon(self):
        spec = _make_spec("forward:")
        assert spec.type == PipelineNodeType.FORWARD
        assert spec.config == {}

    def test_type_with_colon_no_equals(self):
        """Bare word after colon is treated as a flag (empty-string value)."""
        spec = _make_spec("publish:")
        assert spec.type == PipelineNodeType.PUBLISH
        assert spec.config == {}


class TestParseKeyValues:
    def test_single_kv(self):
        spec = _make_spec("react:emoji=heart")
        assert spec.type == PipelineNodeType.REACT
        assert spec.config == {"emoji": "heart"}

    def test_multiple_kv(self):
        spec = _make_spec("llm_generate:model=claude-sonnet-4-6,max_tokens=2000")
        assert spec.type == PipelineNodeType.LLM_GENERATE
        assert spec.config["model"] == "claude-sonnet-4-6"
        assert spec.config["max_tokens"] == 2000

    def test_numeric_int_values(self):
        spec = _make_spec("delay:min_seconds=1,max_seconds=5")
        assert spec.config["min_seconds"] == 1
        assert spec.config["max_seconds"] == 5

    def test_numeric_float_values(self):
        spec = _make_spec("delay:min_seconds=0.5,max_seconds=2.5")
        assert spec.config["min_seconds"] == 0.5
        assert spec.config["max_seconds"] == 2.5

    def test_boolean_values(self):
        spec = _make_spec("publish:reply=true,mode=moderated")
        assert spec.config["reply"] is True
        assert spec.config["mode"] == "moderated"

    def test_false_boolean(self):
        spec = _make_spec("filter:match_links=false")
        assert spec.config["match_links"] is False


class TestParseQuotedValues:
    def test_quoted_value_with_comma(self):
        spec = _make_spec('llm_generate:prompt="Summarize: {text}"')
        assert spec.config["prompt"] == "Summarize: {text}"

    def test_quoted_value_with_spaces(self):
        spec = _make_spec('agent_loop:system_prompt="You are a helpful assistant"')
        assert spec.config["system_prompt"] == "You are a helpful assistant"

    def test_quoted_value_with_equals(self):
        spec = _make_spec('llm_generate:prompt="Use model=claude"')
        assert spec.config["prompt"] == "Use model=claude"

    def test_unclosed_quote_raises(self):
        from src.cli.node_dsl import NodeSpecError

        with pytest.raises(NodeSpecError, match="quote|unclosed|unterminated"):
            _make_spec('react:emoji="unclosed')

    def test_quoted_value_preserves_unicode(self):
        spec = _make_spec('react:emoji="\u2764\ufe0f"')
        assert spec.config["emoji"] == "\u2764\ufe0f"


class TestParseJsonValues:
    def test_json_array(self):
        spec = _make_spec("filter:type=service_message,service_types=[user_joined,user_left]")
        assert spec.config["service_types"] == ["user_joined", "user_left"]

    def test_json_array_with_numbers(self):
        spec = _make_spec("fetch_messages:limit=10,ids=[1,2,3]")
        assert spec.config["ids"] == [1, 2, 3]

    def test_json_object(self):
        spec = _make_spec('source:meta={"key":"val"}')
        assert spec.config["meta"] == {"key": "val"}


class TestExplicitId:
    def test_id_extracted_from_config(self):
        spec = _make_spec("agent_loop:id=agent1,model=claude")
        assert spec.id == "agent1"
        assert "id" not in spec.config
        assert spec.config["model"] == "claude"

    def test_id_standalone(self):
        spec = _make_spec("publish:id=pub1")
        assert spec.id == "pub1"
        assert spec.config == {}


class TestValidation:
    def test_unknown_type_raises(self):
        from src.cli.node_dsl import NodeSpecError

        with pytest.raises(NodeSpecError, match="[Uu]nknown.*type|invalid.*type"):
            _make_spec("nonexistent_type:x=1")

    def test_empty_string_raises(self):
        from src.cli.node_dsl import NodeSpecError

        with pytest.raises(NodeSpecError):
            _make_spec("")

    def test_whitespace_trimmed(self):
        spec = _make_spec("  delay  ")
        assert spec.type == PipelineNodeType.DELAY

    def test_all_valid_types(self):
        """Every PipelineNodeType value should be parseable as a bare type."""
        for node_type in PipelineNodeType:
            spec = _make_spec(node_type.value)
            assert spec.type == node_type


class TestGenerateNodeId:
    def test_default_naming(self):
        from src.cli.node_dsl import generate_node_id

        assert generate_node_id(PipelineNodeType.REACT, 0) == "react_0"
        assert generate_node_id(PipelineNodeType.LLM_GENERATE, 3) == "llm_generate_3"
