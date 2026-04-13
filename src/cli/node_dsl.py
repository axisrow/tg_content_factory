"""Mini-DSL parser for pipeline node specs.

Parses ``type:key=value,key=value`` strings into :class:`NodeSpec` objects
used by :mod:`src.cli.graph_builder` to assemble DAG pipelines on the CLI.

Grammar (informal)::

    node_spec   := type [ ":" kv_list ]
    kv_list     := kv ("," kv)*
    kv          := "id=" IDENT                -- extracted as explicit node ID
                 | IDENT "=" value
    value       := QUOTED_STRING
                 | JSON_ARRAY
                 | JSON_OBJECT
                 | "true" | "false"           -- bool
                 | INT | FLOAT                -- numeric
                 | BARE_WORD                  -- string
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

from src.models import PipelineNodeType


class NodeSpecError(ValueError):
    """Raised when a node spec string is malformed."""


@dataclass
class NodeSpec:
    """Parsed representation of a ``type:key=value`` node spec."""

    type: PipelineNodeType
    config: dict[str, Any] = field(default_factory=dict)
    id: str | None = None


def generate_node_id(node_type: PipelineNodeType, counter: int) -> str:
    """Generate a deterministic node ID like ``react_0``."""
    return f"{node_type.value}_{counter}"


def parse_node_spec(raw: str) -> NodeSpec:
    """Parse a ``type:key=value,key=value`` string into a :class:`NodeSpec`.

    See module docstring for grammar details.
    """
    raw = raw.strip()
    if not raw:
        raise NodeSpecError("Node spec cannot be empty.")

    # Split on first ':' — everything before is the type, after is config.
    colon_idx = raw.find(":")
    if colon_idx == -1:
        type_str = raw
        config_part = ""
    else:
        type_str = raw[:colon_idx]
        config_part = raw[colon_idx + 1 :]

    # Validate type
    type_str = type_str.strip()
    try:
        node_type = PipelineNodeType(type_str)
    except ValueError:
        valid = ", ".join(t.value for t in PipelineNodeType)
        raise NodeSpecError(
            f"Unknown node type '{type_str}'. Valid types: {valid}"
        ) from None

    # Parse config key=value pairs
    config: dict[str, Any] = {}
    explicit_id: str | None = None

    if config_part.strip():
        pairs = _split_kv_pairs(config_part)
        for key, value in pairs:
            if key == "id":
                explicit_id = str(value)
            else:
                config[key] = value

    return NodeSpec(type=node_type, config=config, id=explicit_id)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _split_kv_pairs(text: str) -> list[tuple[str, Any]]:
    """Split comma-separated key=value pairs, respecting quotes and brackets."""
    pairs: list[tuple[str, Any]] = []
    pos = 0
    length = len(text)

    while pos < length:
        # Skip leading whitespace
        while pos < length and text[pos] in " \t":
            pos += 1
        if pos >= length:
            break

        # Read key (up to '=')
        key_start = pos
        while pos < length and text[pos] != "=" and text[pos] != ",":
            pos += 1

        key = text[key_start:pos].strip()
        if not key:
            pos += 1
            continue

        if pos >= length or text[pos] == ",":
            # Key without value — skip bare keys
            pos += 1
            continue

        # Skip '='
        pos += 1  # skip '='

        # Skip whitespace after '='
        while pos < length and text[pos] in " \t":
            pos += 1

        # Read value
        value, pos = _read_value(text, pos)
        pairs.append((key, value))

        # Skip comma separator
        if pos < length and text[pos] == ",":
            pos += 1

    return pairs


def _read_value(text: str, pos: int) -> tuple[Any, int]:
    """Read a value starting at *pos* and return (parsed_value, new_pos)."""
    if pos >= len(text):
        return "", pos

    ch = text[pos]

    # Quoted string
    if ch == '"':
        return _read_quoted(text, pos)

    # JSON array
    if ch == "[":
        return _read_bracketed(text, pos, "[", "]")

    # JSON object
    if ch == "{":
        return _read_bracketed(text, pos, "{", "}")

    # Bare value (until comma or end)
    start = pos
    while pos < len(text) and text[pos] != ",":
        pos += 1
    raw = text[start:pos].strip()
    return _coerce_bare(raw), pos


def _read_quoted(text: str, pos: int) -> tuple[str, int]:
    """Read a double-quoted string starting at *pos*."""
    assert text[pos] == '"'
    pos += 1  # skip opening quote
    start = pos
    while pos < len(text):
        if text[pos] == '"':
            return text[start:pos], pos + 1
        if text[pos] == "\\" and pos + 1 < len(text):
            pos += 2  # skip escaped char
            continue
        pos += 1
    raise NodeSpecError("Unterminated quoted value in node spec.")


def _read_bracketed(text: str, pos: int, open_ch: str, close_ch: str) -> tuple[Any, int]:
    """Read a JSON array or object starting at *pos*."""
    start = pos
    depth = 0
    while pos < len(text):
        if text[pos] == open_ch:
            depth += 1
        elif text[pos] == close_ch:
            depth -= 1
            if depth == 0:
                pos += 1
                raw = text[start:pos]
                try:
                    return json.loads(raw), pos
                except json.JSONDecodeError:
                    # Relaxed array syntax: [a,b,c] → ["a","b","c"]
                    if open_ch == "[" and close_ch == "]":
                        try:
                            return _parse_relaxed_array(raw), pos
                        except Exception as exc:
                            raise NodeSpecError(f"Invalid JSON in node spec: {exc}") from exc
                    raise NodeSpecError("Invalid JSON in node spec.") from None
        elif text[pos] == '"':
            # Skip quoted strings inside JSON to avoid false bracket matches
            pos += 1
            while pos < len(text):
                if text[pos] == '"':
                    break
                if text[pos] == "\\" and pos + 1 < len(text):
                    pos += 1
                pos += 1
        pos += 1
    raise NodeSpecError(f"Unterminated {open_ch}{close_ch} group in node spec.")


def _parse_relaxed_array(raw: str) -> list:
    """Parse ``[a, b, 3]`` as ``["a", "b", 3]`` — bare items become strings."""
    inner = raw.strip()[1:-1]
    if not inner.strip():
        return []
    items: list[Any] = []
    for part in inner.split(","):
        part = part.strip()
        if not part:
            continue
        items.append(_coerce_bare(part))
    return items


def _coerce_bare(raw: str) -> Any:
    """Coerce a bare value string to int/float/bool if appropriate."""
    if raw == "true":
        return True
    if raw == "false":
        return False
    # Try int
    try:
        return int(raw)
    except ValueError:
        pass
    # Try float
    try:
        return float(raw)
    except ValueError:
        pass
    return raw
