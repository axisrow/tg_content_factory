"""Tests for src/utils/json.py — safe_json_dumps."""
from datetime import date, datetime

import pytest
from pydantic import BaseModel

from src.utils.json import safe_json_dumps, safe_json_loads, safe_json_loads_dict, safe_json_loads_list


class _SampleModel(BaseModel):
    name: str
    value: int


def test_plain_types():
    assert safe_json_dumps({"a": 1, "b": "hello"}) == '{"a": 1, "b": "hello"}'


def test_datetime_serialization():
    dt = datetime(2026, 4, 19, 12, 30, 45)
    result = safe_json_dumps({"ts": dt})
    assert '"2026-04-19T12:30:45"' in result


def test_date_serialization():
    d = date(2026, 1, 15)
    result = safe_json_dumps({"d": d})
    assert '"2026-01-15"' in result


def test_bytes_serialization():
    data = b"\xde\xad\xbe\xef"
    result = safe_json_dumps({"raw": data})
    assert "deadbeef" in result


def test_pydantic_model_serialization():
    m = _SampleModel(name="test", value=42)
    result = safe_json_dumps(m)
    assert '"name": "test"' in result
    assert '"value": 42' in result


def test_unknown_type_raises_type_error():
    class Custom:
        pass

    with pytest.raises(TypeError, match="not JSON serializable"):
        safe_json_dumps(Custom())


def test_mixed_types():
    payload = {
        "dt": datetime(2026, 1, 1),
        "data": b"\xff",
        "model": _SampleModel(name="x", value=1),
        "plain": "text",
    }
    result = safe_json_dumps(payload)
    assert "2026-01-01" in result
    assert "ff" in result
    assert '"name": "x"' in result
    assert '"plain": "text"' in result


def test_kwargs_forwarded():
    result = safe_json_dumps({"b": 1, "a": 2}, sort_keys=True)
    assert result == '{"a": 2, "b": 1}'


def test_list_of_datetime():
    items = [datetime(2026, 1, 1), datetime(2026, 6, 15)]
    result = safe_json_dumps(items)
    assert "2026-01-01" in result
    assert "2026-06-15" in result


def test_safe_json_loads_returns_default_on_bad_input():
    assert safe_json_loads("not json", default={"fallback": True}) == {"fallback": True}
    assert safe_json_loads(None, default=[]) == []


def test_safe_json_loads_typed_helpers():
    assert safe_json_loads_dict('{"a": 1}') == {"a": 1}
    assert safe_json_loads_dict("[1]") is None
    assert safe_json_loads_list("[1, 2]") == [1, 2]
    assert safe_json_loads_list('{"a": 1}') == []
