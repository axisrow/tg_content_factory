# Web-panel tests split out of tests/test_search.py (#1720 wave 3).
from __future__ import annotations

import pytest

from tg_webui.routes.search import _extract_length


@pytest.mark.parametrize(
    "query, expected",
    [
        ("crypto len<400", ("crypto", None, 400)),
        ("crypto len>100 len<400", ("crypto", 100, 400)),
        ("len>100", ("", 100, None)),
        ("crypto", ("crypto", None, None)),
        ("", ("", None, None)),
        ("lenovo", ("lenovo", None, None)),
    ],
    ids=[
        "max_only",
        "range",
        "min_only",
        "no_len",
        "empty",
        "lenovo_no_match",
    ],
)
def test_extract_length(query, expected):
    assert _extract_length(query) == expected
