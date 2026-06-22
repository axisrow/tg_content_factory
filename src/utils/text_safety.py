"""Sanitizers for untrusted Telegram text embedded into exports/feeds.

- XML 1.0 forbids most C0 control bytes; ``html.escape`` does not strip them, so a
  single such byte makes a whole RSS/Atom document not well-formed (audit #837/8).
- CSV cells starting with ``= + - @`` (or TAB/CR) are interpreted as formulas by
  Excel/LibreOffice/Sheets — spreadsheet formula injection, CWE-1236 (audit #837/9).
"""

from __future__ import annotations

import html
import re
from typing import Any

# Characters illegal in XML 1.0: C0 control chars (excluding the legal Tab \x09,
# LF \x0a, CR \x0d) plus the BMP noncharacters U+FFFE / U+FFFF, which are forbidden
# in well-formed XML and otherwise break a whole RSS/Atom document. Higher-plane
# noncharacters (U+FDD0–U+FDEF, U+1FFFE…U+10FFFF) are omitted on purpose: Telegram
# message text stays in the BMP, so these two are the only ones we observe.
_XML_ILLEGAL_RE = re.compile("[\x00-\x08\x0b\x0c\x0e-\x1f￾￿]")

# Leading characters that trigger formula evaluation in spreadsheet apps.
_CSV_FORMULA_PREFIXES = ("=", "+", "-", "@", "\t", "\r")


def strip_xml_illegal(text: str) -> str:
    """Remove characters that are illegal in XML 1.0 from ``text``."""
    return _XML_ILLEGAL_RE.sub("", text)


def escape_xml_text(text: str) -> str:
    """Strip XML-illegal control chars, then HTML/XML-escape the result."""
    return html.escape(strip_xml_illegal(text))


def csv_safe_cell(value: Any) -> Any:
    """Neutralize spreadsheet formula injection for a CSV cell.

    String cells whose first character can start a formula are prefixed with a
    single quote so the spreadsheet treats them as literal text. Non-string
    values are returned unchanged.
    """
    if isinstance(value, str) and value[:1] in _CSV_FORMULA_PREFIXES:
        return "'" + value
    return value
