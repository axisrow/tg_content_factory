"""Unit tests for src/utils/text_safety.py (audit #837/8, #837/9)."""

from __future__ import annotations

import xml.etree.ElementTree as ET

import pytest

from src.utils.text_safety import csv_safe_cell, escape_xml_text, strip_xml_illegal


def test_strip_xml_illegal_removes_c0_control_bytes():
    assert strip_xml_illegal("a\x00\x01\x08\x0b\x0c\x0e\x1fb") == "ab"


def test_strip_xml_illegal_keeps_tab_lf_cr():
    assert strip_xml_illegal("a\tb\nc\rd") == "a\tb\nc\rd"


def test_escape_xml_text_yields_well_formed_xml():
    # \x01 would survive html.escape() alone and break the whole document.
    text = "danger\x01ous & <tag>"
    element = ET.fromstring(f"<r>{escape_xml_text(text)}</r>")
    assert element.text == "dangerous & <tag>"


@pytest.mark.parametrize("dangerous", ["=cmd", "+1", "-1", "@x", "\tx", "\rx"])
def test_csv_safe_cell_neutralizes_formula(dangerous):
    safe = csv_safe_cell(dangerous)
    assert safe.startswith("'")
    assert safe[1:] == dangerous


def test_csv_safe_cell_leaves_plain_text():
    assert csv_safe_cell("hello world") == "hello world"


def test_csv_safe_cell_passes_through_non_strings():
    assert csv_safe_cell(123) == 123
    assert csv_safe_cell(None) is None


def test_csv_safe_cell_empty_string():
    assert csv_safe_cell("") == ""
