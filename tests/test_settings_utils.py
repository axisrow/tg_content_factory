import logging

from src.settings_utils import parse_float_setting, parse_int_setting

logger = logging.getLogger("test_settings_utils")


def test_parse_float_setting_valid():
    assert parse_float_setting("2.5", setting_name="x", default=1.0, logger=logger) == 2.5


def test_parse_float_setting_blank_uses_default():
    assert parse_float_setting("", setting_name="x", default=1.0, logger=logger) == 1.0
    assert parse_float_setting(None, setting_name="x", default=1.0, logger=logger) == 1.0


def test_parse_float_setting_garbage_uses_default():
    assert parse_float_setting("abc", setting_name="x", default=1.0, logger=logger) == 1.0


def test_parse_float_setting_rejects_non_finite():
    # float("nan")/float("inf") parse without raising; they must fall back to the
    # default so int()-coercing or clamping callers don't crash (issue #641 review).
    for raw in ("nan", "inf", "-inf", "Infinity"):
        assert parse_float_setting(raw, setting_name="x", default=7.0, logger=logger) == 7.0


def test_parse_int_setting_valid_and_default():
    assert parse_int_setting("12", setting_name="x", default=1, logger=logger) == 12
    assert parse_int_setting("2.5", setting_name="x", default=1, logger=logger) == 1
    assert parse_int_setting("", setting_name="x", default=1, logger=logger) == 1
