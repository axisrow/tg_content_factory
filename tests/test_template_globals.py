from datetime import datetime, timezone

from markupsafe import Markup

from src.web.template_globals import FILTER_FLAG_EMOJI, local_dt_filter


def test_none_returns_dash():
    assert local_dt_filter(None) == "—"


def test_empty_string_returns_dash():
    assert local_dt_filter("") == "—"


def test_whitespace_string_returns_dash():
    assert local_dt_filter("   ") == "—"


def test_naive_datetime_gets_utc_suffix():
    dt = datetime(2024, 3, 15, 10, 30, 0)
    result = str(local_dt_filter(dt))
    assert "data-utc=" in result
    # naive datetime should have +00:00 appended (via replace(tzinfo=utc))
    assert "+00:00" in result


def test_aware_datetime_utc_preserved():
    dt = datetime(2024, 3, 15, 10, 30, 0, tzinfo=timezone.utc)
    result = str(local_dt_filter(dt))
    assert "+00:00" in result
    assert 'class="local-dt"' in result


def test_string_without_tz_gets_z():
    result = str(local_dt_filter("2024-03-15T10:30:00"))
    assert "2024-03-15T10:30:00Z" in result


def test_string_with_z_unchanged():
    result = str(local_dt_filter("2024-03-15T10:30:00Z"))
    assert "2024-03-15T10:30:00Z" in result
    # must not double-append Z
    assert "ZZ" not in result


def test_string_with_positive_offset_unchanged():
    result = str(local_dt_filter("2024-03-15T12:30:00+02:00"))
    assert "2024-03-15T12:30:00+02:00" in result
    assert "ZZ" not in result
    assert result.count("Z") == 0


def test_string_with_negative_offset_unchanged():
    # Negative UTC offsets like -05:00 must not get Z appended (would produce invalid ISO)
    result = str(local_dt_filter("2024-03-15T10:30:00-05:00"))
    assert "2024-03-15T10:30:00-05:00" in result
    assert "Z" not in result


def test_fmt_date_attribute():
    dt = datetime(2024, 3, 15, tzinfo=timezone.utc)
    result = str(local_dt_filter(dt, fmt="date"))
    assert 'data-fmt="date"' in result


def test_fmt_time_attribute():
    dt = datetime(2024, 3, 15, 10, 30, tzinfo=timezone.utc)
    result = str(local_dt_filter(dt, fmt="time"))
    assert 'data-fmt="time"' in result


def test_fmt_default_datetime_attribute():
    dt = datetime(2024, 3, 15, tzinfo=timezone.utc)
    result = str(local_dt_filter(dt))
    assert 'data-fmt="datetime"' in result


def test_fmt_with_quotes_is_escaped():
    # XSS: fmt value containing a quote must be escaped in HTML attribute
    dt = datetime(2024, 3, 15, tzinfo=timezone.utc)
    result = str(local_dt_filter(dt, fmt='"><script>'))
    assert "<script>" not in result
    assert "&lt;" in result or "&#34;" in result or "&quot;" in result


def test_output_is_span():
    dt = datetime(2024, 3, 15, 10, 30, tzinfo=timezone.utc)
    result = str(local_dt_filter(dt))
    assert result.startswith("<span") and result.endswith("</span>")


def test_fallback_text_present_for_datetime():
    dt = datetime(2024, 3, 15, 10, 30, tzinfo=timezone.utc)
    result = str(local_dt_filter(dt))
    assert "2024-03-15 10:30" in result


def test_fallback_text_present_for_string():
    result = str(local_dt_filter("2024-03-15T10:30:00"))
    # fallback is str(value)[:16] of original string
    assert "2024-03-15T10:30" in result


def test_filter_flag_emoji_uses_bootstrap_icons():
    """Every FILTER_FLAG_EMOJI entry must use a Bootstrap Icon (Markup with bi bi- class)."""
    assert len(FILTER_FLAG_EMOJI) > 0, "FILTER_FLAG_EMOJI should not be empty"
    for flag_key, (icon_value, label) in FILTER_FLAG_EMOJI.items():
        assert isinstance(icon_value, Markup), f"{flag_key}: icon must be a Markup instance"
        icon_str = str(icon_value)
        assert "bi bi-" in icon_str, f"{flag_key}: icon must contain 'bi bi-' class, got: {icon_str}"
        assert "<i " in icon_str, f"{flag_key}: icon must be an <i> element"
        assert isinstance(label, str) and len(label) > 0, f"{flag_key}: label must be a non-empty string"
