"""Tests for src/telegram/utils.py"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from src.telegram.utils import normalize_utc


@pytest.mark.asyncio
async def test_normalize_utc_none_returns_none():
    """None input returns None."""
    assert normalize_utc(None) is None


@pytest.mark.asyncio
async def test_normalize_utc_naive_treated_as_utc():
    """Naive datetime gets UTC tzinfo."""
    naive = datetime(2024, 3, 15, 10, 30, 45)
    result = normalize_utc(naive)

    assert result is not None
    assert result.tzinfo == timezone.utc
    assert result.year == 2024
    assert result.month == 3
    assert result.day == 15
    assert result.hour == 10
    assert result.minute == 30
    assert result.second == 45


@pytest.mark.asyncio
async def test_normalize_utc_aware_converted():
    """Non-UTC timezone converted to UTC."""
    # Create datetime in UTC+5
    tz_plus5 = timezone(timedelta(hours=5))
    aware = datetime(2024, 3, 15, 15, 30, 45, tzinfo=tz_plus5)
    result = normalize_utc(aware)

    assert result is not None
    assert result.tzinfo == timezone.utc
    # 15:30 UTC+5 = 10:30 UTC
    assert result.hour == 10
    assert result.minute == 30


@pytest.mark.asyncio
async def test_normalize_utc_already_utc():
    """Already-UTC datetime returned as-is."""
    utc_dt = datetime(2024, 3, 15, 10, 30, 45, tzinfo=timezone.utc)
    result = normalize_utc(utc_dt)

    assert result == utc_dt


@pytest.mark.asyncio
async def test_normalize_utc_preserves_values():
    """Time components unchanged after conversion from naive."""
    naive = datetime(2024, 12, 31, 23, 59, 59, 999999)
    result = normalize_utc(naive)

    assert result is not None
    assert result.year == 2024
    assert result.month == 12
    assert result.day == 31
    assert result.hour == 23
    assert result.minute == 59
    assert result.second == 59
    assert result.microsecond == 999999
