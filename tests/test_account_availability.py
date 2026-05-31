"""Unit tests for the shared account-availability source of truth (#529)."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from src.models import AccountSessionStatus
from src.services.account_availability import compute_account_availability


def _acc(**kw):
    base = {
        "phone": "+8613000000000",
        "is_active": True,
        "flood_wait_until": None,
        "session_status": AccountSessionStatus.OK,
    }
    base.update(kw)
    return SimpleNamespace(**base)


def test_available_when_active_connected_ok_session():
    avail = compute_account_availability(_acc(), connected=True)
    assert avail["state"] == "available"


def test_session_unavailable_takes_precedence():
    # Even active + connected, a bad session means re-auth is required.
    avail = compute_account_availability(
        _acc(session_status=AccountSessionStatus.DECRYPT_FAILED), connected=True
    )
    assert avail["state"] == "session_unavailable"


def test_inactive_when_toggled_off():
    avail = compute_account_availability(_acc(is_active=False), connected=True)
    assert avail["state"] == "inactive"


def test_disconnected_when_session_ok_but_not_in_pool():
    avail = compute_account_availability(_acc(), connected=False)
    assert avail["state"] == "disconnected"


def test_flood_reports_remaining():
    future = datetime.now(timezone.utc) + timedelta(minutes=10)
    avail = compute_account_availability(_acc(flood_wait_until=future), connected=True)
    assert avail["state"] == "flood"
    assert avail["remaining_seconds"] > 0
    assert avail["remaining_minutes"] >= 1


def test_expired_flood_is_available():
    past = datetime.now(timezone.utc) - timedelta(minutes=10)
    avail = compute_account_availability(_acc(flood_wait_until=past), connected=True)
    assert avail["state"] == "available"


def test_naive_flood_timestamp_treated_as_utc():
    future_naive = (datetime.now(timezone.utc) + timedelta(minutes=5)).replace(tzinfo=None)
    avail = compute_account_availability(_acc(flood_wait_until=future_naive), connected=True)
    assert avail["state"] == "flood"
