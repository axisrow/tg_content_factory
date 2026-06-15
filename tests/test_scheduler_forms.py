"""Tests for src/web/scheduler/forms.py validation (audit #837/6)."""

from __future__ import annotations

import pytest

from src.web.scheduler import forms


@pytest.mark.parametrize(
    "job_id",
    ["collect_all", "photo_due", "photo_auto", "warm_all_dialogs", "sq_3", "pipeline_run_5", "content_generate_5"],
)
def test_valid_job_ids(job_id):
    assert forms.is_valid_job_id(job_id) is True


def test_warm_all_dialogs_is_valid():
    # Regression: warm_all_dialogs was missing from the regex, so its web
    # toggle/set-interval returned invalid_job and never persisted (audit #837/6).
    assert forms.is_valid_job_id("warm_all_dialogs") is True


@pytest.mark.parametrize("job_id", ["custom_job", "sq_", "warm", "", "pipeline_run_"])
def test_invalid_job_ids(job_id):
    assert forms.is_valid_job_id(job_id) is False
