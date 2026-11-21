"""A process start identity must mean the same thing in every locale.

Found on a live run of #1868: the gateway was started under ru_RU and the CLI
ran under C, so ``ps -o lstart`` printed one instant two ways and the live
gateway looked like it was down.
"""

from __future__ import annotations

import subprocess

import pytest

from tg_agent.utils import process_identity
from tg_agent.utils.process_identity import process_start_identity, same_start_identity

RU = "пятница, 20 ноября 2026 г. 21:58:19"
C = "Fri Nov 20 21:58:19 2026"


def test_start_identity_is_read_in_a_fixed_locale(monkeypatch):
    calls = []

    def run(command, **kwargs):
        calls.append(kwargs)
        return subprocess.CompletedProcess(command, 0, stdout=f"{C}\n", stderr="")

    monkeypatch.setattr(process_identity.sys, "platform", "darwin")
    monkeypatch.setattr(process_identity.subprocess, "run", run)
    monkeypatch.setenv("LC_ALL", "ru_RU.UTF-8")

    assert process_start_identity(4242) == C
    assert calls[0]["env"]["LC_ALL"] == "C"


@pytest.mark.parametrize(
    ("recorded", "current", "same"),
    [
        (C, C, True),
        (RU, C, True),
        ("суббота, 7 ноября 2026 г. 09:05:01", "Sat Nov  7 09:05:01 2026", True),
        (RU, "Fri Nov 20 21:58:20 2026", False),
        ("4242", "4242", True),
        ("4242", "4243", False),
        (None, C, False),
        (RU, None, False),
        ("", "", False),
        ("no digits", "none here", False),
    ],
    ids=[
        "identical",
        "record-from-another-locale",
        "single-digit-day",
        "another-start-time",
        "linux-ticks-equal",
        "linux-ticks-differ",
        "nothing-recorded",
        "process-gone",
        "both-empty",
        "no-numbers-to-compare",
    ],
)
def test_same_start_identity(recorded, current, same):
    assert same_start_identity(recorded, current) is same
