from __future__ import annotations

import os

import pytest

pytestmark = pytest.mark.real_tg_mutation_safe


def _required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        pytest.skip(f"{name} is required for this explicit-target mutation-safe CLI smoke")
    return value


@pytest.mark.timeout(90)
def test_dialogs_react_explicit_target(run_cli, assert_cli_ok):
    chat_id = _required_env("CLI_REAL_TG_REACT_CHAT_ID")
    message_id = _required_env("CLI_REAL_TG_REACT_MESSAGE_ID")
    emoji = os.environ.get("CLI_REAL_TG_REACT_EMOJI", "👍")
    phone = os.environ.get("CLI_REAL_TG_REACT_PHONE")
    phone_args = ("--phone", phone) if phone else ()

    result = run_cli(
        "dialogs",
        "react",
        chat_id,
        message_id,
        emoji,
        "--yes",
        *phone_args,
        timeout=60,
    )

    assert_cli_ok(result)
    combined = f"{result.stdout}\n{result.stderr}"
    assert "Reaction" in combined and "sent to message" in combined
