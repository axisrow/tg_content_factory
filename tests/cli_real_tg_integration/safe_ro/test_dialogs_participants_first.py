import pytest

pytestmark = pytest.mark.real_tg_safe


@pytest.mark.timeout(180)
def test_dialogs_participants_first(run_cli, assert_cli_ok, live_channel_username):
    """`dialogs participants @username --limit 10` — read participants list.

    Real Telegram API call (GetParticipantsRequest) without DB writes.
    `chat_id` is positional and accepts @username (see parser_domains/dialogs.py:113-114).
    """
    username = live_channel_username
    result = run_cli("dialogs", "participants", username, "--limit", "10", timeout=120)
    assert_cli_ok(result)
    assert result.stdout.strip(), "`dialogs participants` produced empty stdout"
