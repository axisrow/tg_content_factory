import pytest

pytestmark = pytest.mark.real_tg_safe


@pytest.mark.timeout(240)
def test_dialogs_read_first(run_cli, assert_cli_ok, live_channel, live_phone):
    """`dialogs read` — live history via tg_messenger, read-only.

    Unlike `dialogs broadcast-stats` this needs no admin rights: reading a
    dialog's history works for any peer the account can see, so an ordinary
    monitored channel is a valid target.
    """
    pk, _channel_id = live_channel
    result = run_cli(
        "dialogs", "read", pk, "--phone", live_phone, "--limit", "3", timeout=180
    )
    assert_cli_ok(result)
    assert result.stdout.strip(), "`dialogs read` produced empty stdout"


@pytest.mark.timeout(240)
def test_dialogs_read_json_first(run_cli, assert_cli_ok, live_channel, live_phone):
    """`--format json` must stay machine-readable — the shape web/CLI parity rests on."""
    import json

    pk, _channel_id = live_channel
    result = run_cli(
        "dialogs", "read", pk, "--phone", live_phone, "--limit", "3",
        "--format", "json", timeout=180,
    )
    assert_cli_ok(result)
    payload = json.loads(result.stdout)
    assert isinstance(payload, list)
    for message in payload:
        assert {"id", "date", "out"} <= set(message)
