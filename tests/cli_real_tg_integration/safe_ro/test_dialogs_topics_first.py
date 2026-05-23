import pytest

pytestmark = pytest.mark.real_tg_safe


@pytest.mark.timeout(180)
def test_dialogs_topics_first(run_cli, assert_cli_ok, sandbox_channel):
    _pk, channel_id = sandbox_channel
    result = run_cli("dialogs", "topics", "--channel-id", channel_id, timeout=120)
    assert_cli_ok(result)
    # Если канал не форум — выведет «No forum topics found», exit 0 — это валидно
    out_lower = result.stdout.lower()
    assert (
        result.stdout.strip()
        and ("title" in out_lower or "no forum topics" in out_lower)
    ), f"unexpected `dialogs topics` output: {result.stdout!r}"
