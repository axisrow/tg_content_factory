import pytest

pytestmark = pytest.mark.real_tg_mutation_safe


@pytest.mark.timeout(180)
def test_dialogs_participants_scratch_group(run_cli, assert_cli_ok, live_scratch_group):
    result = run_cli(
        "dialogs",
        "participants",
        "--phone",
        live_scratch_group.phone,
        live_scratch_group.chat_ref,
        "--limit",
        "10",
        timeout=120,
    )
    assert_cli_ok(result)
    assert result.stdout.strip(), "`dialogs participants` produced empty stdout"
