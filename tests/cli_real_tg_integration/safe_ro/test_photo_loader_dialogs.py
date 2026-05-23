import pytest

pytestmark = pytest.mark.real_tg_safe


@pytest.mark.timeout(240)
def test_photo_loader_dialogs(run_cli, assert_cli_ok, live_phone):
    result = run_cli("photo-loader", "dialogs", "--phone", live_phone, timeout=180)
    assert_cli_ok(result)
    assert result.stdout.strip(), "`photo-loader dialogs` produced empty stdout"
