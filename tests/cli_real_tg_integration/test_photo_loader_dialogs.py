import pytest

pytestmark = pytest.mark.real_tg_safe


def test_photo_loader_dialogs(run_cli, assert_cli_ok, discover_first_phone):
    phone = discover_first_phone()
    result = run_cli("photo-loader", "dialogs", "--phone", phone, timeout=180)
    assert_cli_ok(result)
    assert result.stdout.strip(), "`photo-loader dialogs` produced empty stdout"
