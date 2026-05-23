import pytest

pytestmark = pytest.mark.real_tg_safe


def test_photo_loader_refresh(run_cli, assert_cli_ok, live_phone):
    """`photo-loader refresh --phone +X` — refresh dialog cache from Telegram.

    Same shape as the existing `photo-loader dialogs` smoke; the only
    difference is the `refresh` action which forces a TG round-trip rather
    than reading the local dialog_cache.
    """
    phone = live_phone
    result = run_cli("photo-loader", "refresh", "--phone", phone, timeout=180)
    assert_cli_ok(result)
    assert result.stdout.strip(), "`photo-loader refresh` produced empty stdout"
