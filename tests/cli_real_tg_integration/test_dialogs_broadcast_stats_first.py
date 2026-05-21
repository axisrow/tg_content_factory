import pytest

pytestmark = pytest.mark.real_tg_safe


def test_dialogs_broadcast_stats_first(run_cli, assert_cli_ok, discover_first_dialog_username):
    username = discover_first_dialog_username()
    result = run_cli("dialogs", "broadcast-stats", username, timeout=120)
    # broadcast-stats может вернуть не-zero для не-broadcast чатов — assert_cli_ok
    # тогда зафейлит и покажет stderr. Это ок для smoke: один из первых найденных
    # @-чатов должен быть broadcast-каналом. Если падает стабильно — `discover`
    # надо уточнить, чтобы выбирал именно channel.
    assert_cli_ok(result)
    assert result.stdout.strip(), "`dialogs broadcast-stats` produced empty stdout"
