import pytest

pytestmark = pytest.mark.real_tg_safe


def test_channel_add_already_existing(run_cli, assert_cli_ok, discover_first_dialog_username):
    """Smoke: вызвать `channel add @username` для уже существующего канала.

    `channel add` идемпотентна — повторное добавление существующего канала
    не дублирует строку, лишь обновляет meta. Это даёт нам реальный API call
    (resolve_channel + fetch_channel_meta) без mutating-эффектов в DB.
    """
    username = discover_first_dialog_username()
    result = run_cli("channel", "add", username, timeout=120)
    assert_cli_ok(result)
    assert result.stdout.strip(), "`channel add` produced empty stdout"
