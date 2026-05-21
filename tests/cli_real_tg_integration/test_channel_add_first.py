import pytest

pytestmark = pytest.mark.real_tg_safe


def test_channel_add_already_existing(run_cli, assert_cli_ok, discover_first_dialog_username):
    """Smoke: вызвать `channel add @username` для уже существующего канала.

    `channel add` идемпотентна на уровне строк — повторное добавление
    существующего канала не плодит дубликаты, а делает ON CONFLICT DO UPDATE
    с обновлением title/username/channel_type/is_active (см.
    src/database/repositories/channels.py:29-37). DB-эффект benign, цель теста —
    реальный TG API call (resolve_channel + fetch_channel_meta).
    """
    username = discover_first_dialog_username()
    result = run_cli("channel", "add", username, timeout=120)
    assert_cli_ok(result)
    assert result.stdout.strip(), "`channel add` produced empty stdout"
