import pytest

pytestmark = pytest.mark.real_tg_safe


@pytest.mark.timeout(180)
def test_channel_add_already_existing(run_cli, assert_cli_ok, live_channel_username):
    """Smoke: вызвать `channel add @username` для уже существующего канала.

    `channel add` идемпотентна на уровне строк — повторное добавление
    существующего канала не плодит дубликаты, а делает ON CONFLICT DO UPDATE
    с обновлением title/username/channel_type/is_active (см.
    src/database/repositories/channels.py:29-37). DB-эффект benign, цель теста —
    реальный TG API call (resolve_channel + fetch_channel_meta).
    """
    result = run_cli("channel", "add", live_channel_username, timeout=120)
    assert_cli_ok(result)
    assert "Added channel:" in result.stdout, f"unexpected `channel add` output: {result.stdout!r}"
