import pytest

pytestmark = pytest.mark.real_tg_safe


def test_dialogs_download_media_help(run_cli, assert_cli_ok):
    """
    `dialogs download-media` требует chat_id + message_id с реальным медиа в
    конкретном сообщении. У теста группы RO-DB нет надёжного способа выбрать
    подходящий msg_id без побочных эффектов, поэтому проверяем только, что
    подкоманда зарегистрирована и `--help` отрабатывает успешно (это и есть
    тривиальная read-only валидация CLI без сетевых вызовов).
    """
    result = run_cli("dialogs", "download-media", "--help")
    assert_cli_ok(result)
    assert "download-media" in (result.stdout + result.stderr).lower()
