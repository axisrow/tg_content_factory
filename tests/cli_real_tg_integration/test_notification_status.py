import pytest

pytestmark = pytest.mark.real_tg_safe


def test_notification_status(run_cli, assert_cli_ok):
    result = run_cli("notification", "status")
    assert_cli_ok(result)
    combined = result.stdout + result.stderr
    # Команда печатает один из трёх состояний (src/cli/commands/notification.py:25–70):
    # «No notification bot configured», полная карточка «Bot: …» / «Bot ID: …»,
    # или диагностику «Notification target unavailable.» (см. _print_target_status).
    assert (
        "No notification bot configured" in combined
        or "Bot:" in combined
        or "Bot ID:" in combined
        or "Notification target unavailable" in combined
    ), f"unexpected `notification status` output: {combined!r}"
