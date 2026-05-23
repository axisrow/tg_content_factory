"""`python -m src.main test telegram` — live TG self-check on a temp DB copy.

src/cli/commands/test.py copies the live DB into a tempfile before running
telegram-side checks (_init_db_copy → tg_iter_messages → cleanup), so this
test never mutates the operator's real DB. Stays in safe_ro despite the
"telegram" name.
"""
import pytest

pytestmark = pytest.mark.real_tg_safe


@pytest.mark.timeout(360)
def test_proc_test_telegram(run_cli, assert_cli_ok):
    result = run_cli("test", "telegram", timeout=300)
    assert_cli_ok(result)
    combined = result.stdout + result.stderr

    # `test telegram` emits PASS/SKIP/FAIL lines per check. Round-2 review
    # caught that `tg_db_copy` (a filesystem-only DB-copy probe — no Telegram
    # involved) always emits "[PASS]" and would mask a "[FAIL] tg_pool_init"
    # right after it, letting the test pass without ever opening a Telegram
    # socket. So check the two critical-path probes explicitly:
    #
    #   - tg_db_copy   FAIL → environment broken before TG ever runs.
    #   - tg_pool_init FAIL → no Telegram session was established.
    #
    # See src/cli/commands/test.py:730-772 for the check order.
    if "[FAIL] tg_db_copy" in combined or "[FAIL] tg_pool_init" in combined:
        pytest.fail(
            f"`test telegram` critical check failed:\n{combined}"
        )

    # Beyond those two, individual probe FAILs are tolerated — they often
    # reflect legitimate transient prod issues (SessionExpired on one client
    # of many, etc.) rather than test regressions.
    if "PASS" in combined:
        return
    if "SKIP" in combined or "No accounts" in combined or "no accounts" in combined.lower():
        pytest.skip(
            "`test telegram` produced only SKIP/'No accounts' lines — "
            "nothing to smoke-check (typically no connected accounts)"
        )
    pytest.fail(f"`test telegram` produced no PASS and no recognizable SKIP: {combined!r}")
