"""`scheduler start` — long-running scheduler daemon, Popen + stdout polling.

Spawns the scheduler via Popen, waits up to 20s for its own startup line, then
terminates via SIGTERM. The terminate is the explicit shutdown signal — there
is no separate `scheduler stop` CLI once `scheduler start` is running
interactively.

Categorized as mutating: while the scheduler doesn't delete user data,
it can enqueue collection_tasks if jobs are due. That state is
reversible/transient.
"""
import select
import time

import pytest

pytestmark = pytest.mark.real_tg_safe


def _wait_for_stdout_contains(proc, needle: str, *, timeout: float = 20.0) -> tuple[bool, str]:
    stdout = proc.stdout
    if stdout is None:
        return False, ""

    output: list[str] = []
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            rest = stdout.read()
            if rest:
                output.append(rest)
            break

        ready, _, _ = select.select([stdout], [], [], 0.2)
        if not ready:
            continue
        line = stdout.readline()
        if not line:
            continue
        output.append(line)
        if needle in line:
            return True, "".join(output)

    return needle in "".join(output), "".join(output)


@pytest.mark.timeout(60)
def test_proc_scheduler_start_prints_startup_and_stays_running(run_cli_popen, cli_real_cli_env):
    import subprocess

    del cli_real_cli_env
    proc = run_cli_popen(
        "scheduler",
        "start",
        capture_stdout=True,
        extra_env={"PYTHONUNBUFFERED": "1"},
    )
    started, stdout_text = _wait_for_stdout_contains(proc, "Scheduler started", timeout=20.0)
    exited_early = proc.poll() is not None

    # communicate() drains stderr + waits in one shot — no risk of pipe stall.
    proc.terminate()
    try:
        stdout_tail, stderr_text = proc.communicate(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()
        stdout_tail, stderr_text = proc.communicate(timeout=5)

    if proc.returncode not in (0, -15, 143):
        # -15 / 143 = SIGTERM. 0 = clean exit on Ctrl+C handling.
        pytest.fail(
            f"`scheduler start` exited with {proc.returncode}; "
            f"stderr tail: {(stderr_text or '')[-500:]!r}"
        )

    combined_stdout = stdout_text + (stdout_tail or "")
    if not started:
        pytest.fail(
            "`scheduler start` did not print its startup line within 20s; "
            f"stdout tail: {combined_stdout[-500:]!r}; stderr tail: {(stderr_text or '')[-500:]!r}"
        )
    if exited_early:
        pytest.fail(
            "`scheduler start` exited before test teardown; "
            f"stdout tail: {combined_stdout[-500:]!r}; stderr tail: {(stderr_text or '')[-500:]!r}"
        )
