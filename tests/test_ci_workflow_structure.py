"""Regression guards for the CI workflow structure (#1090, plan #1097).

These tests parse ``.github/workflows/ci.yml`` and assert the structural
invariants the #1097 owner-plan locked in, so a future edit can't silently
undo them:

- the monolithic ``lint-and-test`` job is split into parallel jobs
  (``lint`` | ``static-checks`` | test jobs) that fan out for speed (#1097 §5) —
  this parallel split is the real CI speedup;
- the test gate is three parallel jobs (``tests-smoke`` | ``tests-shards`` |
  ``tests-serial``). The shards split the suite FILE-atomically via
  ``scripts/shard_tests.py`` so the ``--dist=loadfile`` invariant (tests in one
  file stay on one worker) survives; the shard legs take NO positional
  ``tests`` arg — the file list is the selection, a positional arg would make
  every shard run the full-suite union;
- every test job carries a five-minute BUDGET (owner decision, 2026-10-08):
  a test job past five minutes reds CI instead of quietly growing the wall.
  Test jobs are also leg-free — the same commands run on PR and push;
- code coverage measurement is REMOVED from CI (owner decision, 2026-10-08):
  no ``--cov`` flags, no ``coverage-combine`` job, no ``[tool.coverage]``
  ratchet. ``pytest-cov`` stays a dev dependency for local, manual use only;
- the ``pip-audit`` dependency scan stays *advisory* (``continue-on-error``)
  like the other security/dup guards (#1097 §4);
- the ``doc-coverage`` step is **removed** from CI — interrogate stays a local
  script (#1072) but is no longer a CI step (#1097 §3);
- the existing blocking gates (import contracts, complexity, warnings-as-errors)
  survive the job split.

Note: pytest-testmon selective runs were evaluated for #1090 and dropped — on
this large suite testmon must run single-process without coverage (it only
deselects single-process and crashes under xdist+coverage), which is often
slower than the full ``-n auto`` sweep, not faster. More runners, not fewer
tests, is the CI win that landed.

These are pure-text/YAML assertions: no DB, no network — a ``unit`` level test.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

CI_YML = Path(__file__).resolve().parent.parent / ".github" / "workflows" / "ci.yml"

TEST_JOBS = ("tests-smoke", "tests-shards", "tests-serial")
TEST_BUDGET_MINUTES = 5


@pytest.fixture(scope="module")
def ci_config() -> dict:
    """Parse ci.yml once for the whole module."""
    return yaml.safe_load(CI_YML.read_text(encoding="utf-8"))


def _job_steps_text(job: dict) -> str:
    """Concatenate every ``run`` block of a job into one searchable string."""
    parts: list[str] = []
    for step in job.get("steps", []):
        run = step.get("run")
        if run:
            parts.append(run)
    return "\n".join(parts)


def _steps_by_name(job: dict) -> dict[str, dict]:
    return {s.get("name"): s for s in job.get("steps", [])}


def test_ci_yaml_is_valid(ci_config: dict) -> None:
    """ci.yml must parse and declare jobs."""
    assert isinstance(ci_config, dict)
    assert "jobs" in ci_config


def test_jobs_split_into_parallel_lint_static_tests(ci_config: dict) -> None:
    """#1097 §5: the monolithic lint-and-test job is split into parallel jobs.

    There must be a dedicated ``lint`` job, a ``static-checks`` job and the
    three-way test gate (smoke | shards | serial); the old combined
    ``lint-and-test`` job, the former monolithic ``tests`` job and the removed
    ``coverage-combine`` job must all stay gone.
    """
    jobs = ci_config["jobs"]
    assert "lint" in jobs, "expected a dedicated parallel `lint` job"
    assert "static-checks" in jobs, "expected a `static-checks` job"
    for name in TEST_JOBS:
        assert name in jobs, f"expected the `{name}` job"
    assert "lint-and-test" not in jobs, "old monolithic `lint-and-test` job must be removed"
    assert "tests" not in jobs, "old monolithic `tests` job must be removed (now smoke | shards | serial)"
    assert "coverage-combine" not in jobs, "coverage machinery was removed from CI (owner decision, 2026-10-08)"


def test_parallel_jobs_have_no_needs_chains(ci_config: dict) -> None:
    """The lint/static-checks/test jobs must run in parallel (no `needs:`).

    A `needs:` dependency would serialize them and erase the #1097 §5 speedup.
    """
    jobs = ci_config["jobs"]
    for name in ("lint", "static-checks", *TEST_JOBS):
        assert "needs" not in jobs[name], f"job `{name}` must not declare `needs:` (would serialize the split)"


def test_lint_job_runs_ruff(ci_config: dict) -> None:
    """The split-out lint job runs ruff so lint fails fast in parallel."""
    text = _job_steps_text(ci_config["jobs"]["lint"])
    assert "ruff check" in text, "lint job must run `ruff check`"


def test_test_jobs_have_five_minute_budget(ci_config: dict) -> None:
    """Every test job reds when it exceeds five minutes (owner budget).

    ``timeout-minutes: 5`` is the enforcement knob: the runner kills the job
    and the check goes red. Without this pin the number can quietly drift back
    to a hang-guard value and the wall starts creeping again.
    """
    for name in TEST_JOBS:
        budget = ci_config["jobs"][name].get("timeout-minutes")
        assert budget == TEST_BUDGET_MINUTES, (
            f"{name} must carry the {TEST_BUDGET_MINUTES}-minute budget (got {budget!r})"
        )


def test_test_jobs_are_leg_free(ci_config: dict) -> None:
    """Test jobs run identical commands on every event — no PR/push legs.

    The leg split existed only for coverage (#1456, retired 2026-10-08). A
    re-introduced ``github.event_name`` condition inside a test job is the
    fingerprint of that pattern coming back.
    """
    for name in TEST_JOBS:
        for step in ci_config["jobs"][name].get("steps", []):
            cond = step.get("if") or ""
            assert "github.event_name" not in cond, f"{name}/{step.get('name')!r} must not branch on the event"


def test_smoke_job_runs_preflight(ci_config: dict) -> None:
    """The smoke job runs the offline preflight over real critical paths."""
    text = _job_steps_text(ci_config["jobs"]["tests-smoke"])
    assert "-m smoke" in text, "smoke job must run the smoke preflight marker"
    assert "--cov" not in text, "coverage was removed from CI — no --cov in the smoke job"


def test_shard_jobs_run_half_the_parallel_suite(ci_config: dict) -> None:
    """The shards split the parallel-safe suite file-atomically, ×3.

    Guards the shard mechanics: the same ``-m`` filter as the serial lane,
    ``-n auto`` fan-out, the shard script in the command, the matrix set — and,
    critically, NO positional ``tests`` arg: the file list from
    scripts/shard_tests.py is the whole selection, a positional arg would make
    each shard run the full-suite union (a silent 3× duplicate run).
    """
    job = ci_config["jobs"]["tests-shards"]
    matrix = job["strategy"]["matrix"]["shard-id"]
    assert matrix == [0, 1, 2], "sharding must be a 3-way matrix"
    assert job["strategy"]["fail-fast"] is False, "one red shard must not kill the others"

    run = _steps_by_name(job)["Pytest (parallel-safe)"]["run"]
    assert "-m" in run and "not aiosqlite_serial" in run, "shards must exclude the serial lane"
    assert "-n auto" in run, "shards must fan out across the runner cores"
    assert "scripts/shard_tests.py" in run, "shard selection must come from scripts/shard_tests.py"
    assert "--num-shards 3" in run and "--shard-id" in run, "shard id must come from the matrix"
    assert not run.startswith("pytest tests"), (
        "shard legs must NOT pass a positional `tests` arg — the file list is the selection"
    )
    assert run.startswith("pytest -q"), "shard legs must start with the pytest invocation itself"
    assert "--cov" not in run, "coverage was removed from CI — no --cov in the shard command"


def test_serial_job_keeps_per_file_parallel_lane(ci_config: dict) -> None:
    """The serial lane keeps `-m aiosqlite_serial -n auto --dist=loadfile`.

    Independent files run on separate workers while each file stays on one
    worker (the old single-process command left all 986 tests serialized).
    """
    run = _steps_by_name(ci_config["jobs"]["tests-serial"])["Pytest (aiosqlite serial, per-file parallel)"]["run"]
    assert "-m aiosqlite_serial" in run
    assert "-n auto" in run and "--dist=loadfile" in run, (
        "aiosqlite_serial files must run in parallel while each file stays on one worker"
    )
    assert "--cov" not in run, "coverage was removed from CI — no --cov in the serial lane"


def test_no_coverage_machinery_anywhere_in_ci(ci_config: dict) -> None:
    """Coverage measurement is gone from CI (owner decision, 2026-10-08).

    No step in any job may measure, combine, report or upload coverage. The
    #1052 fail_under ratchet retired with it; ``pytest-cov`` remains a dev
    dependency for local, manual use only.
    """
    for job_name, job in ci_config["jobs"].items():
        text = _job_steps_text(job)
        assert "--cov" not in text, f"coverage was removed from CI — found --cov in {job_name}"
        for step in job.get("steps", []):
            uses = step.get("uses") or ""
            assert "upload-artifact" not in uses or "coverage" not in (step.get("with", {}).get("name", "")), (
                f"{job_name}/{step.get('name')!r} must not upload coverage artifacts"
            )


def test_test_jobs_do_not_use_testmon(ci_config: dict) -> None:
    """testmon was dropped for #1090 — no test-job step may pass a --testmon flag.

    testmon-collection is incompatible with xdist + coverage (it INTERNALERRORs)
    and only deselects single-process. Guard against it being re-added to the
    full ``-n auto`` suite, which would crash the gate.
    """
    jobs = ci_config["jobs"]
    for name in TEST_JOBS:
        for step in jobs[name].get("steps", []):
            run = step.get("run") or ""
            assert "--testmon" not in run, f"{name} must not use testmon (xdist+cov INTERNALERROR); got: {run!r}"


def test_pip_audit_is_advisory(ci_config: dict) -> None:
    """#1097 §4: pip-audit dependency scan stays advisory (non-blocking)."""
    steps = ci_config["jobs"]["static-checks"]["steps"]
    audit_steps = [s for s in steps if "pip-audit" in (s.get("run") or "")]
    assert audit_steps, "static-checks must run pip-audit"
    for step in audit_steps:
        assert step.get("continue-on-error") is True, "pip-audit must be advisory (continue-on-error: true)"


def test_doc_coverage_removed_from_ci(ci_config: dict) -> None:
    """#1097 §3: the doc-coverage advisory step is removed from CI.

    interrogate stays a local script (#1072) but must not be EXECUTED as a CI
    step. We check the executable `run` blocks (a comment explaining the removal
    is fine), and also assert no step is *named* like a doc-coverage gate.
    """
    for job_name, job in ci_config["jobs"].items():
        for step in job.get("steps", []):
            run = step.get("run") or ""
            assert "doc_coverage.py" not in run, f"doc-coverage must not be executed in CI (job {job_name})"
            assert "interrogate" not in run.lower(), f"interrogate must not be invoked in CI (job {job_name})"
            name = (step.get("name") or "").lower()
            assert "doc-coverage" not in name and "doc coverage" not in name, (
                f"no doc-coverage step may remain (job {job_name}, step {step.get('name')!r})"
            )


def test_complexity_and_import_gates_preserved(ci_config: dict) -> None:
    """Splitting jobs must NOT drop the existing blocking static gates.

    lint-imports (import contracts) and the cyclomatic-complexity gate must
    still run somewhere across the lint/static-checks jobs.
    """
    lint_text = _job_steps_text(ci_config["jobs"]["lint"])
    static_text = _job_steps_text(ci_config["jobs"]["static-checks"])
    combined = lint_text + "\n" + static_text
    assert "lint-imports" in combined, "import architecture contracts gate must be preserved"
    assert "code_health.py --fail-on F" in combined, "cyclomatic-complexity gate must be preserved"


def test_warnings_as_errors_check_preserved(ci_config: dict) -> None:
    """The filterwarnings=['error'] enforcement check must survive the split."""
    static_text = _job_steps_text(ci_config["jobs"]["static-checks"])
    assert "filterwarnings" in static_text, "warnings-as-errors enforcement check must be preserved in CI"
