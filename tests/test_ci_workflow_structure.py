"""Regression guards for the CI workflow structure (#1090, plan #1097).

These tests parse ``.github/workflows/ci.yml`` and assert the structural
invariants the #1097 owner-plan locked in, so a future edit can't silently
undo them:

- the monolithic ``lint-and-test`` job is split into parallel jobs
  (``lint`` | ``static-checks`` | test jobs) that fan out for speed (#1097 §5) —
  this parallel split is the real CI speedup;
- the test gate itself is three parallel jobs (``tests-smoke`` |
  ``tests-shards`` | ``tests-serial``) whose wall is the slowest shard, plus a
  push-only ``coverage-combine`` downstream job (#1052). The shards split the
  suite FILE-atomically via ``scripts/shard_tests.py`` so the
  ``--dist=loadfile`` invariant (tests in one file stay on one worker)
  survives; the shard legs take NO positional ``tests`` arg — the file list is
  the selection, a positional arg would make every shard run the full-suite
  union;
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
COVERAGE_PRODUCERS = ("tests-smoke", "tests-shards", "tests-serial")


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
    three-way test gate (smoke | shards | serial) with its push-only
    ``coverage-combine`` downstream; the old combined ``lint-and-test`` job AND
    the former monolithic ``tests`` job must be gone so neither can drift back
    to a serial bottleneck.
    """
    jobs = ci_config["jobs"]
    assert "lint" in jobs, "expected a dedicated parallel `lint` job"
    assert "static-checks" in jobs, "expected a `static-checks` job"
    for name in (*TEST_JOBS, "coverage-combine"):
        assert name in jobs, f"expected the `{name}` job"
    assert "lint-and-test" not in jobs, "old monolithic `lint-and-test` job must be removed"
    assert "tests" not in jobs, "old monolithic `tests` job must be removed (now smoke | shards | serial)"


def test_parallel_jobs_have_no_needs_chains(ci_config: dict) -> None:
    """The lint/static-checks/test jobs must run in parallel (no `needs:`).

    A `needs:` dependency would serialize them and erase the #1097 §5 speedup.
    """
    jobs = ci_config["jobs"]
    for name in ("lint", "static-checks", *TEST_JOBS):
        assert "needs" not in jobs[name], f"job `{name}` must not declare `needs:` (would serialize the split)"


def test_coverage_combine_needs_all_producers(ci_config: dict) -> None:
    """coverage-combine is the one legitimate `needs:` consumer.

    It must depend on every coverage producer (so its `coverage combine` can't
    hit the old "no data to combine" exit-1 gotcha) and only on them.
    """
    needs = ci_config["jobs"]["coverage-combine"].get("needs")
    assert needs is not None, "coverage-combine must declare `needs:`"
    assert set(needs) == set(COVERAGE_PRODUCERS), f"coverage-combine needs must be exactly {COVERAGE_PRODUCERS}"


def test_lint_job_runs_ruff(ci_config: dict) -> None:
    """The split-out lint job runs ruff so lint fails fast in parallel."""
    text = _job_steps_text(ci_config["jobs"]["lint"])
    assert "ruff check" in text, "lint job must run `ruff check`"


def test_smoke_job_runs_preflight(ci_config: dict) -> None:
    """The smoke job runs the offline preflight on both event legs (#1456).

    The push leg MUST measure coverage: the smoke lines have to stay in the
    combined dataset or the fail_under ratchet (#1052) would drift.
    """
    steps = _steps_by_name(ci_config["jobs"]["tests-smoke"])
    pr_run = steps["Pytest (smoke preflight)"]["run"]
    main_run = steps["Pytest (smoke preflight, with coverage)"]["run"]
    for run in (pr_run, main_run):
        assert "-m smoke" in run, "smoke job must run the smoke preflight marker"
    assert "--cov" not in pr_run, "PR smoke leg must not measure coverage"
    assert "--cov=src" in main_run and "--cov-report=" in main_run, "push smoke leg must measure src"
    assert "github.event_name == 'pull_request'" in (steps["Pytest (smoke preflight)"].get("if") or "")
    assert "github.event_name == 'push'" in (steps["Pytest (smoke preflight, with coverage)"].get("if") or "")


def test_shard_jobs_run_half_the_parallel_suite(ci_config: dict) -> None:
    """The shards split the parallel-safe suite file-atomically, ×2, both legs.

    Guards the shard mechanics: the same `-m` filter as the serial exclusion,
    `-n auto` fan-out, the shard script in the command, the matrix set — and,
    critically, NO positional ``tests`` arg: the file list from
    scripts/shard_tests.py is the whole selection, a positional arg would make
    each shard run the full-suite union (a silent 2× duplicate run).
    """
    job = ci_config["jobs"]["tests-shards"]
    matrix = job["strategy"]["matrix"]["shard-id"]
    assert matrix == [0, 1], "sharding must be a 2-way matrix"
    assert job["strategy"]["fail-fast"] is False, "one red shard must not kill the other"
    assert "matrix.shard-id" in (job.get("env", {}).get("COVERAGE_FILE") or ""), (
        "each shard must name its own coverage dataset via COVERAGE_FILE"
    )

    steps = _steps_by_name(job)
    pr_run = steps["Pytest (parallel-safe)"]["run"]
    main_run = steps["Pytest (parallel-safe, with coverage)"]["run"]
    for run in (pr_run, main_run):
        assert "-m" in run and "not aiosqlite_serial" in run, "shards must exclude the serial lane"
        assert "-n auto" in run, "shards must fan out across the runner cores"
        assert "scripts/shard_tests.py" in run, "shard selection must come from scripts/shard_tests.py"
        assert "--num-shards 2" in run and "--shard-id" in run, "shard id must come from the matrix"
        assert not run.startswith("pytest tests"), (
            "shard legs must NOT pass a positional `tests` arg — the file list is the selection"
        )
        assert run.startswith("pytest -q"), "shard legs must start with the pytest invocation itself"
    assert "--cov" not in pr_run, "PR shard leg must not measure coverage"
    assert "--cov=src" in main_run and "--cov-report=" in main_run, "push shard leg must measure src"
    assert "github.event_name == 'pull_request'" in (steps["Pytest (parallel-safe)"].get("if") or "")
    assert "github.event_name == 'push'" in (steps["Pytest (parallel-safe, with coverage)"].get("if") or "")


def test_serial_job_keeps_per_file_parallel_lane(ci_config: dict) -> None:
    """The serial lane keeps `-m aiosqlite_serial -n auto --dist=loadfile`.

    Independent files run on separate workers while each file stays on one
    worker. The push leg starts a FRESH dataset — `--cov-append` is gone now
    that the lane is its own job and its coverage travels to coverage-combine
    as a separate artifact (#1052).
    """
    steps = _steps_by_name(ci_config["jobs"]["tests-serial"])
    pr_run = steps["Pytest (aiosqlite serial, per-file parallel)"]["run"]
    main_run = steps["Pytest (aiosqlite serial, per-file parallel, with coverage)"]["run"]
    for run in (pr_run, main_run):
        assert "-m aiosqlite_serial" in run
        assert "-n auto" in run and "--dist=loadfile" in run, (
            "aiosqlite_serial files must run in parallel while each file stays on one worker"
        )
    assert "--cov" not in pr_run, "PR serial leg must not measure coverage"
    assert "--cov=src" in main_run and "--cov-report=" in main_run, "push serial leg must measure src"
    assert "--cov-append" not in main_run, "serial lane is its own job — a fresh dataset, no append"
    serial_pr_if = steps["Pytest (aiosqlite serial, per-file parallel)"].get("if") or ""
    assert "github.event_name == 'pull_request'" in serial_pr_if
    assert "github.event_name == 'push'" in (
        steps["Pytest (aiosqlite serial, per-file parallel, with coverage)"].get("if") or ""
    )


def test_coverage_producers_upload_gated_data_artifacts(ci_config: dict) -> None:
    """Every coverage producer uploads its dataset, push-gated, fail-loud.

    `if-no-files-found: error` is what guarantees coverage-combine's
    `coverage combine` always has data files — the old silent-exit-1 gotcha.
    """
    for name in COVERAGE_PRODUCERS:
        steps = _steps_by_name(ci_config["jobs"][name])
        upload = steps.get("Upload coverage data")
        assert upload is not None, f"{name} must upload its coverage data"
        assert "github.event_name == 'push'" in (upload.get("if") or ""), f"{name} upload must be push-only"
        with_ = upload.get("with", {})
        assert str(with_.get("name", "")).startswith("coverage-data-"), (
            f"{name} dataset artifact must be coverage-data-*"
        )
        assert with_.get("if-no-files-found") == "error", f"{name} upload must fail loud when no data file exists"


def test_coverage_combine_merges_and_reports(ci_config: dict) -> None:
    """coverage-combine downloads all datasets, combines, reports, publishes xml."""
    job = ci_config["jobs"]["coverage-combine"]
    assert "github.event_name == 'push'" in (job.get("if") or ""), "coverage-combine must be main-only (#1456)"

    steps = _steps_by_name(job)
    download = steps.get("Download coverage data")
    assert download is not None and download.get("uses", "").startswith("actions/download-artifact@")
    assert download.get("with", {}).get("pattern") == "coverage-data-*"
    assert download.get("with", {}).get("merge-multiple") is True

    report = steps.get("Coverage report (combined)")
    assert report is not None, "coverage-combine must run the combined report"
    for cmd in ("coverage combine", "coverage report", "coverage xml"):
        assert cmd in report["run"], f"combined report must run `{cmd}`"

    upload = steps.get("Upload coverage artifact")
    assert upload is not None and upload.get("with", {}).get("name") == "coverage-xml"


def test_test_jobs_do_not_use_testmon(ci_config: dict) -> None:
    """testmon was dropped for #1090 — no test-job step may pass a --testmon flag.

    testmon-collection is incompatible with xdist + coverage (it INTERNALERRORs)
    and only deselects single-process. Guard against it being re-added to the
    full `-n auto --cov` suite, which would crash the gate.
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
