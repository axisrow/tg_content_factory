"""Regression guards for the CI workflow structure (#1090, plan #1097).

These tests parse ``.github/workflows/ci.yml`` and assert the structural
invariants the #1097 owner-plan locked in, so a future edit can't silently
undo them:

- the monolithic ``lint-and-test`` job is split into parallel jobs
  (``lint`` | ``static-checks`` | ``tests``) that fan out for speed (#1097 §5) —
  this parallel split is the real CI speedup;
- the ``pip-audit`` dependency scan stays *advisory* (``continue-on-error``)
  like the other security/dup guards (#1097 §4);
- the ``doc-coverage`` step is **removed** from CI — interrogate stays a local
  script (#1072) but is no longer a CI step (#1097 §3);
- the existing blocking gates (import contracts, complexity, warnings-as-errors)
  survive the job split.

Note: pytest-testmon selective runs were evaluated for #1090 and dropped — on
this large suite testmon must run single-process without coverage (it only
deselects single-process and crashes under xdist+coverage), which is often
slower than the full ``-n auto`` sweep, not faster. The parallel-job split is
the CI win that landed.

These are pure-text/YAML assertions: no DB, no network — a ``unit`` level test.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

CI_YML = Path(__file__).resolve().parent.parent / ".github" / "workflows" / "ci.yml"


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


def test_ci_yaml_is_valid(ci_config: dict) -> None:
    """ci.yml must parse and declare jobs."""
    assert isinstance(ci_config, dict)
    assert "jobs" in ci_config


def test_jobs_split_into_parallel_lint_static_tests(ci_config: dict) -> None:
    """#1097 §5: the monolithic lint-and-test job is split into parallel jobs.

    There must be a dedicated ``lint`` job, a ``static-checks`` job and a
    ``tests`` job, and the old combined ``lint-and-test`` job must be gone so it
    can't drift back to a serial bottleneck.
    """
    jobs = ci_config["jobs"]
    assert "lint" in jobs, "expected a dedicated parallel `lint` job"
    assert "static-checks" in jobs, "expected a `static-checks` job"
    assert "tests" in jobs, "expected a `tests` job"
    assert "lint-and-test" not in jobs, "old monolithic `lint-and-test` job must be removed"


def test_parallel_jobs_have_no_needs_chains(ci_config: dict) -> None:
    """The lint/static-checks/tests jobs must run in parallel (no `needs:`).

    A `needs:` dependency would serialize them and erase the #1097 §5 speedup.
    """
    jobs = ci_config["jobs"]
    for name in ("lint", "static-checks", "tests"):
        assert "needs" not in jobs[name], f"job `{name}` must not declare `needs:` (would serialize the split)"


def test_lint_job_runs_ruff(ci_config: dict) -> None:
    """The split-out lint job runs ruff so lint fails fast in parallel."""
    text = _job_steps_text(ci_config["jobs"]["lint"])
    assert "ruff check" in text, "lint job must run `ruff check`"


def test_tests_job_runs_full_suite(ci_config: dict) -> None:
    """The tests job must run the full smoke + parallel + serial suite.

    Guards against accidentally dropping a leg of the suite. The parallel-safe
    leg uses `-n auto`; the serial leg uses the aiosqlite_serial marker; both
    measure coverage so the fail_under gate has data.
    """
    steps = ci_config["jobs"]["tests"]["steps"]
    runs = [s.get("run") or "" for s in steps]
    assert any("-m smoke" in r for r in runs), "tests job must run the smoke preflight"
    parallel = [r for r in runs if 'not aiosqlite_serial' in r and "-n auto" in r]
    serial = [r for r in runs if "-m aiosqlite_serial" in r]
    assert parallel, "tests job must run the parallel-safe leg with -n auto"
    assert serial, "tests job must run the aiosqlite_serial leg"
    assert all("--cov=src" in r for r in parallel + serial), "both test legs must measure coverage"


def test_tests_job_does_not_use_testmon(ci_config: dict) -> None:
    """testmon was dropped for #1090 — no pytest step may pass a --testmon flag.

    testmon-collection is incompatible with xdist + coverage (it INTERNALERRORs)
    and only deselects single-process. Guard against it being re-added to the
    full `-n auto --cov` suite, which would crash the gate.
    """
    for step in ci_config["jobs"]["tests"]["steps"]:
        run = step.get("run") or ""
        assert "--testmon" not in run, f"tests job must not use testmon (xdist+cov INTERNALERROR); got: {run!r}"


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
