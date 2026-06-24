"""Regression guards for the CI workflow structure (#1090, plan #1097).

These tests parse ``.github/workflows/ci.yml`` and assert the structural
invariants the #1097 owner-plan locked in, so a future edit can't silently
undo them:

- the monolithic ``lint-and-test`` job is split into parallel jobs
  (``lint`` | ``static-checks`` | ``tests``) that fan out for speed (#1097 §5);
- on a PR the test gate runs ``pytest --testmon`` — only the tests affected by
  the change — instead of the full ~9000-test suite (#1090 selective run);
- on ``main`` (post-merge) the full suite still runs as the blocking gate
  (conservative: testmon never replaces the full run on main);
- the ``pip-audit`` dependency scan stays *advisory* (``continue-on-error``)
  like the other security/dup guards (#1097 §4);
- the ``doc-coverage`` step is **removed** from CI — interrogate stays a local
  script (#1072) but is no longer a CI step (#1097 §3).

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


@pytest.fixture(scope="module")
def ci_text() -> str:
    """Raw ci.yml text for substring-level assertions."""
    return CI_YML.read_text(encoding="utf-8")


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


def test_lint_job_runs_ruff(ci_config: dict) -> None:
    """The split-out lint job runs ruff so lint fails fast in parallel."""
    text = _job_steps_text(ci_config["jobs"]["lint"])
    assert "ruff check" in text, "lint job must run `ruff check`"


def _is_selective_testmon(run: str) -> bool:
    """A *selecting* testmon run uses bare ``--testmon`` (re-runs only affected).

    ``--testmon-noselect`` updates the DB but deselects nothing, so it is a FULL
    run, not a selective one — distinguish the two precisely.
    """
    return "--testmon" in run and "--testmon-noselect" not in run


def test_pr_runs_testmon_selective(ci_config: dict) -> None:
    """#1090: on a pull_request the test gate runs selective `pytest --testmon`.

    Selective run is the whole point: a PR exercises only the affected tests.
    Every *selecting* testmon step must be guarded to the pull_request event,
    and at least one must exist.
    """
    steps = ci_config["jobs"]["tests"]["steps"]
    selective_steps = [s for s in steps if _is_selective_testmon(s.get("run") or "")]
    assert selective_steps, "tests job must have a selective `pytest --testmon` step for PRs"
    for step in selective_steps:
        cond = step.get("if", "")
        assert "pull_request" in cond, (
            f"selective testmon step must be PR-guarded (`if: ... pull_request ...`); got if={cond!r}"
        )


def test_selective_testmon_runs_single_process(ci_config: dict) -> None:
    """testmon only deselects in one process — selective steps must NOT use -n auto.

    Under pytest-xdist workers testmon still collects data but re-runs every
    test instead of deselecting the stable ones, erasing the speedup. This guard
    stops a future edit from "optimising" the selective PR steps with `-n auto`
    and silently killing the selection (#1090).
    """
    steps = ci_config["jobs"]["tests"]["steps"]
    selective_steps = [s for s in steps if _is_selective_testmon(s.get("run") or "")]
    assert selective_steps, "expected selective testmon steps to exist"
    for step in selective_steps:
        run = step.get("run") or ""
        assert "-n auto" not in run and "-n " not in run, (
            f"selective testmon step must run single-process (no xdist); got: {run!r}"
        )


def test_testmon_cache_keyed_by_pr_number_not_branch(ci_config: dict) -> None:
    """The `.testmondata` cache must be keyed by PR number, not branch name.

    Branch names (`head_ref`) collide across unrelated PRs (`patch-1`,
    `feature`, …), so a name-keyed cache + restore-keys wildcard could reuse an
    unrelated PR's baseline and select against the wrong code line — a possible
    false green. Keying by `github.event.pull_request.number` (repo-unique,
    stable) removes that class. Regression guard for a review finding (#1090).
    """
    steps = ci_config["jobs"]["tests"]["steps"]
    cache_steps = [s for s in steps if (s.get("uses") or "").startswith("actions/cache")]
    assert cache_steps, "tests job must have an actions/cache step for the testmon baseline"
    for step in cache_steps:
        with_block = step.get("with", {})
        key = with_block.get("key", "")
        restore = with_block.get("restore-keys", "")
        assert "pull_request.number" in key, (
            f"testmon cache key must include the PR number (repo-unique identity); got key={key!r}"
        )
        assert "head_ref" not in key and "head_ref" not in restore, (
            "testmon cache key/restore must NOT use head_ref (branch names collide across PRs)"
        )


def test_main_runs_full_suite_blocking(ci_config: dict) -> None:
    """Conservative gate: on `main`/push the FULL suite still runs (no selection).

    On push the run uses the plain `-n auto --cov` full suite with NO testmon
    flag at all (testmon-collection is incompatible with xdist+coverage). Those
    steps must be guarded to the push event and must NOT carry continue-on-error
    (they stay the blocking gate).
    """
    steps = ci_config["jobs"]["tests"]["steps"]
    full_steps = [
        s
        for s in steps
        if "pytest" in (s.get("run") or "")
        and not _is_selective_testmon(s.get("run") or "")
        and "-m smoke" not in (s.get("run") or "")
    ]
    assert full_steps, "tests job must keep a full-suite pytest step for main"
    push_guarded = [s for s in full_steps if "push" in (s.get("if", ""))]
    assert push_guarded, "the full-suite step(s) must be guarded to the push (main) event"
    for step in push_guarded:
        assert step.get("continue-on-error") is not True, (
            "the full-suite gate on main must stay blocking (no continue-on-error)"
        )
    # The PR selective steps and the main full steps must be mutually exclusive
    # by event so a PR never runs the full suite and main never selects.
    for step in push_guarded:
        assert "pull_request" not in step.get("if", ""), "full-suite step must not also fire on PRs"


def test_main_full_run_does_not_use_testmon(ci_config: dict) -> None:
    """main's full `-n auto --cov` run must NOT invoke testmon at all.

    testmon-collection is incompatible with xdist + coverage (it INTERNALERRORs,
    verified against pytest-testmon 2.2.0). Any `--testmon*` flag on a push-time
    pytest step would crash the blocking main gate, so guard against it.
    """
    steps = ci_config["jobs"]["tests"]["steps"]
    for step in steps:
        run = step.get("run") or ""
        cond = step.get("if", "")
        is_push_only = "push" in cond and "pull_request" not in cond
        if "pytest" in run and is_push_only:
            assert "--testmon" not in run, (
                f"main full-suite step must not use testmon (xdist+cov INTERNALERROR); got: {run!r}"
            )


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


def test_warnings_as_errors_check_preserved(ci_text: str) -> None:
    """The filterwarnings=['error'] enforcement check must survive the split."""
    assert "filterwarnings" in ci_text, "warnings-as-errors enforcement check must be preserved in CI"
