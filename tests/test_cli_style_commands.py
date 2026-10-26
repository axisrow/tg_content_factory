"""Offline CLI tests for the `style` group — local-only style specs (#1687).

No live Telegram, no DB: every test drives the production Typer app through
``CliRunner`` against throwaway directories. ``TG_AGENT_GLOBAL_DIR`` relocates
the designs root into ``tmp_path``, so the real ``~/.tg-agent`` is never
touched and specs can never land in a worktree — that path independence IS the
feature (#1584: a committed design.md had to be force-pushed out).
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

import pytest
from typer.testing import CliRunner

from tg_agent.cli.typer_app import app

runner = CliRunner()

SOURCE_ROOT = Path(__file__).resolve().parent.parent


def _isolate(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Isolated project dir: cwd, config, designs root outside the worktree."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / "config.yaml").write_text("database:\n  path: local.db\n", encoding="utf-8")
    monkeypatch.setenv("TG_AGENT_GLOBAL_DIR", str(tmp_path / "global"))
    return tmp_path / "global" / "designs"


# --------------------------------------------------------------------------- #
# init — bootstrap outside the repository
# --------------------------------------------------------------------------- #


def test_init_creates_template_outside_worktree(tmp_path, monkeypatch):
    designs = _isolate(tmp_path, monkeypatch)
    result = runner.invoke(app, ["style", "init", "--channel", "@Digest_Channel"])
    assert result.exit_code == 0
    spec = designs / "Digest_Channel.md"
    assert spec.exists()
    assert str(spec) in result.stdout
    assert "Fill it in together with the owner" in result.stdout
    # Key normalization: @ handle stripped; t.me link and trailing slash too.
    again = runner.invoke(app, ["style", "init", "--channel", "https://t.me/Digest_Channel/"])
    assert again.exit_code == 1  # exists — never silently rewritten
    assert "--force" in again.stderr


def test_init_keeps_worktree_git_status_clean(tmp_path, monkeypatch):
    repo = tmp_path / "repo"
    repo.mkdir()
    monkeypatch.chdir(repo)
    (repo / "config.yaml").write_text("database:\n  path: local.db\n", encoding="utf-8")
    (repo / ".gitignore").write_text("data/\n", encoding="utf-8")
    monkeypatch.setenv("TG_AGENT_GLOBAL_DIR", str(tmp_path / "global"))
    subprocess.run(["git", "init", "-q"], cwd=repo, check=True, capture_output=True)
    subprocess.run(["git", "add", "."], cwd=repo, check=True, capture_output=True)
    subprocess.run(
        ["git", "-c", "user.name=t", "-c", "user.email=t@example.com", "commit", "-qm", "init"],
        cwd=repo,
        check=True,
        capture_output=True,
    )

    result = runner.invoke(app, ["style", "init", "--channel", "digest_channel"])
    assert result.exit_code == 0

    status = subprocess.run(
        ["git", "status", "--porcelain"], cwd=repo, check=True, capture_output=True, text=True
    )
    assert status.stdout == ""
    assert not (repo / "designs").exists()
    assert (tmp_path / "global" / "designs" / "digest_channel.md").exists()


def test_init_refuses_overwrite_and_force_rewrites(tmp_path, monkeypatch):
    designs = _isolate(tmp_path, monkeypatch)
    assert runner.invoke(app, ["style", "init", "--channel", "digest_channel"]).exit_code == 0
    (designs / "digest_channel.md").write_text("OWNER VOICE", encoding="utf-8")

    refused = runner.invoke(app, ["style", "init", "--channel", "digest_channel"])
    assert refused.exit_code == 1
    assert (designs / "digest_channel.md").read_text(encoding="utf-8") == "OWNER VOICE"

    forced = runner.invoke(app, ["style", "init", "--channel", "digest_channel", "--force"])
    assert forced.exit_code == 0
    assert "OWNER VOICE" not in (designs / "digest_channel.md").read_text(encoding="utf-8")


def test_reinit_after_delete_regenerates(tmp_path, monkeypatch):
    designs = _isolate(tmp_path, monkeypatch)
    assert runner.invoke(app, ["style", "init", "--channel", "digest_channel"]).exit_code == 0
    (designs / "digest_channel.md").unlink()
    result = runner.invoke(app, ["style", "init", "--channel", "digest_channel"])
    assert result.exit_code == 0
    assert (designs / "digest_channel.md").exists()


def test_init_accepts_negative_numeric_channel_id(tmp_path, monkeypatch):
    designs = _isolate(tmp_path, monkeypatch)
    result = runner.invoke(app, ["style", "init", "--channel", "-1001234567890"])
    assert result.exit_code == 0
    assert (designs / "-1001234567890.md").exists()


@pytest.mark.parametrize("bad", ["../evil", "a/b", "a\\b", ".", "..", "-.", "-..", ""])
def test_init_rejects_unsafe_keys(tmp_path, monkeypatch, bad):
    designs = _isolate(tmp_path, monkeypatch)
    result = runner.invoke(app, ["style", "init", "--channel", bad])
    assert result.exit_code == 1
    assert not list(designs.glob("*.md")) if designs.is_dir() else True
    assert not (tmp_path / "evil.md").exists()


# --------------------------------------------------------------------------- #
# template — placeholders only, no real voice content
# --------------------------------------------------------------------------- #


def test_template_is_placeholder_sections_only(tmp_path, monkeypatch):
    designs = _isolate(tmp_path, monkeypatch)
    assert runner.invoke(app, ["style", "init", "--channel", "digest_channel"]).exit_code == 0
    text = (designs / "digest_channel.md").read_text(encoding="utf-8")
    for section in ("## Voice", "## Post skeleton", "## Nicknames", "## Emoji policy", "## Digest rules"):
        assert section in text
    assert "NEVER commit" in text
    assert "digest_channel" in text  # key substituted into the title
    # No real-voice shaped content: no @ handles, no phone-like digit runs.
    assert "@" not in text
    assert not re.search(r"\+?\d{9,}", text)


# --------------------------------------------------------------------------- #
# list / show
# --------------------------------------------------------------------------- #


def test_list_empty_then_populated(tmp_path, monkeypatch):
    _isolate(tmp_path, monkeypatch)
    empty = runner.invoke(app, ["style", "list"])
    assert empty.exit_code == 0
    assert "No style specs yet" in empty.stdout

    assert runner.invoke(app, ["style", "init", "--channel", "digest_channel"]).exit_code == 0
    listed = runner.invoke(app, ["style", "list"])
    assert listed.exit_code == 0
    assert "digest_channel" in listed.stdout


def test_show_prints_spec_and_path(tmp_path, monkeypatch):
    designs = _isolate(tmp_path, monkeypatch)
    assert runner.invoke(app, ["style", "init", "--channel", "digest_channel"]).exit_code == 0
    result = runner.invoke(app, ["style", "show", "--channel", "@digest_channel"])
    assert result.exit_code == 0
    assert str(designs / "digest_channel.md") in result.stdout
    assert "## Voice" in result.stdout


def test_show_missing_fails_with_init_hint(tmp_path, monkeypatch):
    _isolate(tmp_path, monkeypatch)
    result = runner.invoke(app, ["style", "show", "--channel", "digest_channel"])
    assert result.exit_code == 1
    assert "style init" in result.stderr


# --------------------------------------------------------------------------- #
# repo convention — no style spec is ever tracked (#1584 guard)
# --------------------------------------------------------------------------- #


def test_no_design_spec_is_tracked_in_the_repository():
    """The repo tracks no design.md / designs/ artifact — local-only by construction."""
    tracked = subprocess.run(
        ["git", "ls-files"], cwd=SOURCE_ROOT, check=True, capture_output=True, text=True
    ).stdout.splitlines()
    offenders = [
        path
        for path in tracked
        if Path(path).name.lower() == "design.md" or "designs" in Path(path).parts
    ]
    assert offenders == []
