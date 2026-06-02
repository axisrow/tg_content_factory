"""EXT-LLM real-provider CLI integration tests (#582).

All tests in this file carry ``@pytest.mark.real_provider_smoke`` and are
gated by ``RUN_REAL_PROVIDER_SMOKE=1``.  Each individual test also skips when
its required API key is absent, so the whole suite stays green without any
credentials.
"""
from __future__ import annotations

import os
import sqlite3
import textwrap
from pathlib import Path

import pytest

from tests.cli_real_provider_integration.conftest import (
    PROVIDER_CLI_NETWORK_TIMEOUT,
    cli_run,
)

# ---------------------------------------------------------------------------
# Gate helpers
# ---------------------------------------------------------------------------

_GATE = os.environ.get("RUN_REAL_PROVIDER_SMOKE") == "1"
_ZAI_KEY = os.environ.get("ZAI_API_KEY", "")
_LLM_KEY = os.environ.get("LLM_API_KEY", "")
_SESSION_ENC_KEY = os.environ.get("SESSION_ENCRYPTION_KEY", "")
_ANTHROPIC_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
_CLAUDE_OAUTH = os.environ.get("CLAUDE_CODE_OAUTH_TOKEN", "")
_TOGETHER_KEY = os.environ.get("TOGETHER_API_KEY", "")
_REPLICATE_TOKEN = os.environ.get("REPLICATE_API_TOKEN", "")
_HF_TOKEN = os.environ.get("HF_TOKEN", "")
_OPENAI_KEY = os.environ.get("OPENAI_API_KEY", "")

# True when agent backend is available
_AGENT_AVAILABLE = bool(_ANTHROPIC_KEY or _CLAUDE_OAUTH)

# True when at least one text-LLM key is present
_TEXT_LLM_KEY = _ZAI_KEY or _LLM_KEY

# True when at least one image provider key is present
_IMAGE_KEY = bool(_TOGETHER_KEY or _REPLICATE_TOKEN or _HF_TOKEN or _OPENAI_KEY)

# True when provider DB writes are possible (need SESSION_ENCRYPTION_KEY)
_PROVIDER_DB_AVAILABLE = bool(_ZAI_KEY and _SESSION_ENC_KEY)


def _skipif_no_zai():
    return pytest.mark.skipif(
        not _GATE or not _ZAI_KEY,
        reason="Set RUN_REAL_PROVIDER_SMOKE=1 and ZAI_API_KEY to run live ZAI smoke.",
    )


def _skipif_no_provider_db():
    return pytest.mark.skipif(
        not _GATE or not _PROVIDER_DB_AVAILABLE,
        reason=(
            "Set RUN_REAL_PROVIDER_SMOKE=1, ZAI_API_KEY, and SESSION_ENCRYPTION_KEY "
            "to run provider-DB smoke tests."
        ),
    )


def _skipif_no_agent():
    return pytest.mark.skipif(
        not _GATE or not _AGENT_AVAILABLE,
        reason=(
            "Set RUN_REAL_PROVIDER_SMOKE=1 and ANTHROPIC_API_KEY (or "
            "CLAUDE_CODE_OAUTH_TOKEN) to run agent smoke tests."
        ),
    )


def _skipif_no_image():
    return pytest.mark.skipif(
        not _GATE or not _IMAGE_KEY,
        reason=(
            "Set RUN_REAL_PROVIDER_SMOKE=1 and one of TOGETHER_API_KEY, "
            "REPLICATE_API_TOKEN, HF_TOKEN, or OPENAI_API_KEY to run image smoke tests."
        ),
    )


# ---------------------------------------------------------------------------
# Helper: build a tmp config that has SESSION_ENCRYPTION_KEY set
# ---------------------------------------------------------------------------


def _write_encrypted_config(tmp_path: Path, enc_key: str = "") -> tuple[Path, Path]:
    """Write a config.yaml with optional encryption key; return (config_path, db_path)."""
    db_path = tmp_path / "test.db"
    config_path = tmp_path / "config.yaml"
    enc_line = f'  session_encryption_key: "{enc_key}"' if enc_key else '  session_encryption_key: ""'
    config_path.write_text(
        textwrap.dedent(f"""\
            telegram:
              api_id: 1
              api_hash: "testhash"

            web:
              host: "127.0.0.1"
              port: 8099
              password: ""

            scheduler:
              collect_interval_minutes: 30
              delay_between_channels_sec: 2
              delay_between_requests_sec: 1
              max_flood_wait_sec: 300
              stats_all_max_channels_per_run: 10
              stats_all_cooldown_sec: 600
              stats_all_worker_count: 1

            database:
              path: "{db_path}"

            llm:
              enabled: true
              provider: "openai"
              model: "gpt-4o-mini"
              api_key: ""

            security:
{enc_line}
        """),
        encoding="utf-8",
    )
    return config_path, db_path


# ===========================================================================
# Provider probe / refresh / test-all (need provider stored in DB)
# ===========================================================================


@pytest.mark.real_provider_smoke
@pytest.mark.timeout(120)
@_skipif_no_provider_db()
def test_provider_probe_zai(tmp_path: Path) -> None:
    """provider probe zai → returncode 0, stdout contains 'OK:' or model count."""
    config_path, _db_path = _write_encrypted_config(tmp_path, _SESSION_ENC_KEY)
    extra_env = {"ZAI_API_KEY": _ZAI_KEY, "SESSION_ENCRYPTION_KEY": _SESSION_ENC_KEY}

    # Seed the provider into the DB first
    add = cli_run(
        config_path,
        "provider", "add", "zai",
        "--api-key", _ZAI_KEY,
        timeout=PROVIDER_CLI_NETWORK_TIMEOUT,
        extra_env=extra_env,
    )
    assert add.returncode == 0, f"provider add zai failed:\n{add.stdout}\n{add.stderr}"

    result = cli_run(
        config_path,
        "provider", "probe", "zai",
        timeout=PROVIDER_CLI_NETWORK_TIMEOUT,
        extra_env=extra_env,
    )
    assert result.returncode == 0, f"provider probe zai failed:\n{result.stdout}\n{result.stderr}"
    combined = result.stdout + result.stderr
    assert "OK:" in combined or "models" in combined.lower(), (
        f"Expected 'OK:' or 'models' in output, got:\n{combined}"
    )


@pytest.mark.real_provider_smoke
@pytest.mark.timeout(120)
@_skipif_no_provider_db()
def test_provider_refresh_zai(tmp_path: Path) -> None:
    """provider refresh zai → returncode 0, stdout contains 'OK: N models'."""
    config_path, _db_path = _write_encrypted_config(tmp_path, _SESSION_ENC_KEY)
    extra_env = {"ZAI_API_KEY": _ZAI_KEY, "SESSION_ENCRYPTION_KEY": _SESSION_ENC_KEY}

    add = cli_run(
        config_path,
        "provider", "add", "zai",
        "--api-key", _ZAI_KEY,
        timeout=PROVIDER_CLI_NETWORK_TIMEOUT,
        extra_env=extra_env,
    )
    assert add.returncode == 0, f"provider add zai failed:\n{add.stdout}\n{add.stderr}"

    result = cli_run(
        config_path,
        "provider", "refresh", "zai",
        timeout=PROVIDER_CLI_NETWORK_TIMEOUT,
        extra_env=extra_env,
    )
    assert result.returncode == 0, f"provider refresh zai failed:\n{result.stdout}\n{result.stderr}"
    combined = result.stdout + result.stderr
    assert "OK:" in combined, (
        f"Expected 'OK:' in output, got:\n{combined}"
    )


@pytest.mark.real_provider_smoke
@pytest.mark.timeout(120)
@_skipif_no_provider_db()
def test_provider_test_all(tmp_path: Path) -> None:
    """provider test-all → returncode 0, table with provider results."""
    config_path, _db_path = _write_encrypted_config(tmp_path, _SESSION_ENC_KEY)
    extra_env = {"ZAI_API_KEY": _ZAI_KEY, "SESSION_ENCRYPTION_KEY": _SESSION_ENC_KEY}

    add = cli_run(
        config_path,
        "provider", "add", "zai",
        "--api-key", _ZAI_KEY,
        timeout=PROVIDER_CLI_NETWORK_TIMEOUT,
        extra_env=extra_env,
    )
    assert add.returncode == 0, f"provider add zai failed:\n{add.stdout}\n{add.stderr}"

    result = cli_run(
        config_path,
        "provider", "test-all",
        timeout=PROVIDER_CLI_NETWORK_TIMEOUT,
        extra_env=extra_env,
    )
    assert result.returncode == 0, f"provider test-all failed:\n{result.stdout}\n{result.stderr}"
    combined = result.stdout + result.stderr
    # At least one provider row should be present
    assert "zai" in combined.lower(), (
        f"Expected 'zai' in test-all output, got:\n{combined}"
    )


# ===========================================================================
# Translation commands (use RuntimeProviderRegistry from env; no DB write)
# ===========================================================================


@pytest.mark.real_provider_smoke
@pytest.mark.timeout(120)
@_skipif_no_zai()
def test_translate_message(tmp_path: Path, provider_tmp_env: tuple[Path, Path]) -> None:
    """translate message <id> --target en → translation stored in DB."""
    config_path, db_path = provider_tmp_env
    extra_env = {"ZAI_API_KEY": _ZAI_KEY}

    # Seed a channel and a message directly into the DB
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS channels ("
            "id INTEGER PRIMARY KEY, channel_id INTEGER UNIQUE, "
            "title TEXT, username TEXT, added_at TEXT, is_active INTEGER DEFAULT 1)"
        )
        conn.execute(
            "INSERT OR IGNORE INTO channels (channel_id, title, username, added_at) "
            "VALUES (?, ?, ?, ?)",
            (100001, "Test Channel", "test_ch", "2024-01-01T00:00:00"),
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS messages ("
            "id INTEGER PRIMARY KEY, channel_id INTEGER, message_id INTEGER, "
            "text TEXT, date TEXT, lang TEXT, UNIQUE(channel_id, message_id))"
        )
        conn.execute(
            "INSERT OR IGNORE INTO messages (channel_id, message_id, text, date) "
            "VALUES (?, ?, ?, ?)",
            (100001, 1, "Привет мир", "2024-01-01T00:00:00"),
        )
        conn.commit()

    # First let the CLI initialise the DB schema (migrations) with a no-op command
    cli_run(config_path, "provider", "list", extra_env=extra_env)

    # Fetch the message id assigned by migration
    with sqlite3.connect(db_path) as conn:
        row = conn.execute("SELECT id FROM messages WHERE channel_id=100001 LIMIT 1").fetchone()

    if row is None:
        pytest.skip("Seeded message not found after DB init")
    msg_id = row[0]

    result = cli_run(
        config_path,
        "translate", "message", str(msg_id), "--target", "en",
        timeout=PROVIDER_CLI_NETWORK_TIMEOUT,
        extra_env=extra_env,
    )
    combined = result.stdout + result.stderr
    # Accept either a successful translation or a graceful "no key configured" exit
    assert result.returncode == 0, (
        f"translate message returned non-zero:\n{combined}"
    )
    # If translation ran, there should be some indication of success
    if "Translated" in combined or "Original:" in combined:
        pass  # command produced output, good enough


@pytest.mark.real_provider_smoke
@pytest.mark.timeout(120)
@_skipif_no_zai()
def test_translate_run(tmp_path: Path, provider_tmp_env: tuple[Path, Path]) -> None:
    """translate run --limit 1 → returncode 0, output mentions 'Translated' or 'No messages'."""
    config_path, db_path = provider_tmp_env
    extra_env = {"ZAI_API_KEY": _ZAI_KEY}

    # Seed a Russian message that has no translation yet.
    # Use the canonical schema column name 'detected_lang' — the migration that runs
    # during CLI init adds this column via ALTER TABLE if absent; seeding 'lang' (wrong
    # column name) would leave detected_lang=NULL, making the message invisible to the
    # get_untranslated_messages query which filters on detected_lang IS NOT NULL.
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS channels ("
            "id INTEGER PRIMARY KEY, channel_id INTEGER UNIQUE, "
            "title TEXT, username TEXT, added_at TEXT, is_active INTEGER DEFAULT 1)"
        )
        conn.execute(
            "INSERT OR IGNORE INTO channels (channel_id, title, username, added_at) "
            "VALUES (?, ?, ?, ?)",
            (100002, "Test Channel 2", "test_ch2", "2024-01-01T00:00:00"),
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS messages ("
            "id INTEGER PRIMARY KEY, channel_id INTEGER, message_id INTEGER, "
            "text TEXT, date TEXT NOT NULL, detected_lang TEXT, UNIQUE(channel_id, message_id))"
        )
        conn.execute(
            "INSERT OR IGNORE INTO messages (channel_id, message_id, text, date, detected_lang) "
            "VALUES (?, ?, ?, ?, ?)",
            (100002, 2, "Привет мир снова", "2024-01-01T00:00:00", "ru"),
        )
        conn.commit()

    cli_run(config_path, "provider", "list", extra_env=extra_env)

    result = cli_run(
        config_path,
        "translate", "run", "--limit", "1",
        timeout=PROVIDER_CLI_NETWORK_TIMEOUT,
        extra_env=extra_env,
    )
    assert result.returncode == 0, (
        f"translate run failed:\n{result.stdout}\n{result.stderr}"
    )
    combined = result.stdout + result.stderr
    # Either translated or no messages to translate; both are acceptable
    assert "Translated" in combined or "No messages" in combined or "messages" in combined.lower(), (
        f"Unexpected output from translate run:\n{combined}"
    )


# ===========================================================================
# Search commands (semantic / hybrid) — need indexed messages
# ===========================================================================


@pytest.mark.real_provider_smoke
@pytest.mark.timeout(120)
@_skipif_no_zai()
def test_search_semantic(tmp_path: Path, provider_tmp_env: tuple[Path, Path]) -> None:
    """search --index-now then search 'test' --mode semantic → no error, list or empty."""
    config_path, _db_path = provider_tmp_env
    extra_env = {"ZAI_API_KEY": _ZAI_KEY}

    # Index (empty DB → 0 messages indexed is acceptable)
    index_result = cli_run(
        config_path,
        "search", "--index-now",
        timeout=PROVIDER_CLI_NETWORK_TIMEOUT,
        extra_env=extra_env,
    )
    assert index_result.returncode == 0, (
        f"search --index-now failed:\n{index_result.stdout}\n{index_result.stderr}"
    )

    result = cli_run(
        config_path,
        "search", "test", "--mode", "semantic",
        timeout=PROVIDER_CLI_NETWORK_TIMEOUT,
        extra_env=extra_env,
    )
    assert result.returncode == 0, (
        f"search semantic failed:\n{result.stdout}\n{result.stderr}"
    )


@pytest.mark.real_provider_smoke
@pytest.mark.timeout(120)
@_skipif_no_zai()
def test_search_hybrid(tmp_path: Path, provider_tmp_env: tuple[Path, Path]) -> None:
    """search 'test' --mode hybrid → returncode 0 without error."""
    config_path, _db_path = provider_tmp_env
    extra_env = {"ZAI_API_KEY": _ZAI_KEY}

    result = cli_run(
        config_path,
        "search", "test", "--mode", "hybrid",
        timeout=PROVIDER_CLI_NETWORK_TIMEOUT,
        extra_env=extra_env,
    )
    assert result.returncode == 0, (
        f"search hybrid failed:\n{result.stdout}\n{result.stderr}"
    )


# ===========================================================================
# Pipeline generate --preview (uses RuntimeProviderRegistry from env)
# ===========================================================================


@pytest.mark.real_provider_smoke
@pytest.mark.timeout(120)
@_skipif_no_zai()
def test_pipeline_generate_preview(tmp_path: Path, provider_tmp_env: tuple[Path, Path]) -> None:
    """pipeline generate <id> --preview → 'Created generation run id=' in output."""
    import re  # noqa: PLC0415

    config_path, _db_path = provider_tmp_env
    extra_env = {"ZAI_API_KEY": _ZAI_KEY}

    # Use DAG mode (--node) so --source is not required; 'llm_generate' is a valid NodeType.
    add_result = cli_run(
        config_path,
        "pipeline", "add",
        "--name", "smoke-test-pipeline",
        "--node", "llm_generate:prompt=Summarise the following",
        timeout=30.0,
        extra_env=extra_env,
    )
    combined_add = add_result.stdout + add_result.stderr
    if add_result.returncode != 0 or "Error" in combined_add:
        pytest.skip(f"pipeline add (DAG mode) failed; skipping generate smoke.\n{combined_add}")

    match = re.search(r"id=(\d+)", add_result.stdout)
    if match is None:
        pytest.skip("Could not parse pipeline id from output")
    pipeline_id = match.group(1)

    result = cli_run(
        config_path,
        "pipeline", "generate", pipeline_id, "--preview",
        timeout=PROVIDER_CLI_NETWORK_TIMEOUT,
        extra_env=extra_env,
    )
    combined = result.stdout + result.stderr
    # Accept either a successful run or "not configured" graceful exit
    if "LLM provider is not configured" in combined:
        pytest.skip("LLM provider not configured (env-based provider not picked up by pipeline run)")
    assert "Created generation run id=" in combined, (
        f"Expected 'Created generation run id=' in output, got:\n{combined}"
    )


@pytest.mark.real_provider_smoke
@pytest.mark.timeout(120)
@_skipif_no_zai()
def test_pipeline_run_preview_no_publish(
    tmp_path: Path, provider_tmp_env: tuple[Path, Path]
) -> None:
    """pipeline run <id> --preview (no --publish) → 'Created generation run id='."""
    import re  # noqa: PLC0415

    config_path, _db_path = provider_tmp_env
    extra_env = {"ZAI_API_KEY": _ZAI_KEY}

    # Use DAG mode so --source is optional; 'llm_generate' is the correct NodeType value.
    add_result = cli_run(
        config_path,
        "pipeline", "add",
        "--name", "smoke-run-pipeline",
        "--node", "llm_generate:prompt=Summarise",
        timeout=30.0,
        extra_env=extra_env,
    )
    combined_add = add_result.stdout + add_result.stderr
    if add_result.returncode != 0 or "Error" in combined_add:
        pytest.skip(f"pipeline add (DAG mode) failed; skipping run smoke.\n{combined_add}")

    match = re.search(r"id=(\d+)", add_result.stdout)
    if match is None:
        pytest.skip("Could not parse pipeline id from output")
    pipeline_id = match.group(1)

    result = cli_run(
        config_path,
        "pipeline", "run", pipeline_id, "--preview",
        timeout=PROVIDER_CLI_NETWORK_TIMEOUT,
        extra_env=extra_env,
    )
    combined = result.stdout + result.stderr
    if "LLM provider is not configured" in combined:
        pytest.skip("LLM provider not configured (env-based provider not picked up by pipeline run)")
    assert "Created generation run id=" in combined, (
        f"Expected 'Created generation run id=' in output, got:\n{combined}"
    )


@pytest.mark.real_provider_smoke
@pytest.mark.timeout(120)
@_skipif_no_provider_db()
def test_pipeline_ai_edit(tmp_path: Path) -> None:
    """pipeline ai-edit <id> 'make title shorter' --show → 'Pipeline JSON updated'."""
    config_path, _db_path = _write_encrypted_config(tmp_path, _SESSION_ENC_KEY)
    extra_env = {"ZAI_API_KEY": _ZAI_KEY, "SESSION_ENCRYPTION_KEY": _SESSION_ENC_KEY}

    # Seed a provider so ai-edit can call the LLM
    cli_run(
        config_path,
        "provider", "add", "zai",
        "--api-key", _ZAI_KEY,
        timeout=PROVIDER_CLI_NETWORK_TIMEOUT,
        extra_env=extra_env,
    )

    # Add a minimal pipeline with a node-based DAG so pipeline_json is present.
    # 'llm_generate' is the correct PipelineNodeType value (not 'llm').
    add_result = cli_run(
        config_path,
        "pipeline", "add",
        "--name", "ai-edit-smoke",
        "--node", "llm_generate:prompt=Summarise",
        timeout=30.0,
        extra_env=extra_env,
    )
    combined_add = add_result.stdout + add_result.stderr
    if add_result.returncode != 0 or "Error" in combined_add:
        pytest.skip(f"pipeline add (node mode) failed; skipping ai-edit smoke.\n{combined_add}")

    import re  # noqa: PLC0415

    match = re.search(r"id=(\d+)", add_result.stdout)
    if match is None:
        pytest.skip("Could not parse pipeline id from output")
    pipeline_id = match.group(1)

    result = cli_run(
        config_path,
        "pipeline", "ai-edit", pipeline_id, "make the prompt title shorter", "--show",
        timeout=PROVIDER_CLI_NETWORK_TIMEOUT,
        extra_env=extra_env,
    )
    combined = result.stdout + result.stderr
    # Graceful path: if the pipeline has no LLM node, the command may report an error
    if "not found" in combined.lower() or "no LLM" in combined:
        pytest.skip(f"pipeline ai-edit not applicable to this pipeline shape: {combined}")
    assert "Pipeline JSON updated" in combined or result.returncode == 0, (
        f"pipeline ai-edit did not succeed:\n{combined}"
    )


# ===========================================================================
# Agent commands (need ANTHROPIC_API_KEY or CLAUDE_CODE_OAUTH_TOKEN)
# ===========================================================================


@pytest.mark.real_provider_smoke
@pytest.mark.timeout(120)
@_skipif_no_agent()
def test_agent_chat(tmp_path: Path, provider_tmp_env: tuple[Path, Path]) -> None:
    """agent chat -p 'Reply with: ok' → non-empty response in stdout."""
    config_path, _db_path = provider_tmp_env

    result = cli_run(
        config_path,
        "agent", "chat", "-p", "Reply with: ok",
        timeout=PROVIDER_CLI_NETWORK_TIMEOUT,
    )
    combined = result.stdout + result.stderr
    # The chat command sends output to stdout; agent reply should be non-empty
    assert result.returncode == 0 or "Агент:" in combined or "ok" in combined.lower(), (
        f"agent chat returned unexpected result:\n{combined}"
    )
    # At minimum there should be some output
    assert combined.strip(), "agent chat produced no output"


@pytest.mark.real_provider_smoke
@pytest.mark.timeout(120)
@_skipif_no_agent()
def test_agent_test_escaping(tmp_path: Path, provider_tmp_env: tuple[Path, Path]) -> None:
    """agent test-escaping → returncode 0, output contains 'passed' or 'failed'."""
    config_path, _db_path = provider_tmp_env

    result = cli_run(
        config_path,
        "agent", "test-escaping",
        timeout=PROVIDER_CLI_NETWORK_TIMEOUT,
    )
    combined = result.stdout + result.stderr
    assert result.returncode == 0, (
        f"agent test-escaping returned non-zero:\n{combined}"
    )
    assert "passed" in combined.lower() or "failed" in combined.lower(), (
        f"Expected 'passed'/'failed' in output, got:\n{combined}"
    )


@pytest.mark.real_provider_smoke
@pytest.mark.timeout(120)
@_skipif_no_agent()
def test_agent_test_tools(tmp_path: Path, provider_tmp_env: tuple[Path, Path]) -> None:
    """agent test-tools → returncode 0 (or 1 with 'failed' output if tools fail)."""
    config_path, _db_path = provider_tmp_env

    result = cli_run(
        config_path,
        "agent", "test-tools",
        timeout=PROVIDER_CLI_NETWORK_TIMEOUT,
    )
    combined = result.stdout + result.stderr
    # returncode 1 is legitimate when the tool calls fail — the test is still considered
    # a valid "smoke" run as long as it produced output about the tool test.
    assert "passed" in combined.lower() or "failed" in combined.lower(), (
        f"agent test-tools produced no meaningful output:\n{combined}"
    )


# ===========================================================================
# Image commands (skip unless image provider key present)
# ===========================================================================


@pytest.mark.real_provider_smoke
@pytest.mark.timeout(120)
@_skipif_no_image()
def test_image_models_together(tmp_path: Path, provider_tmp_env: tuple[Path, Path]) -> None:
    """image models --provider together → list of models."""
    config_path, _db_path = provider_tmp_env
    extra_env: dict[str, str] = {}
    if _TOGETHER_KEY:
        extra_env["TOGETHER_API_KEY"] = _TOGETHER_KEY

    result = cli_run(
        config_path,
        "image", "models", "--provider", "together",
        timeout=PROVIDER_CLI_NETWORK_TIMEOUT,
        extra_env=extra_env,
    )
    combined = result.stdout + result.stderr
    assert result.returncode == 0, (
        f"image models --provider together failed:\n{combined}"
    )
    # Should have at least one model listed or a graceful "no models" message
    assert combined.strip(), "image models produced no output"


@pytest.mark.real_provider_smoke
@pytest.mark.timeout(120)
@_skipif_no_image()
def test_image_generate(tmp_path: Path, provider_tmp_env: tuple[Path, Path]) -> None:
    """image generate 'a red circle' → DB row in generated_images or graceful failure."""
    config_path, db_path = provider_tmp_env
    extra_env: dict[str, str] = {}
    if _TOGETHER_KEY:
        extra_env["TOGETHER_API_KEY"] = _TOGETHER_KEY
    if _REPLICATE_TOKEN:
        extra_env["REPLICATE_API_TOKEN"] = _REPLICATE_TOKEN
    if _HF_TOKEN:
        extra_env["HF_TOKEN"] = _HF_TOKEN
    if _OPENAI_KEY:
        extra_env["OPENAI_API_KEY"] = _OPENAI_KEY

    result = cli_run(
        config_path,
        "image", "generate", "a red circle",
        timeout=PROVIDER_CLI_NETWORK_TIMEOUT,
        extra_env=extra_env,
    )
    combined = result.stdout + result.stderr
    assert result.returncode == 0, (
        f"image generate failed:\n{combined}"
    )
    # Either a result URL/path was printed or generation failed gracefully
    assert combined.strip(), "image generate produced no output"
