# Task Completion Checklist

Run before considering a coding task done (mirrors CI in `.github/workflows/ci.yml`):

1. **Lint**: `ruff check src/ tests/ conftest.py` (use `--fix` to auto-resolve). Must be clean.
2. **Import contract**: `lint-imports --config .importlinter` — entrypoints must not import telethon directly.
3. **Warnings-are-errors**: ensure no new warnings — `filterwarnings = ["error"]` in pyproject; CI has a dedicated step that fails if this is relaxed. Fix warnings, never suppress.
4. **Tests (parallel)**: `pytest tests/ -v -m "not aiosqlite_serial" -n auto`.
5. **Tests (serial)**: `pytest tests/ -v -m aiosqlite_serial`.

Extra guards depending on what changed:
- Touched SQL/JOINs → `tests/test_sql_conventions.py` must pass (enforces `channel_id` JOIN rule).
- Added/changed a CLI command → keep CLI/Web parity; real-CLI coverage policy in `tests/cli_real_tg_integration/` (`test_real_telegram_policy.py`) requires every leaf command covered or manifested.
- Never convert mutating Telegram flows (BotFather, photo send, auth, leave_channels) to generic live pytest.

Branch policy: never commit to `main` — always a feature branch + PR (branch names with `+` are rejected by CI).
