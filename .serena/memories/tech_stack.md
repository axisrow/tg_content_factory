# Tech Stack

- **Language**: Python `>=3.11` (dev host runs 3.11). Async everywhere (asyncio).
- **Package name**: `tg-agent` (pyproject). Install dev deps: `pip install -e ".[dev]"` (no uv/poetry for the project itself).
- **Web**: FastAPI `>=0.136,<0.137`; HTMX-driven UI (progressive enhancement). Auth = HTTP Basic, password only via `WEB_PASS`, username hardcoded `admin`.
- **DB**: SQLite via `aiosqlite` (single file, WAL, autocommit). No ORM — repositories + Pydantic mapping.
- **Models**: Pydantic v2 (`model_validate`, not `parse_obj`).
- **Telegram**: Telethon `>=1.43`; multi-account via `ClientPool` with StringSession. Sessions stored `enc:v2:*` when `SESSION_ENCRYPTION_KEY` set.
- **Scheduler**: APScheduler `>=3.11`.
- **Config**: `config.yaml` with `${ENV_VAR}` substitution; a key whose value is purely `${VAR}` is dropped if the var is empty/absent.
- **LLM/providers**: `ProviderService` auto-registers from env (`OPENAI_API_KEY`, `ANTHROPIC_API_KEY`/`CLAUDE_CODE_OAUTH_TOKEN`, `COHERE_API_KEY`, `OLLAMA_BASE`, …); lightweight HTTP adapters in `src/services/provider_adapters.py` (no heavy SDK deps). Image models use `provider:model_id` strings.
- **Agent backends**: auto-select order `deepagents` → `claude-agent-sdk` → `deepagents`; Codex SDK backend is opt-in only (not in fallback chain).
- **Lint/test**: ruff `>=0.15`; pytest `>=9` + pytest-asyncio (`asyncio_mode="auto"`), pytest-xdist, pytest-timeout (120s), pytest-cov.
- **Optional**: S3-compatible media store (`src/services/s3_store.py`) when S3 config present.
