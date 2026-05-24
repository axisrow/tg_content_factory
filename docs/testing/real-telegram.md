# Политика real Telegram testing

Этот документ фиксирует два разных класса live-проверок:

- unit/integration pytest с выделенным sandbox-аккаунтом через `REAL_TG_*`;
- ручной операторский CLI inventory, который запускает настоящий CLI против реального `config.yaml` и реальной SQLite DB.

Обычный `pytest`, CI и регулярные локальные проверки не должны читать реальную базу и не должны ходить в Telegram без явного opt-in.

## Базовые правила

- Live Telegram всегда opt-in.
- `real_tg_safe` не означает “можно запускать в CI”; он означает только “нет Telegram-visible mutations”.
- `real_tg_mutation_safe` означает bounded Telegram-visible mutation по явно выбранной оператором цели, например реакция на конкретное сообщение.
- CLI inventory из `tests/cli_real_tg_integration/` предназначен для ручного запуска время от времени оператором на своей live-среде.
- High-risk Telegram-visible write actions остаются `real_tg_manual` и требуют отдельного ручного gate.
- Локальные DB writes допустимы только в явно выделенных CLI folders (`safe_write`, `mutating`, `destructive`) и должны быть idempotent, cleanup-backed или осознанно операторскими.

## Pytest markers

Доступные markers:

- `@pytest.mark.real_tg_safe`
- `@pytest.mark.real_tg_mutation_safe`
- `@pytest.mark.real_tg_manual`
- `@pytest.mark.real_tg_never`

Правила:

- live Telegram test обязан иметь `real_tg_safe`, `real_tg_mutation_safe` или `real_tg_manual`;
- live Telegram test обязан использовать `real_telegram_sandbox` или `cli_real_cli_env`;
- `real_tg_safe` требует `RUN_REAL_TELEGRAM_SAFE=1`;
- `real_tg_mutation_safe` требует `RUN_REAL_TELEGRAM_MUTATION_SAFE=1`;
- `real_tg_manual` требует `RUN_REAL_TELEGRAM_MANUAL=1`;
- `real_tg_never` несовместим с live fixtures;
- без marker + live fixture доступ к real Telegram считается ошибкой конфигурации теста.

## Sandbox pytest tests

Не-CLI live tests продолжают использовать выделенный sandbox contract:

- fixture: `real_telegram_sandbox`;
- обязательные env vars: `REAL_TG_API_ID`, `REAL_TG_API_HASH`, `REAL_TG_PHONE`, `REAL_TG_SESSION`;
- optional bindings: `REAL_TG_READ_CHANNEL_USERNAME`, `REAL_TG_READ_CHANNEL_ID`, `REAL_TG_PRIVATE_CHAT_ID`, `REAL_TG_BOT_USERNAME`.

Такие тесты не должны использовать личный live account или реальные рабочие чаты.

## Manual CLI Inventory

CLI inventory находится в `tests/cli_real_tg_integration/`.

Fixture `cli_real_cli_env` запускает subprocess как:

```bash
python -m src.main --config <real config.yaml> ...
```

Источник live-состояния:

- `CLI_REAL_TG_ROOT`, если задан, иначе текущий checkout;
- `CLI_REAL_TG_CONFIG`, если задан, иначе `<CLI_REAL_TG_ROOT>/config.yaml`;
- `database.path` из `load_config(config_path)`, резолвится относительно `CLI_REAL_TG_ROOT`;
- реальные Telegram credentials/accounts/provider settings из config/DB.

`REAL_TG_*` для CLI subprocess inventory не используются.

Перед запуском fixture проверяет:

- `RUN_CLI_REAL_TG_LIVE=1`;
- существует `config.yaml`;
- существует и не пустая DB из `database.path`;
- в config есть Telegram `api_id`/`api_hash`;
- в DB есть active accounts с `session_string`.

## CLI Folders And Gates

`safe_ro/`

- Read-only smoke commands against live DB/API.
- Gate: `RUN_CLI_REAL_TG_LIVE=1 RUN_REAL_TELEGRAM_SAFE=1`.

`safe_write/`

- Локальные DB writes, которые должны быть cleanup-backed, idempotent или no-op.
- Gate: `RUN_CLI_REAL_TG_LIVE=1 RUN_REAL_TELEGRAM_SAFE=1`.

`heavy/`

- Long-running/API-heavy commands.
- Gate: `RUN_CLI_REAL_TG_LIVE=1 RUN_REAL_TELEGRAM_SAFE=1 RUN_CLI_REAL_TG_HEAVY=1`.

`mutating/`

- Реальные локальные runtime/task/settings mutations.
- Gate: `RUN_CLI_REAL_TG_LIVE=1 RUN_REAL_TELEGRAM_SAFE=1 RUN_CLI_MUTATING=1`.

`mutation_safe/`

- Bounded Telegram-visible mutations по live DB/cache fixtures, например `dialogs react` на собранном сообщении,
  `dialogs mark-read --max-id`, cleanup-backed archive/unarchive, cleanup-backed pin/unpin.
- Gate: `RUN_CLI_REAL_TG_LIVE=1 RUN_REAL_TELEGRAM_MUTATION_SAFE=1`.

`destructive/`

- Process-control commands such as `serve`, `worker`, `stop`, `restart`.
- Gate: `RUN_CLI_REAL_TG_LIVE=1 RUN_REAL_TELEGRAM_MANUAL=1 RUN_CLI_DESTRUCTIVE=1`.

`manual/`

- High-risk Telegram-visible mutations: sends, auth, BotFather, leave/delete/admin/edit/photo publish.
- Pin/unpin are only mutation-safe for own cached dialogs with `--notify` forbidden and cleanup-backed tests.
- Gate: `RUN_CLI_REAL_TG_LIVE=1 RUN_REAL_TELEGRAM_MANUAL=1`.

## Coverage Contract

`tests/test_real_telegram_policy.py` introspects `src.cli.parser_domains` through `build_parser()` and requires every parser leaf command to be either:

- covered by a CLI inventory test; or
- listed in `tests/cli_real_tg_integration/command_manifest.py` as manual/excluded with a reason.

The same policy test audits literal `run_cli(...)`, `run_cli_popen(...)`, and `cli_run_direct(...)` calls and classifies commands exactly. Special cases such as `channel refresh-meta --all` and `channel stats --all` are separate heavy cases and are not allowed by the plain single-command classification.

## Recommended Commands

Default non-live development:

```bash
python3 -m ruff check src/ tests/ conftest.py
python3 -m pytest tests/ -v -m "not aiosqlite_serial and not real_tg_safe and not real_tg_mutation_safe and not real_tg_manual and not real_provider_smoke" -n auto
python3 -m pytest tests/ -v -m "aiosqlite_serial and not real_tg_safe and not real_tg_mutation_safe and not real_tg_manual and not real_provider_smoke"
```

Verify the CLI inventory is disabled by default:

```bash
python3 -m pytest tests/cli_real_tg_integration -q
```

Manual safe read-only CLI inventory:

```bash
RUN_CLI_REAL_TG_LIVE=1 RUN_REAL_TELEGRAM_SAFE=1 \
python3 -m pytest tests/cli_real_tg_integration/safe_ro -v
```

Manual local-write inventory:

```bash
RUN_CLI_REAL_TG_LIVE=1 RUN_REAL_TELEGRAM_SAFE=1 \
python3 -m pytest tests/cli_real_tg_integration/safe_write -v
```

Manual heavy inventory:

```bash
RUN_CLI_REAL_TG_LIVE=1 RUN_REAL_TELEGRAM_SAFE=1 RUN_CLI_REAL_TG_HEAVY=1 \
python3 -m pytest tests/cli_real_tg_integration/heavy -v
```

Manual mutating inventory:

```bash
RUN_CLI_REAL_TG_LIVE=1 RUN_REAL_TELEGRAM_SAFE=1 RUN_CLI_MUTATING=1 \
python3 -m pytest tests/cli_real_tg_integration/mutating -v
```

Manual mutation-safe Telegram-visible inventory:

```bash
RUN_CLI_REAL_TG_LIVE=1 RUN_REAL_TELEGRAM_MUTATION_SAFE=1 \
python3 -m pytest tests/cli_real_tg_integration/mutation_safe -v
```

The mutation-safe inventory discovers targets from the live DB/cache, like the read-only inventory:

- archive/unarchive/mark-read/react use an active collected dialog/message target;
- pin/unpin require an own cached dialog with a collected message, otherwise those tests skip;
- react tests set the requested emoji and then clear it with `dialogs react --clear` / `my-telegram react --clear`;
- pin tests unpin in cleanup, and unpin tests first pin then unpin so the final state is unpinned;
- archive tests unarchive in cleanup, and unarchive tests first archive then unarchive so the final state is unarchived.

`CLI_REAL_TG_REACT_EMOJI` can override the default reaction emoji for react tests.

Manual destructive process-control inventory:

```bash
RUN_CLI_REAL_TG_LIVE=1 RUN_REAL_TELEGRAM_MANUAL=1 RUN_CLI_DESTRUCTIVE=1 \
python3 -m pytest tests/cli_real_tg_integration/destructive -v
```

Manual Telegram-visible scenarios:

```bash
RUN_CLI_REAL_TG_LIVE=1 RUN_REAL_TELEGRAM_MANUAL=1 \
python3 -m pytest tests/cli_real_tg_integration/manual -v
```

## What Not To Do

- Do not add CLI live inventory to CI.
- Do not run it from cron or regular local validation.
- Do not use `REAL_TG_*` sandbox credentials for CLI subprocess inventory.
- Do not add Telegram-visible mutations under `real_tg_safe`.
- Do not add high-risk or broad Telegram-visible mutations under `real_tg_mutation_safe`; keep those under `real_tg_manual`.
- Do not run mutation-safe pin with `--notify` or mutation-safe unpin without `--message-id`.
- Do not run mutation-safe mark-read without `--max-id`.
- Do not rely on “first live row” directly from tests; add a named live fixture with skip guards and cleanup expectations.
- Do not run process-control inventory against a config that already has a managed server PID file; use a separate config or stop the server first.
