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
- Локальные DB writes допустимы только в явно выделенных CLI folders (`safe_write`, `mutating`, `process_control`) и должны быть idempotent, cleanup-backed или осознанно операторскими.

## Pytest markers

Доступные markers:

- `@pytest.mark.real_tg_safe`
- `@pytest.mark.real_tg_mutation_safe`
- `@pytest.mark.real_tg_manual`
- `@pytest.mark.real_tg_never`

Правила:

- live Telegram test обязан иметь `real_tg_safe`, `real_tg_mutation_safe` или `real_tg_manual`;
- live Telegram test обязан использовать `real_telegram_sandbox` или `cli_real_cli_env`;
- `real_tg_safe` требует `RUN_REAL_TELEGRAM_SAFE=1` (или авто, см. ниже);
- `real_tg_mutation_safe` требует `RUN_REAL_TELEGRAM_MUTATION_SAFE=1` (или авто);
- `real_tg_manual` требует `RUN_REAL_TELEGRAM_MANUAL=1` (или авто);
- `real_tg_never` несовместим с live fixtures;
- без marker + live fixture доступ к real Telegram считается ошибкой конфигурации теста.

### Авто-включение по готовности проекта

CLI live-инвентарь (`cli_real_cli_env`-тесты) теперь авто-включается, когда проект реально
настроен для live, без ручного выставления `RUN_*`-гейтов. Предикат
`live_cli_project_ready()` (`tests/cli_real_tg_integration/_live_readiness.py`) истинен,
когда: есть `config.yaml`, существует и не пуста DB из `database.path`, заданы `api_id`/`api_hash`
(в config/env **или** в settings-таблице DB по ключам `tg_api_id`/`tg_api_hash`), и в DB есть
хотя бы один active account с `session_string`.

Все `RUN_*`-гейты остаются опциональным override через единый помощник `_gate_enabled`:

- env задан в `1/true/yes/on` → форс-вкл;
- env задан в `0/false/no/off` → форс-выкл (kill switch даже на готовом проекте);
- env не задан → авто по `live_cli_project_ready()`.

Категория выбирается путём (подкаталогом) в команде pytest; запуск родительской папки
`tests/cli_real_tg_integration/` на готовом проекте собирает все категории. В CI (нет
заполненной DB/accounts) предикат ложен → все гейты закрыты → graceful skip, без обращений
к Telegram. Авто-включение применяется только к CLI-live fixture; sandbox-fixture
(`real_telegram_sandbox`) остаётся строго env-gated.

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

- гейт `RUN_CLI_REAL_TG_LIVE` открыт (`=1`, либо не задан и проект live-ready — см.
  «Авто-включение по готовности проекта»);
- существует `config.yaml`;
- существует и не пустая DB из `database.path`;
- заданы Telegram `api_id`/`api_hash` — в config/env **или** в settings-таблице DB
  (`tg_api_id`/`tg_api_hash`), тем же fallback-порядком, что и продовый `init_pool`;
- в DB есть active accounts с `session_string`;
- хотя бы один active account проходит read-only readiness probe:
  `python -m src.main --config <config.yaml> account info --phone <phone>`.

Readiness probe ждет не просто `is_active=1`, а фактическое CLI-подключение к Telegram. По текущим live-логам
успешное подключение занимает секунды, самый медленный нормальный auth/connect был около 26 секунд, поэтому default
wait — 60 секунд. Для плохой сети можно поднять его вручную:

- `CLI_REAL_TG_CONNECT_WAIT_SECONDS=60` — общий лимит ожидания active account + успешного probe;
- `CLI_REAL_TG_CONNECT_POLL_SECONDS=2` — пауза между проверками DB/probe;
- `CLI_REAL_TG_CONNECT_PROBE_TIMEOUT_SECONDS=60` — timeout одного `account info --phone` probe;
- `CLI_REAL_TG_PHONE=+...` — опционально фиксирует конкретный active account для readiness probe и live CLI
  inventory. Используйте это, когда старые active accounts еще имеют `flood_wait_until`, а проверять нужно свежий
  подключенный аккаунт.

Без `CLI_REAL_TG_PHONE` fixture сначала пробует active accounts без актуального `flood_wait_until`, и только потом
flood-waited accounts.

## CLI Folders And Gates

Гейты ниже — это override: на live-ready проекте они авто-открываются без env (см.
«Авто-включение по готовности проекта»). Перечисленные `RUN_*=1` нужны только чтобы
форсировать запуск на не-готовом проекте или в CI; `RUN_*=0` форсирует skip даже на
готовом проекте.

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
  `dialogs mark-read --max-id`, cleanup-backed archive/unarchive, cleanup-backed pin/unpin, scratch-message
  send/edit with final edit cleanup.
- Gate: `RUN_CLI_REAL_TG_LIVE=1 RUN_REAL_TELEGRAM_MUTATION_SAFE=1`.

`process_control/`

- Process-control commands such as `serve`, `worker`, `stop`, `restart`.
- Gate: `RUN_CLI_REAL_TG_LIVE=1 RUN_REAL_TELEGRAM_MANUAL=1 RUN_CLI_PROCESS_CONTROL=1`.

`manual/`

- High-risk Telegram-visible mutations: unbounded sends, auth, BotFather, leave/delete/admin/permissions/photo publish.
- Pin/unpin are only mutation-safe for own cached dialogs with `--notify` forbidden and cleanup-backed tests.
- Gate: `RUN_CLI_REAL_TG_LIVE=1 RUN_REAL_TELEGRAM_MANUAL=1`.

`dangerous/`

- Account/data-destruction commands, маркированы `real_tg_never` — никогда не запрашивают live
  fixture и не имеют env-гейта (только статический policy-аудит).

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

Чтобы прогнать heavy inventory через конкретный свежий аккаунт:

```bash
RUN_CLI_REAL_TG_LIVE=1 RUN_REAL_TELEGRAM_SAFE=1 RUN_CLI_REAL_TG_HEAVY=1 \
CLI_REAL_TG_PHONE=+70000000000 \
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
- scratch send/edit tests use `CLI_REAL_TG_MUTATION_CHAT` when set, otherwise an own cached dialog; they do not delete
  the sent marker and leave it as a final `codex live cli edit test completed ...` message;
- react tests set the requested emoji and then clear it with `dialogs react --clear` / `my-telegram react --clear`;
- pin tests unpin in cleanup, and unpin tests first pin then unpin so the final state is unpinned;
- archive tests unarchive in cleanup, and unarchive tests first archive then unarchive so the final state is unarchived.

`CLI_REAL_TG_REACT_EMOJI` can override the default reaction emoji for react tests.
`CLI_REAL_TG_MUTATION_CHAT` can pin scratch-message tests to a specific test chat, and
`CLI_REAL_TG_MUTATION_PHONE` can pin the connected account used for that chat.
If the test account was just toggled/authenticated from the UI, the fixture waits up to
`CLI_REAL_TG_CONNECT_WAIT_SECONDS` for a real `account info --phone` probe before running mutation-safe commands.
Use `CLI_REAL_TG_CONNECT_WAIT_SECONDS=180` only as an explicit poor-network override, not as the default.

Manual process-control inventory:

```bash
RUN_CLI_REAL_TG_LIVE=1 RUN_REAL_TELEGRAM_MANUAL=1 RUN_CLI_PROCESS_CONTROL=1 \
python3 -m pytest tests/cli_real_tg_integration/process_control -v
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
- Do not clean up mutation-safe scratch messages with `delete-message`; use a final `edit-message` only.
- Do not rely on “first live row” directly from tests; add a named live fixture with skip guards and cleanup expectations.
- Do not run process-control inventory against a config that already has a managed server PID file; use a separate config or stop the server first.
