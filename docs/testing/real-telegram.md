# Политика real Telegram testing

Этот документ фиксирует, какие сценарии можно проверять через реальный Telegram API, а какие должны оставаться только fake/harness-based.

Базовое правило: обычный `pytest` и runtime tests в репозитории не должны ходить в реальный Telegram по умолчанию. Live Telegram всегда opt-in, только через специальный fixture и только на выделенном sandbox-аккаунте.

## Обязательные принципы

- Используется только disposable sandbox-аккаунт без личных чатов и без рабочих контактов.
- Все live-тесты работают только с заранее привязанными sandbox-ресурсами: `Saved Messages`, тестовый private chat, тестовый read-only channel, тестовый bot.
- Личный аккаунт разработчика нельзя использовать ни для CI, ни для ручных прогонов из репозитория.
- Generic route/service/unit tests не переводятся на real Telegram client "как есть". Для live API создаются отдельные opt-in сценарии с жёстким sandbox contract.

## Классы сценариев

### 1. Safe automated

Разрешены только read-only операции на sandbox-аккаунте и allowlist-ресурсах. Такие тесты можно запускать автоматически, но только при явном gate `RUN_REAL_TELEGRAM_SAFE=1`.

Разрешённые действия:

- `get_me`, users info, проверка авторизации и подключения
- `get_dialogs`, прогрев entity/dialog cache
- `resolve_channel` и `resolve_entity` для заранее привязанных sandbox-идентификаторов
- `iter_messages`
- channel stats
- `search_my_chats`
- `search_in_channel`
- pool init и read-only checkout paths

Запрещено внутри `real_tg_safe`:

- `send_message`
- `send_file`
- `delete_dialog` / `leave_channels`
- BotFather flows
- auth flows (`send_code`, `resend_code`, `verify_code`)
- Premium global search и quota checks

### 2. Manual-only on sandbox account

Это stateful, quota-spending или внешне заметные действия. Они не входят в обычный `pytest` и запускаются только вручную при `RUN_REAL_TELEGRAM_MANUAL=1`.

Сюда относятся:

- auth flows: `send_code`, `resend_code`, `verify_code`
- BotFather create/delete bot
- direct notifications и любые `send_message`
- photo publish / schedule / batch и любые `send_file`
- Premium global search и `check_search_quota`
- flood / rotation stress
- сценарии, где нужен визуальный/manual cleanup после выполнения

### 3. Never real client

Это тесты, которые нельзя переводить на живой клиент вообще, если у них нет отдельного sandbox-spec и отдельного live сценария.

Сюда относятся:

- generic pytest-кейсы, которые используют динамически найденные реальные диалоги и потом мутируют их
- массовые `leave_channels` / `delete_dialog` без жёсткой привязки к disposable sandbox-чатам
- отправка сообщений, фото или BotFather-команд в не-sandbox чаты
- любые live-прогоны на личном аккаунте
- попытка "просто заменить fake client на реальный" в route/service/unit tests

Если нужен real API coverage для такого поведения, создаётся отдельный manual-only сценарий с явным sandbox contract. Существующий generic test при этом остаётся harness-based.

## Матрица по текущим кодовым путям

### CLI live checks (`src/cli/commands/test.py`)

`Safe automated`:

- `tg_pool_init`
- `tg_users_info`
- `tg_get_dialogs`
- `tg_resolve_channel`
- `tg_iter_messages`
- `tg_channel_stats`
- `tg_search_my_chats`
- `tg_search_in_channel`

`Manual-only`:

- `tg_search_premium`
- `tg_search_quota`

Важно: текущий `test telegram` в одном проходе содержит и safe-, и manual-only шаги. Его нельзя считать CI-safe целиком, пока manual-only шаги не выделены в отдельный режим или не исключены внешним раннером.

### Не переводить на обычный live pytest

Эти потоки остаются fake/harness-based и не должны "в лоб" запускаться с real Telegram client:

- notification setup/delete и BotFather orchestration
- photo send/schedule/batch
- direct notifier sends
- auth login flows
- `my_telegram leave` / `leave_channels`

## Env contract для live tests

Для real Telegram tests используются только выделенные `REAL_TG_*` переменные. Generic `TG_*` переменные не используются намеренно, чтобы тесты не могли случайно подняться на личном аккаунте.

Обязательные переменные:

- `RUN_REAL_TELEGRAM_SAFE=1` для `real_tg_safe`
- `RUN_REAL_TELEGRAM_MANUAL=1` для `real_tg_manual`
- `REAL_TG_API_ID`
- `REAL_TG_API_HASH`
- `REAL_TG_PHONE`
- `REAL_TG_SESSION`

Опциональные sandbox bindings:

- `REAL_TG_READ_CHANNEL_USERNAME`
- `REAL_TG_READ_CHANNEL_ID`
- `REAL_TG_PRIVATE_CHAT_ID`
- `REAL_TG_BOT_USERNAME`

## Pytest contract

Доступные markers:

- `@pytest.mark.real_tg_safe`
- `@pytest.mark.real_tg_manual`
- `@pytest.mark.real_tg_never`

Доступный live fixture:

- `real_telegram_sandbox`

Правила:

- live Telegram test обязан иметь marker `real_tg_safe` или `real_tg_manual`
- live Telegram test обязан использовать `real_telegram_sandbox`
- `real_tg_safe` и `real_tg_manual` по умолчанию не запускаются без соответствующего env gate
- `real_tg_never` несовместим с `real_telegram_sandbox`
- без marker+fixture доступ к real Telegram считается ошибкой конфигурации теста

## Что использовать по умолчанию

Для обычной разработки и CI:

- `RealPoolHarness`
- `FakeCliTelethonClient`
- patched transport из `tests/conftest.py`

Real Telegram нужен только для узких opt-in smoke/manual сценариев, а не как замена существующего test suite.
