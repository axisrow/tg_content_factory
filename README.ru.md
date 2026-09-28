# TG Agent

[![Release](https://img.shields.io/github/v/release/axisrow/tg_content_factory)](https://github.com/axisrow/tg_content_factory/releases)

Персональный инструмент автоматизации Telegram, созданный для управления **агентами кодирования** — Claude Code, OpenCode, Codex. Установите CLI, добавьте скилл — и ваш агент сможет действовать в Telegram за вас: читать и отправлять сообщения, управлять каналами и чатами, скачивать медиа, гонять контент-фабрику на LLM. Контент-фабрика (сбор → поиск → генерация → публикация) — один из модулей.

[English version](README.md)

## Как это устроено

- **`tg-agent` CLI** — поверхность продукта: ~250 команд на весь спектр Telegram (`channel`, `dialogs`, `messages`, `search`, `pipeline`, `photo-loader`, `analytics`, `scheduler`, `account`, …). Каждая команда самодостаточна: открывает Telegram-подключение, делает работу, завершается. Демон не нужен.
- **Скилл для агентов кодирования** — в репо лежит [скилл](skills/tg-agent/SKILL.md), который обучает агента каталогу команд, модели рантайма и правилам безопасности.
- **`tg-agent worker`** — опциональный долгоживущий процесс для сбора по расписанию и очередей отправки.

## Быстрый старт (агенты кодирования)

### Требования

- Python 3.11+
- API-ключи Telegram с [my.telegram.org/apps](https://my.telegram.org/apps)

### 1. Установите CLI

```bash
pip install tg-agent
```

Создайте `.env` в рабочем каталоге (подхватывается каждой командой автоматически):

```
TG_API_ID=ваш_api_id
TG_API_HASH=ваш_api_hash
SESSION_ENCRYPTION_KEY=    # опционально: шифрование session string в БД
```

### 2. Авторизуйте аккаунт (интерактивно, один раз)

```bash
tg-agent account add
```

Код входа придёт в Telegram — вводите его сами. Проверка: `tg-agent account list`.

### 3. Добавьте скилл своему агенту

**Claude Code** — оба пути равнозначны:

```
/plugin marketplace add axisrow/tg_content_factory
/plugin install tg-agent@tg-agent-marketplace
```

или скопируйте каталог скилла:

```bash
cp -r skills/tg-agent ~/.claude/skills/tg-agent
```

**OpenCode / Codex / другие агенты** — отдайте агенту файл [`skills/tg-agent/SKILL.md`](skills/tg-agent/SKILL.md) как инструкцию (include в AGENTS.md, вложение в системный промпт и т.п.).

### 4. Просто попросите агента

- «Покажи вчерашние посты из @durov»
- «Отправь этот черновик в Избранное»
- «Собери новые сообщения из моих каналов и найди упоминания \<ключевое слово\>»

## Что умеет

- **Создан для агентов кодирования** — CLI это контракт: новые возможности сначала появляются как CLI-команды (и тестируются там), остальные поверхности — только если они вообще нужны
- **Все типы чатов** — каналы, супергруппы, гигагруппы, форумы, открытые и закрытые
- **Мультиаккаунт** с автоматической ротацией при flood-wait
- **3 режима поиска** — локальная БД (FTS5), напрямую через Telegram API, AI/LLM
- **Сбор по расписанию** — инкрементальный; живёт в фоновом `tg-agent worker`
- **Мониторинг по ключевым словам** — текст и regex, уведомления через Telegram-бота
- **Контент-фабрика** — LLM-пайплайны: генерация → модерация → публикация, с генерацией картинок
- **Встроенный антиспам** — дедупликация, детекция низкоуникального контента, кросс-канальный спам, фильтры по подписчикам и языку
- **Аналитика** — топ-посты, тренды, тепловые карты активности, трендовые темы и эмодзи
- **Безопасность** — шифрование сессий (Fernet + PBKDF2), HMAC-signed cookies веб-панели
- **Docker-ready**

## Легаси-поверхности

Они продолжают работать, но **заморожены**: разработка приостановлена на неопределённый срок, новые возможности на них не строятся. Активно развивается только CLI + скилл.

- **Веб-панель** (FastAPI + Bootstrap 5) — `python -m src.main serve`, затем http://localhost:8080 (пароль из `WEB_PASS`)
- **TUI** и **встроенный агент-чат** (`tg-agent agent chat`; бэкенды `claude-agent-sdk` / `deepagents`)
- **MCP-сервер** (`python -m src.main mcp-server`)

### Легаси: split-деплой (Docker / k8s)

По умолчанию `serve` поднимает встроенный Telegram-воркер в том же процессе.
Для split-деплоя передайте `--no-worker` и запустите отдельный воркер-сервис:

```bash
# контейнер 1 — только веб-панель и API
python -m src.main serve --no-worker

# контейнер 2 — Telegram-воркер (общий SQLite-том)
python -m src.main worker
```

## Docker

```bash
cp .env.example .env
# заполните своими данными
docker-compose up -d
```

## Важное замечание по roadmap semantic search

Текущая реализация семантического и гибридного поиска исторически строилась
вокруг runtime-загрузки `sqlite-vec`. Как обязательный foundation этот подход
оказался слишком хрупким: одного установленного пакета `sqlite-vec` недостаточно,
потому что активная сборка Python/SQLite должна еще поддерживать
`sqlite3.enable_load_extension(...)`. На практике это означает, что один и тот же
`pip install` может давать разный результат на разных машинах, вплоть до
сценария "пакет установлен, но semantic search недоступен".

Поэтому roadmap исправляется в сторону portable SQLite-first backend, который
должен работать на обычной установке Python без `enable_load_extension`. Пока
эта реализация не доведена до кода, `sqlite-vec` следует считать переходной
зависимостью, а не гарантированным переключателем фичи. Публичный интерфейс
поиска при этом не меняется: индексация embeddings, semantic search и hybrid
search остаются целевым контрактом.

Подробности, мотивация и целевая архитектура описаны в
[docs/semantic-search.md](docs/semantic-search.md).

## Конфигурация

### Переменные окружения (.env)

| Переменная | Обязательна | Описание |
|---|---|---|
| `TG_API_ID` | Да | Telegram API ID |
| `TG_API_HASH` | Да | Telegram API Hash |
| `SESSION_ENCRYPTION_KEY` | Нет* | Ключ шифрования Telegram session string в БД |
| `WEB_PASS` | —† | Пароль веб-панели (только легаси-дашборд) |
| `LLM_API_KEY` | Нет | API-ключ для AI-поиска |
| `ANTHROPIC_API_KEY` | —† | Только `claude-agent-sdk` (легаси встроенный агент-чат) |
| `CLAUDE_CODE_OAUTH_TOKEN` | —† | OAuth токен для `claude-agent-sdk` (легаси) |
| `AGENT_MODEL` | —† | Override модели Claude SDK (легаси агент-чат) |
| `AGENT_FALLBACK_MODEL` | —† | `provider:model` для `deepagents` fallback (легаси) |
| `AGENT_FALLBACK_API_KEY` | —† | Явный API key для легаси fallback-провайдера |

\* Если не задан, сессии хранятся в plaintext. Если в БД уже есть зашифрованные сессии (`enc:v*`), приложение не запустится пока ключ не будет указан.

\† Только для легаси-поверхностей (веб-панель, встроенный агент-чат) — см. «Легаси-поверхности».

### config.yaml

Поддерживает подстановку `${ENV_VAR}`. Пустые переменные окружения игнорируются (применяются значения по умолчанию).

| Секция | Описание |
|---|---|
| `telegram` | API-ключи (`api_id`, `api_hash`) |
| `web` | Хост, порт, пароль (по умолчанию: `0.0.0.0:8080`) — легаси веб-панель |
| `scheduler` | Интервал сбора, задержки, лимиты, макс. flood wait |
| `notifications` | `admin_chat_id` для уведомлений о совпадениях |
| `database` | Путь к SQLite (по умолчанию: `data/tg_search.db`) |
| `llm` | Провайдер LLM, модель, API-ключ — AI-поиск и контент-фабрика |
| `security` | Настройки шифрования сессий |

## Справочник CLI (выборочно)

```bash
tg-agent worker                                  # фоновый воркер: сбор по расписанию, очереди
tg-agent channel collect --channel-id ID         # разовый инкрементальный сбор (без демона)
tg-agent search "запрос" --limit 20              # поиск по собранной истории
tg-agent messages read @channel --format json    # чтение истории сообщений
tg-agent dialogs send                            # реальные действия в реальных чатах
tg-agent pipeline generate                       # контент-фабрика на LLM
tg-agent serve                                   # легаси веб-панель
```

Полный каталог для агентов — [`skills/tg-agent/reference.md`](skills/tg-agent/reference.md); у каждой
группы есть `--help`.

### `telethon-cli`

`telethon-cli` устанавливается вместе с проектом и использует те же
`TG_API_ID` и `TG_API_HASH` из `.env`.

Опциональные переменные только для CLI:

- `TG_SESSION` задаёт кастомный путь или имя Telethon-сессии.
- `TG_PASSWORD` передаёт пароль 2FA для неинтерактивных запусков.

Legacy-переменные `TELETHON_*` `telethon-cli` по-прежнему понимает для
совместимости, но в этом проекте стандартом считаются `TG_*`.

```bash
telethon-cli login
telethon-cli users get-me --output json
```

## Веб-интерфейс (легаси)

| Страница | Путь | Описание |
|---|---|---|
| Вход в панель | `/login` | Вход в веб-панель по паролю `WEB_PASS` |
| Дашборд | `/` | Статистика, статус планировщика, подключённые аккаунты |
| Авторизация Telegram | `/auth/login` | Добавление Telegram-аккаунтов (телефон + код + 2FA) |
| Аккаунты | `/accounts` | Управление подключёнными аккаунтами |
| Каналы | `/channels` | Добавление/удаление каналов, ключевые слова, импорт |
| Поиск | `/search` | Поиск сообщений (локальный / Telegram / AI) |
| Фильтры | `/filter` | Отчёт антиспам-фильтров и управление |
| Планировщик | `/scheduler` | Запуск/остановка сбора и поиска по ключевым словам |

## Roadmap

- Portable semantic search на обычной установке Python без обязательной runtime-загрузки SQLite extension
- Рост возможностей для агентов: каждая новая фича появляется в CLI (и скилле) первой
- LLM для фабрики контента
- LLM для интеллектуального поиска
- LLM для борьбы со спамом в чатах
- Работа с личными сообщениями
- Автоматизация действий в Telegram (рассылка и пр.)

## Разработка

 ```bash
 # Установка dev-зависимостей
 pip install -e ".[dev]"

 # Параллельно запускаем только safe-подмножество
 pytest tests/ -v -m "not aiosqlite_serial" -n auto

 # Тесты с aiosqlite выполняем последовательно
 pytest tests/ -v -m aiosqlite_serial

 # Один тест
 pytest tests/test_web.py::test_health_endpoint -v

 # Сравнить serial и safe mixed-mode прогон всего suite
 python -m src.main test benchmark

 # Линтер
 ruff check src/ tests/ conftest.py
 ```

### Важное замечание по CI

- `push` workflow проверяет только head текущей ветки.
- `pull_request` workflow проверяет результат merge с `main`.
- Поэтому ветка может быть зелёной на `push` и красной на `pull_request`, если
  в `main` уже есть lint/test-проблема, которая попадает в merge ref PR.
- Перед повторным запуском PR-checks подтягивайте и синхронизируйте
  `origin/main`, чтобы локальная проверка совпадала с CI.

### Политика real Telegram testing

Правила для безопасных automated/live/manual прогонов против настоящего Telegram API описаны в [docs/testing/real-telegram.md](docs/testing/real-telegram.md).

Коротко:

- обычный `pytest` остаётся fake/harness-first;
- real Telegram допускается только через opt-in policy markers и sandbox-аккаунт;
- mutating сценарии вроде BotFather, photo send и `leave_channels` не переводятся на generic live pytest.
