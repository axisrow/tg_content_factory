# TG Agent

[![Release](https://img.shields.io/github/v/release/axisrow/tg_content_factory)](https://github.com/axisrow/tg_content_factory/releases)

Автоматизация Telegram для **агентов кодирования** — Claude Code, OpenCode, Codex. Распространяется как **плагин Claude Code** (скилл + CLI): установите плагин — и ваш агент сможет действовать в Telegram за вас: читать и отправлять сообщения, управлять каналами и чатами, скачивать медиа, гонять контент-фабрику на LLM. Контент-фабрика (сбор → поиск → генерация → публикация) — один из модулей.

[English version](README.md)

## Как это устроено

- **Скилл + плагин Claude Code** — в репо лежит [скилл](skills/tg-agent/SKILL.md) (ставится из маркетплейса плагинов или копируется вручную), который обучает агента каталогу команд, модели рантайма и правилам безопасности. Скилл управляет CLI — никакой MCP-поверхности, поедающей контекстное окно.
- **`tg-agent` CLI** — поверхность продукта: ~250 команд на весь спектр Telegram (`channel`, `dialogs`, `messages`, `search`, `pipeline`, `photo-loader`, `analytics`, `scheduler`, `account`, …). Каждая команда самодостаточна: делает работу и завершается. Демон не нужен — без него команда сама подключает аккаунт.
- **Воркер-демон** (`tg-agent restart`) — опциональный долгоживущий процесс для сбора по расписанию и очередей отправки. Пока он работает, он владеет Telegram-подключением каждого аккаунта (одно подключение на аккаунт на машине): живые команды (`messages read --live`, все действия `dialogs`, `account info`, `search --mode telegram`, `pipeline run`, …) выполняются на подключении демона через локальный relay (127.0.0.1, токен-файл доступен только владельцу) — тот же вывод, без второго входа, без очереди. Передача сбора идёт в очередь демона и будит его сразу. В режиме gateway (`tg-agent global enable`) владелец подключений — общий gateway.

> **Обновление до 0.2.5:** перезапустите работающий демон один раз (`tg-agent restart`) и общий gateway, если он используется (`tg-agent gateway restart --shared`) — старый демон или gateway не обслуживает relay, и живые команды рядом с ним сразу падают с этой подсказкой. Веб-панель (`serve`, опциональный пакет `tg-webui`) и встроенный агент-чат (`agent …`; интерактивный TUI — опциональный пакет `tg-tui`) остаются устаревшими опциональными поверхностями. Stdio-мост MCP (`mcp-server`) и агентные бэкенды Codex/ADK удалены в #1868.

## Быстрый старт (агенты кодирования)

### Требования

- Python 3.11+
- API-ключи Telegram с [my.telegram.org/apps](https://my.telegram.org/apps)

### 1. Установите плагин (Claude Code) — или только скилл

```
/plugin marketplace add https://github.com/axisrow/tg_content_factory
/plugin install tg-agent@tg-agent-marketplace
```

> Короткая форма (`axisrow/tg_content_factory`) клонирует по SSH — без GitHub SSH-ключей падает; используйте полный HTTPS-URL как выше. Если маркетплейс не виден сразу после `marketplace add`, выполните `/reload-plugins` перед `plugin install`.

**OpenCode / Codex / другие агенты** — без плагина: отдайте агенту файл [`skills/tg-agent/SKILL.md`](skills/tg-agent/SKILL.md) как инструкцию (include в AGENTS.md, вложение в системный промпт и т.п.) или скопируйте каталог скилла:

```bash
cp -r skills/tg-agent ~/.claude/skills/tg-agent
```

### 2. Установите CLI

Скилл управляет CLI `tg-agent`, он должен быть на машине, где работает агент:

```bash
pip install tg-agent
```

Создайте `.env` в рабочем каталоге (подхватывается каждой командой автоматически):

```
TG_API_ID=ваш_api_id
TG_API_HASH=ваш_api_hash
SESSION_ENCRYPTION_KEY=your_random_secret    # обязателен для добавления/импорта аккаунтов
```

Для нескольких проектов на одной машине копию ключа в каждом `.env` можно не
дублировать: сохраните его один раз в ОС-хранилище секретов командой
`tg-agent key set` (macOS Keychain / Windows Credential Manager / secret-tool).
Явно заданный `SESSION_ENCRYPTION_KEY` по-прежнему имеет приоритет.

### 3. Авторизуйте аккаунт (интерактивно, один раз)

```bash
tg-agent account add
```

Код входа придёт в Telegram — вводите его сами. Проверка: `tg-agent account list`.

### 4. Просто попросите агента

- «Покажи вчерашние посты из @durov»
- «Отправь этот черновик в Избранное»
- «Собери новые сообщения из моих каналов и найди упоминания \<ключевое слово\>»

## Что умеет

- **Создан для агентов кодирования** — CLI это контракт: новые возможности появляются как CLI-команды (и тестируются там) и в скилле
- **Все типы чатов** — каналы, супергруппы, гигагруппы, форумы, открытые и закрытые
- **Мультиаккаунт** с автоматической ротацией при flood-wait; одно Telegram-подключение на аккаунт на машине, общее для всех CLI-процессов через relay демона (или gateway)
- **Поиск** — локальная БД (FTS5, семантический, гибридный) и живой поиск в Telegram (глобальный, по своим чатам, в одном канале)
- **Сбор по расписанию** — инкрементальный; живёт в фоновом воркер-демоне
- **Мониторинг по ключевым словам** — текст и regex, уведомления через Telegram-бота
- **Контент-фабрика** — LLM-пайплайны: генерация → модерация → публикация, с генерацией картинок
- **Встроенный антиспам** — дедупликация, детекция низкоуникального контента, кросс-канальный спам, фильтры по подписчикам и языку
- **Аналитика** — топ-посты, тренды, тепловые карты активности, трендовые темы и эмодзи
- **Безопасность** — шифрование сессий (Fernet + PBKDF2); relay демона слушает только 127.0.0.1, за случайным токеном в файле, доступном только владельцу
- **Docker-ready**

## Docker

```bash
cp .env.example .env
# заполните своими данными
docker-compose up -d
```

Образ запускает рантайм Telegram-воркера (automation-first) — сбор,
планировщик, очереди — с PID-файлом. Веб-поверхности в образе нет (опциональная
веб-панель в него не входит), поэтому нет ни опубликованного порта, ни HTTP-healthcheck.

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
| `SESSION_ENCRYPTION_KEY` | Да* | Ключ шифрования Telegram session string в БД; может храниться в ОС-хранилище секретов (`tg-agent key set`) |
| `LLM_API_KEY` | Нет | Запасной API-ключ (секция `llm`) для эмбеддингов семантического поиска, если не задан `settings semantic --api-key` |
| `ANTHROPIC_API_KEY` | Нет | Бэкенд `claude-agent-sdk` агента контент-пайплайнов |
| `CLAUDE_CODE_OAUTH_TOKEN` | Нет | OAuth-токен Claude Code для бэкенда `claude-agent-sdk` (агент контент-пайплайнов) |
| `AGENT_MODEL` | Нет | Override модели бэкенда `claude-agent-sdk` (агент контент-пайплайнов) |
| `AGENT_FALLBACK_MODEL` | Нет | `provider:model` для бэкенда `deepagents` агента контент-пайплайнов |
| `AGENT_FALLBACK_API_KEY` | Нет | Явный API key для fallback-провайдера `deepagents` |

\* Обязателен для добавления/импорта аккаунтов и расшифровки сессий. Новые сессии всегда шифруются (`enc:v2:`); запись plaintext запрещена. Создайте секрет командой `python -c "import secrets; print(secrets.token_urlsafe(32))"` и сохраните его для чтения сессий. `config.yaml` не нужен: ключ читается напрямую из `.env`. Проекту в режиме gateway (`tg-agent global enable`) свой ключ не нужен: сессиями владеет общий gateway.

### config.yaml

Поддерживает подстановку `${ENV_VAR}`. Пустые переменные окружения игнорируются (применяются значения по умолчанию).

| Секция | Описание |
|---|---|
| `telegram` | API-ключи (`api_id`, `api_hash`) |
| `scheduler` | Интервал сбора, задержки, лимиты, макс. flood wait |
| `notifications` | `admin_chat_id` для уведомлений о совпадениях |
| `database` | Путь к SQLite (по умолчанию: `data/tg_search.db`); рядом лежит endpoint-файл relay демона (`<db>.relay.json`, 0600) |
| `llm` | Провайдер LLM, модель, API-ключ — запасной провайдер/ключ для эмбеддингов семантического поиска |
| `agent` | Агент контент-пайплайнов: модель, fallback-модель/ключ `deepagents`, таймауты стрима |
| `security` | Настройки шифрования сессий |

### Правила выбора бэкенда агента контент-пайплайнов

Пайплайны с `--generation-backend deep_agents` работают на движке агента (`AgentManager`):

- Автовыбор: `deepagents`, если в БД есть рабочие конфиги провайдеров (`tg-agent provider add`); затем `claude-agent-sdk`, если задан `ANTHROPIC_API_KEY` или `CLAUDE_CODE_OAUTH_TOKEN`; затем `deepagents` с `AGENT_FALLBACK_MODEL`.
- `ANTHROPIC_API_KEY` и `CLAUDE_CODE_OAUTH_TOKEN` никогда не переиспользуются `deepagents`.
- `tg-agent settings agent --backend auto|claude|deepagents` принудительно выбирает бэкенд; override действует только в режиме разработчика (`tg-agent settings set agent_dev_mode_enabled 1`).

## Справочник CLI (выборочно)

```bash
tg-agent restart                                 # управляемый демон: остановить прежний, запустить воркер
tg-agent worker                                  # тот же рантайм без шага остановки
tg-agent daemon status                           # read-only: поднят ли демон, PID, хранилище
tg-agent channel collect ID                      # один канал (при работающем демоне — в его очередь)
tg-agent search "запрос" --limit 20              # поиск по собранной истории
tg-agent messages read @channel --format json    # чтение истории сообщений
tg-agent dialogs send                            # реальные действия в реальных чатах
tg-agent pipeline generate                       # контент-фабрика на LLM
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

 # Тесты с aiosqlite: последовательно внутри файла, независимые файлы — параллельно
 pytest tests/ -q -m aiosqlite_serial -n auto --dist=loadfile

 # Один тест
 pytest tests/test_local_install_consistency.py -v

 # Сравнить serial и safe mixed-mode прогон всего suite
 python -m tg_agent.main test benchmark

 # Линтер
 ruff check tg_agent/ tests/ packages/ conftest.py
 ```

### Важное замечание по CI

- Job `build` в `.github/workflows/ci.yml` собирает Python-пакет на
  `ubuntu-latest`, `windows-latest` и `macos-latest` (Python 3.11).
  На каждой ОС собираются sdist и wheel для `tg-agent` и внутреннего
  `tg-shared`, затем оба wheel устанавливаются с
  зависимостями и проверяется через `pip check`, `tg-agent --version` и
  `tg-agent --help` вне каталога исходников. Пакеты доступны в разделе
  **Actions → CI → нужный запуск → Artifacts** как `dist-<ОС>`.
  Это Python-дистрибутивы для установки через `pip`, не standalone `.exe`/`.app`.
  Полный набор тестов и линтеры выполняются на Ubuntu.
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

