# TG Agent

[![Release](https://img.shields.io/github/v/release/axisrow/tg_content_factory)](https://github.com/axisrow/tg_content_factory/releases)

Персональный тулкит для мониторинга Telegram — сбор сообщений, поиск по каналам, уведомления по ключевым словам. Pet-проект для собственных нужд.

[English version](README.md)

## Что умеет

- **Все типы чатов** — каналы, супергруппы, гигагруппы, форумы, открытые и закрытые
- **Мультиаккаунт** с автоматической ротацией при flood-wait
- **3 режима поиска** — локальная БД (FTS5), напрямую через Telegram API, AI/LLM
- Все результаты поиска кешируются в локальную SQLite базу
- **Сбор по расписанию** — инкрементальный сбор сообщений по таймеру
- **Мониторинг по ключевым словам** — текст и regex, уведомления через Telegram-бота
- **Встроенный антиспам** — дедупликация, детекция низкоуникального контента, кросс-канальный спам, фильтры по подписчикам, фильтр нелатинского контента
- **Очередь задач** — фоновая обработка с отслеживанием статуса
- **Веб-панель** — FastAPI + Pico CSS, управление всем из браузера
- **Безопасность** — шифрование сессий (Fernet + PBKDF2), пароль веб-панели, HTTP Basic fallback, HMAC-signed cookies
- **Docker-ready**

## Быстрый старт

### Требования

- Python 3.11+
- API-ключи Telegram с [my.telegram.org/apps](https://my.telegram.org/apps)

### Установка

```bash
pip install .
cp .env.example .env
```

Заполните `.env`:

```
TG_API_ID=ваш_api_id
TG_API_HASH=ваш_api_hash
WEB_PASS=ваш_пароль
SESSION_ENCRYPTION_KEY=    # шифрование session string аккаунтов в БД
LLM_API_KEY=               # опционально, для AI-поиска
AGENT_MODEL=               # опционально, override модели Claude SDK
AGENT_FALLBACK_MODEL=      # опционально, provider:model для deepagents fallback
AGENT_FALLBACK_API_KEY=    # опционально, явный API key для fallback-провайдера
```

Запустите сервер:

```bash
python -m src.main serve
```

Откройте http://localhost:8080 в браузере и введите пароль из `WEB_PASS`.

## Docker

```bash
cp .env.example .env
# заполните своими данными
docker-compose up -d
```

## Конфигурация

### Переменные окружения (.env)

| Переменная | Обязательна | Описание |
|---|---|---|
| `TG_API_ID` | Да | Telegram API ID |
| `TG_API_HASH` | Да | Telegram API Hash |
| `WEB_PASS` | Да | Пароль веб-панели |
| `SESSION_ENCRYPTION_KEY` | Нет* | Ключ шифрования Telegram session string в БД |
| `LLM_API_KEY` | Нет | API-ключ для AI-поиска |
| `ANTHROPIC_API_KEY` | Нет | API-ключ только для `claude-agent-sdk` |
| `CLAUDE_CODE_OAUTH_TOKEN` | Нет | OAuth токен только для `claude-agent-sdk` |
| `AGENT_MODEL` | Нет | Override модели Claude SDK для `/agent` |
| `AGENT_FALLBACK_MODEL` | Нет | `provider:model` для `deepagents` fallback в `/agent` |
| `AGENT_FALLBACK_API_KEY` | Нет | Явный API key для LangChain fallback |

\* Если не задан, сессии хранятся в plaintext. Если в БД уже есть зашифрованные сессии (`enc:v*`), приложение не запустится пока ключ не будет указан.

### config.yaml

Поддерживает подстановку `${ENV_VAR}`. Пустые переменные окружения игнорируются (применяются значения по умолчанию).

| Секция | Описание |
|---|---|
| `telegram` | API-ключи (`api_id`, `api_hash`) |
| `web` | Хост, порт, пароль (по умолчанию: `0.0.0.0:8080`) |
| `scheduler` | Интервал сбора, задержки, лимиты, макс. flood wait |
| `notifications` | `admin_chat_id` для уведомлений о совпадениях |
| `database` | Путь к SQLite (по умолчанию: `data/tg_search.db`) |
| `llm` | Провайдер LLM, модель, API-ключ, флаг включения |
| `security` | Настройки шифрования сессий |

## CLI

```bash
# Веб-сервер
python -m src.main [--config CONFIG] serve [--web-pass PASS]
python -m src.main [--config CONFIG] stop
python -m src.main [--config CONFIG] restart [--web-pass PASS]

# Разовый сбор
python -m src.main [--config CONFIG] collect [--channel-id ID]

# Поиск
python -m src.main [--config CONFIG] search "запрос" [--limit N] [--mode MODE]

# Управление каналами
python -m src.main channel list|add|delete|toggle|collect|stats|refresh-types|import

# Фильтры контента
python -m src.main filter analyze|apply|reset|precheck

# Ключевые слова
python -m src.main keyword list|add|delete|toggle

# Аккаунты
python -m src.main account list|toggle|delete

# Планировщик
python -m src.main scheduler start|trigger|search

# Бот уведомлений
python -m src.main notification setup|status|delete
```

## Веб-интерфейс

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

- LLM для фабрики контента
- LLM для интеллектуального поиска
- LLM для борьбы со спамом в чатах
- Работа с личными сообщениями
- Автоматизация действий в Telegram (рассылка и пр.)

## Разработка

```bash
# Установка dev-зависимостей
pip install -e ".[dev]"

# Тесты
pytest tests/ -v

# Линтер
ruff check src/ tests/
```
