# Настройка

## config.yaml

Создайте файл `config.yaml` в корне проекта:

```yaml
telegram:
  api_id: ${TELEGRAM_API_ID}
  api_hash: ${TELEGRAM_API_HASH}

database:
  path: data/db.sqlite

web:
  host: 0.0.0.0
  port: 8000
```

## Переменные окружения

Значения `${VAR}` автоматически подставляются из окружения. Если переменная отсутствует — ключ удаляется из конфига (не пустая строка).

### Обязательные

| Переменная | Описание |
|-----------|----------|
| `TELEGRAM_API_ID` | API ID из my.telegram.org |
| `TELEGRAM_API_HASH` | API Hash из my.telegram.org |

### Аутентификация Web UI

| Переменная | Описание |
|-----------|----------|
| `WEB_PASS` | Пароль для входа в Web UI (username: `admin`) |

### AI-агент

| Переменная | Описание |
|-----------|----------|
| `ANTHROPIC_API_KEY` | Ключ Anthropic API (для Claude-backend агента) |
| `CLAUDE_CODE_OAUTH_TOKEN` | OAuth-токен Claude Code (альтернатива API-ключу) |
| `OPENAI_API_KEY` | OpenAI (LLM провайдер для пайплайнов) |
| `COHERE_API_KEY` | Cohere (LLM провайдер) |
| `OLLAMA_BASE` | URL Ollama (например `http://localhost:11434`) |

### Генерация изображений

| Переменная | Описание |
|-----------|----------|
| `TOGETHER_API_KEY` | Together AI (FLUX и другие модели) |
| `HF_API_TOKEN` | HuggingFace Inference API |
| `REPLICATE_API_TOKEN` | Replicate |

### Безопасность

| Переменная | Описание |
|-----------|----------|
| `SESSION_ENCRYPTION_KEY` | Ключ шифрования Telegram session strings (32 байта hex) |

## Первый запуск

```bash
# Запустить Web UI
python -m src.main serve

# Или указать пароль напрямую
python -m src.main serve --web-pass mysecret
```

Откройте `http://localhost:8000` и войдите через Settings → Auth для добавления Telegram аккаунта.
