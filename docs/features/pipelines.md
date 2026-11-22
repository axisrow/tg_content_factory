# Контент-пайплайны

Автоматическая генерация и публикация контента в Telegram-каналы.

## Поток данных

```
Pipeline config
    → ContentGenerationService (LLM + RAG)
    → generation_runs record
    → [если AUTO] PublishService → Telegram channel
    → [если MANUAL] Moderation Queue → approve → publish
```

## Создание пайплайна

```bash
python -m tg_agent.main pipeline add
python -m tg_agent.main pipeline list
python -m tg_agent.main pipeline show 1
```

## Генерация контента

```bash
python -m tg_agent.main pipeline generate 1      # генерация
python -m tg_agent.main pipeline run 1           # генерация + публикация
python -m tg_agent.main pipeline runs 1          # история запусков
python -m tg_agent.main pipeline run-show 42     # детали запуска
```

## Модерация

```bash
python -m tg_agent.main pipeline queue          # очередь на модерацию
python -m tg_agent.main pipeline approve 42     # одобрить
python -m tg_agent.main pipeline reject 42      # отклонить
python -m tg_agent.main pipeline publish 42     # опубликовать
python -m tg_agent.main pipeline bulk-approve 1 2 3
```

## Режимы публикации

- **MANUAL** — контент попадает в очередь модерации
- **AUTO** — автоматически публикуется в целевой канал

## Провайдеры LLM

Настраиваются через CLI — `tg-agent provider add NAME --api-key KEY [--base-url URL]` (а также `provider list|delete|probe|refresh|test-all`; секреты шифруются и требуют `SESSION_ENCRYPTION_KEY`) — или переменными окружения (`OPENAI_API_KEY`, `COHERE_API_KEY`, `OLLAMA_BASE`, …). Пайплайн с `--generation-backend deep_agents` генерирует через движок агента (бэкенды `deepagents` / `claude-agent-sdk`, выбор — `tg-agent settings agent --backend auto|claude|deepagents`), см. [Агенты и Telegram](agent.md).

Пока работает воркер-демон, `pipeline run|generate|publish` выполняют Telegram-шаги на его подключении через локальный relay (#1868).
