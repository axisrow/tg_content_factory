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

=== "CLI"
    ```bash
    python -m src.main pipeline add
    python -m src.main pipeline list
    python -m src.main pipeline show 1
    ```

=== "Web"
    `GET /pipelines/` → Add Pipeline

## Генерация контента

=== "CLI"
    ```bash
    python -m src.main pipeline generate 1      # генерация
    python -m src.main pipeline run 1           # генерация + публикация
    python -m src.main pipeline runs 1          # история запусков
    python -m src.main pipeline run-show 42     # детали запуска
    ```

=== "Web"
    `GET /pipelines/{id}/generate` · `POST /pipelines/{id}/generate`

## Модерация

=== "CLI"
    ```bash
    python -m src.main pipeline queue          # очередь на модерацию
    python -m src.main pipeline approve 42     # одобрить
    python -m src.main pipeline reject 42      # отклонить
    python -m src.main pipeline publish 42     # опубликовать
    python -m src.main pipeline bulk-approve 1 2 3
    ```

=== "Web"
    `GET /moderation/` · `POST /moderation/{run_id}/approve`

## Режимы публикации

- **MANUAL** — контент попадает в очередь модерации
- **AUTO** — автоматически публикуется в целевой канал

## Провайдеры LLM

Настраиваются в Web UI → Settings → Agent Providers. Поддерживаются: OpenAI, Cohere, Ollama, DeepAgents, Claude.
