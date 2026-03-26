# Поиск

Три режима поиска по собранным сообщениям.

## Режимы

### FTS5 (по умолчанию)
Полнотекстовый поиск с поддержкой wildcard. Индекс хранится в SQLite FTS5 таблице.

```bash
python -m src.main search "ключевое слово"
python -m src.main search "искусственный интелл*"
```

### Семантический
Векторные эмбеддинги через NumPy KNN (без внешних зависимостей). Находит семантически близкие сообщения.

```bash
python -m src.main search "запрос" --mode semantic
```

Для активации нужно сначала проиндексировать:

=== "CLI"
    Настройте через `settings set` или Web UI → Settings → Semantic Search → Index

=== "Web"
    `POST /settings/semantic-index`

### AI-поиск
LLM-powered поиск через AI-агента:

```bash
python -m src.main agent chat "найди сообщения про блокчейн за последний месяц"
```

## Поисковые запросы (уведомления)

Сохранённые запросы, которые проверяются планировщиком и отправляют уведомления при новых совпадениях.

=== "CLI"
    ```bash
    python -m src.main search-query list
    python -m src.main search-query add "ключевое слово"
    python -m src.main search-query run 1         # разовый запуск
    python -m src.main search-query stats 1       # статистика совпадений
    ```

=== "Web"
    `GET /search-queries/` · `POST /search-queries/add`

Уведомления содержат ссылку на оригинальное сообщение (`t.me/channel/message_id`).
