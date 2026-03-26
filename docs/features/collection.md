# Сбор сообщений

Инкрементальный сбор сообщений из Telegram-каналов с поддержкой multi-account.

## Как работает

1. Для каждого канала берётся `last_collected_id` из БД
2. Telethon итерирует сообщения с `min_id = last_collected_id`, `reverse=True`
3. Сообщения вставляются через `INSERT OR IGNORE` (дубликаты игнорируются)
4. После цикла `last_collected_id` обновляется до `max(seen message_ids)`

## Режимы сбора

| Режим | Описание |
|-------|----------|
| Инкрементальный | Только новые сообщения (по умолчанию) |
| Полный | С нуля, `force=True` |
| Превью | `collect sample` — без сохранения в БД |

## CLI

```bash
python -m src.main collect                              # все каналы, инкрементально
python -m src.main channel collect --channel-id ID     # один канал
python -m src.main collect sample --channel-id ID --limit 10  # превью
```

## Web

`POST /channels/collect-all` · `POST /channels/{pk}/collect`

## Статистика

```bash
python -m src.main channel stats --channel-id ID   # статистика одного канала
python -m src.main analytics velocity              # сообщений в день
```

## Cancellation

Коллекция может быть отменена через `asyncio.Event` (`Collector._cancel_event`), который проверяется каждые 10 сообщений и на границе каждого канала.

```bash
python -m src.main scheduler task-cancel 42
```

Web: `POST /scheduler/tasks/{task_id}/cancel`
