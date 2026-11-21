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
python -m tg_agent.main collect                              # все каналы, инкрементально
python -m tg_agent.main channel collect ID [--wait]          # один канал
python -m tg_agent.main collect sample CHANNEL_ID --limit 10 # превью
```

Сбор — работа воркер-демона. Пока он запущен, `collect` и `channel collect` ставят задачи в его очередь сбора, и постановка будит воркер сразу, без ожидания поллинга (#1868); `channel collect --wait` дожидается завершения задачи. Без демона `channel collect` (и `collect --channel-id`) собирает канал сам, в своём процессе, а `collect` по всем каналам только ставит задачи — их выполнит воркер при следующем запуске (`tg-agent restart`). `collect sample` ничего не сохраняет и при запущенном демоне читает через его подключение (локальный relay).

## Статистика

```bash
python -m tg_agent.main channel stats --channel-id ID   # статистика одного канала
python -m tg_agent.main analytics velocity              # сообщений в день
```

## Cancellation

Коллекция может быть отменена через `asyncio.Event` (`Collector._cancel_event`), который проверяется каждые 10 сообщений и на границе каждого канала.

```bash
python -m tg_agent.main scheduler task-cancel 42
```
