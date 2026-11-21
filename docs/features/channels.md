# Управление каналами

Мониторинг и сбор сообщений из Telegram-каналов, групп и чатов.

## Добавление каналов

```bash
python -m tg_agent.main channel add @channel_username
python -m tg_agent.main channel add -1001234567890
python -m tg_agent.main channel import channels.txt
```

## Сбор сообщений

Инкрементальный сбор: только новые сообщения с момента последнего сбора.

```bash
python -m tg_agent.main collect                          # все каналы
python -m tg_agent.main channel collect ID               # один канал
python -m tg_agent.main collect sample CHANNEL_ID --limit 10  # превью без сохранения
```

Пока работает воркер-демон, сбор уходит в его очередь (и сразу его будит); подробнее — [Сбор сообщений](collection.md).

## Управление

```bash
python -m tg_agent.main channel list
python -m tg_agent.main channel toggle --channel-id ID
python -m tg_agent.main channel delete --channel-id ID
python -m tg_agent.main channel stats --channel-id ID
python -m tg_agent.main channel refresh-types
python -m tg_agent.main channel refresh-meta         # about, linked_chat_id
python -m tg_agent.main channel refresh-card         # все открытые метаданные: title, username, type, about, linked_chat_id, has_comments
```

## Метаданные канала

Для каждого канала хранятся:
- `about` — описание канала
- `linked_chat_id` — привязанный чат для комментариев
- `has_comments` — наличие комментариев
- `channel_type` — broadcast / megagroup / gigagroup / dm
- `subscriber_count`, `message_count`
