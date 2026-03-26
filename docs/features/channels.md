# Управление каналами

Мониторинг и сбор сообщений из Telegram-каналов, групп и чатов.

## Добавление каналов

=== "CLI"
    ```bash
    python -m src.main channel add @channel_username
    python -m src.main channel add -1001234567890
    python -m src.main channel import channels.txt
    ```

=== "Web"
    `GET /channels/` → Add Channel / Import

## Сбор сообщений

Инкрементальный сбор: только новые сообщения с момента последнего сбора.

=== "CLI"
    ```bash
    python -m src.main collect                          # все каналы
    python -m src.main channel collect --channel-id ID  # один канал
    python -m src.main collect sample --limit 10        # превью без сохранения
    ```

=== "Web"
    `POST /channels/collect-all` · `POST /channels/{pk}/collect`

## Управление

=== "CLI"
    ```bash
    python -m src.main channel list
    python -m src.main channel toggle --channel-id ID
    python -m src.main channel delete --channel-id ID
    python -m src.main channel stats --channel-id ID
    python -m src.main channel refresh-types
    python -m src.main channel refresh-meta         # about, linked_chat_id
    ```

=== "Web"
    `POST /channels/{pk}/toggle` · `POST /channels/{pk}/delete`

## Метаданные канала

Для каждого канала хранятся:
- `about` — описание канала
- `linked_chat_id` — привязанный чат для комментариев
- `has_comments` — наличие комментариев
- `channel_type` — broadcast / megagroup / gigagroup / dm
- `subscriber_count`, `message_count`
