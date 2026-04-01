# Dialogs

Управление Telegram-диалогами, сообщениями и участниками прямо из интерфейса.

## Диалоги

=== "CLI"
    ```bash
    python -m src.main dialogs list --phone +79001234567
    python -m src.main dialogs refresh --phone +79001234567
    python -m src.main dialogs leave 123456 789012 --phone +79001234567 --yes
    ```

=== "Web"
    `GET /dialogs/` · `POST /dialogs/refresh` · `POST /dialogs/leave`

## Сообщения

=== "CLI"
    ```bash
    python -m src.main dialogs send @username "текст" --yes
    python -m src.main dialogs edit-message @chat 42 "новый текст" --yes
    python -m src.main dialogs delete-message @chat 42 43 44 --yes
    python -m src.main dialogs pin-message @chat 42 --notify
    python -m src.main dialogs unpin-message @chat --message-id 42
    python -m src.main dialogs mark-read @chat --max-id 100
    ```

=== "Web"
    `POST /dialogs/send` · `POST /dialogs/edit-message` · `POST /dialogs/delete-message`
    `POST /dialogs/pin-message` · `POST /dialogs/unpin-message` · `POST /dialogs/mark-read`

## Участники

=== "CLI"
    ```bash
    python -m src.main dialogs participants @chat --limit 100 --search "Иван"
    python -m src.main dialogs edit-admin @chat @user --title "Модератор" --yes
    python -m src.main dialogs edit-permissions @chat @user --until-date 2025-12-31 --yes
    python -m src.main dialogs kick @chat @user --yes
    ```

=== "Web"
    `GET /dialogs/participants` · `POST /dialogs/edit-admin`
    `POST /dialogs/edit-permissions` · `POST /dialogs/kick`

## Медиа и статистика

=== "CLI"
    ```bash
    python -m src.main dialogs download-media @chat 42 --output-dir ./downloads
    python -m src.main dialogs broadcast-stats @channel
    ```

=== "Web"
    `POST /dialogs/download-media` · `GET /dialogs/broadcast-stats`

## Архив

=== "CLI"
    ```bash
    python -m src.main dialogs archive @chat
    python -m src.main dialogs unarchive @chat
    ```

=== "Web"
    `POST /dialogs/archive` · `POST /dialogs/unarchive`

## Создание каналов

=== "CLI"
    ```bash
    python -m src.main dialogs create-channel --title "Мой канал" --username mychannel
    ```

=== "Web"
    `GET /dialogs/create-channel` · `POST /dialogs/create-channel`

## Кеш диалогов

=== "CLI"
    ```bash
    python -m src.main dialogs cache-status
    python -m src.main dialogs cache-clear --phone +79001234567
    ```

=== "Web"
    `GET /dialogs/cache-status` · `POST /dialogs/cache-clear`
