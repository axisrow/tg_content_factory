# My Telegram

Управление Telegram-диалогами, сообщениями и участниками прямо из интерфейса.

## Диалоги

=== "CLI"
    ```bash
    python -m src.main my-telegram list --phone +79001234567
    python -m src.main my-telegram refresh --phone +79001234567
    python -m src.main my-telegram leave 123456 789012 --phone +79001234567 --yes
    ```

=== "Web"
    `GET /my-telegram/` · `POST /my-telegram/refresh` · `POST /my-telegram/leave`

## Сообщения

=== "CLI"
    ```bash
    python -m src.main my-telegram send @username "текст" --yes
    python -m src.main my-telegram edit-message @chat 42 "новый текст" --yes
    python -m src.main my-telegram delete-message @chat 42 43 44 --yes
    python -m src.main my-telegram pin-message @chat 42 --notify
    python -m src.main my-telegram unpin-message @chat --message-id 42
    python -m src.main my-telegram mark-read @chat --max-id 100
    ```

=== "Web"
    `POST /my-telegram/send` · `POST /my-telegram/edit-message` · `POST /my-telegram/delete-message`
    `POST /my-telegram/pin-message` · `POST /my-telegram/unpin-message` · `POST /my-telegram/mark-read`

## Участники

=== "CLI"
    ```bash
    python -m src.main my-telegram participants @chat --limit 100 --search "Иван"
    python -m src.main my-telegram edit-admin @chat @user --title "Модератор" --yes
    python -m src.main my-telegram edit-permissions @chat @user --until-date 2025-12-31 --yes
    python -m src.main my-telegram kick @chat @user --yes
    ```

=== "Web"
    `GET /my-telegram/participants` · `POST /my-telegram/edit-admin`
    `POST /my-telegram/edit-permissions` · `POST /my-telegram/kick`

## Медиа и статистика

=== "CLI"
    ```bash
    python -m src.main my-telegram download-media @chat 42 --output-dir ./downloads
    python -m src.main my-telegram broadcast-stats @channel
    ```

=== "Web"
    `POST /my-telegram/download-media` · `GET /my-telegram/broadcast-stats`

## Архив

=== "CLI"
    ```bash
    python -m src.main my-telegram archive @chat
    python -m src.main my-telegram unarchive @chat
    ```

=== "Web"
    `POST /my-telegram/archive` · `POST /my-telegram/unarchive`

## Создание каналов

=== "CLI"
    ```bash
    python -m src.main my-telegram create-channel --title "Мой канал" --username mychannel
    ```

=== "Web"
    `GET /my-telegram/create-channel` · `POST /my-telegram/create-channel`

## Кеш диалогов

=== "CLI"
    ```bash
    python -m src.main my-telegram cache-status
    python -m src.main my-telegram cache-clear --phone +79001234567
    ```

=== "Web"
    `GET /my-telegram/cache-status` · `POST /my-telegram/cache-clear`
