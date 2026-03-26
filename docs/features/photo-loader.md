# Фото-загрузчик

Батчевая отправка фото в Telegram-каналы с поддержкой расписания и авто-очереди.

## Отправка фото

=== "CLI"
    ```bash
    python -m src.main photo-loader send --phone +79001234567
    python -m src.main photo-loader schedule-send --phone +79001234567
    python -m src.main photo-loader batch-create manifest.json
    python -m src.main photo-loader run-due    # запустить задачи по расписанию
    ```

=== "Web"
    `POST /my-telegram/photos/send` · `POST /my-telegram/photos/schedule`
    `POST /my-telegram/photos/batch` · `POST /my-telegram/photos/run-due`

## Управление батчами

=== "CLI"
    ```bash
    python -m src.main photo-loader batch-list
    python -m src.main photo-loader batch-cancel ITEM_ID
    ```

=== "Web"
    `POST /my-telegram/photos/items/{item_id}/cancel`

## Авто-загрузка

=== "CLI"
    ```bash
    python -m src.main photo-loader auto-create
    python -m src.main photo-loader auto-list
    python -m src.main photo-loader auto-update JOB_ID
    python -m src.main photo-loader auto-toggle JOB_ID
    python -m src.main photo-loader auto-delete JOB_ID
    ```

=== "Web"
    `POST /my-telegram/photos/auto` · `POST /my-telegram/photos/auto/{job_id}/toggle`
    `POST /my-telegram/photos/auto/{job_id}/delete`

## Просмотр диалогов

=== "CLI"
    ```bash
    python -m src.main photo-loader dialogs --phone +79001234567
    python -m src.main photo-loader refresh --phone +79001234567
    ```

=== "Web"
    `GET /my-telegram/photos/` · `POST /my-telegram/photos/refresh`
