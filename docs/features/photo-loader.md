# Фото-загрузчик

Батчевая отправка фото в Telegram-каналы с поддержкой расписания и авто-очереди.

## Отправка фото

```bash
python -m tg_agent.main photo-loader send --phone +79001234567
python -m tg_agent.main photo-loader schedule-send --phone +79001234567
python -m tg_agent.main photo-loader batch-create manifest.json
python -m tg_agent.main photo-loader run-due    # запустить задачи по расписанию
```

## Управление батчами

```bash
python -m tg_agent.main photo-loader batch-list
python -m tg_agent.main photo-loader batch-cancel ITEM_ID
```

## Авто-загрузка

```bash
python -m tg_agent.main photo-loader auto-create
python -m tg_agent.main photo-loader auto-list
python -m tg_agent.main photo-loader auto-update JOB_ID
python -m tg_agent.main photo-loader auto-toggle JOB_ID
python -m tg_agent.main photo-loader auto-delete JOB_ID
```

## Просмотр диалогов

```bash
python -m tg_agent.main photo-loader dialogs --phone +79001234567
python -m tg_agent.main photo-loader refresh --phone +79001234567
```
