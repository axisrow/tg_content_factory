# Dialogs

Управление Telegram-диалогами, сообщениями и участниками прямо из CLI.

Каждое действие `dialogs` выполняется синхронно и печатает результат сразу. Пока работает воркер-демон, команда идёт на его Telegram-подключении через локальный relay (#1868): тот же вывод, без второго входа и без постановки в очередь. Без демона команда подключает аккаунт сама. Очередь Telegram-команд воркера (`dialogs queue status|cancel|clear-pending`) осталась для работы, которая ставится в неё намеренно (`scheduler trigger`, `dm drafts send` и т. п.).

## Диалоги

```bash
python -m tg_agent.main dialogs list --phone +79001234567
python -m tg_agent.main dialogs refresh --phone +79001234567
python -m tg_agent.main dialogs leave 123456 789012 --phone +79001234567 --yes
```

## Сообщения

```bash
python -m tg_agent.main dialogs send @username "текст" --yes
python -m tg_agent.main dialogs react @chat 42 👍 --phone +79001234567
python -m tg_agent.main dialogs edit-message @chat 42 "новый текст" --yes
python -m tg_agent.main dialogs delete-message @chat 42 43 44 --yes
python -m tg_agent.main dialogs pin-message @chat 42 --notify
python -m tg_agent.main dialogs unpin-message @chat --message-id 42
python -m tg_agent.main dialogs mark-read @chat --max-id 100
```

## Участники

```bash
python -m tg_agent.main dialogs participants @chat --limit 100 --search "Иван"
python -m tg_agent.main dialogs edit-admin @chat @user --title "Модератор" --yes
python -m tg_agent.main dialogs edit-permissions @chat @user --until-date 2025-12-31 --yes
python -m tg_agent.main dialogs kick @chat @user --yes
```

## Медиа и статистика

```bash
python -m tg_agent.main dialogs download-media @chat 42 --output-dir ./downloads
python -m tg_agent.main dialogs broadcast-stats @channel
```

## Архив

```bash
python -m tg_agent.main dialogs archive @chat
python -m tg_agent.main dialogs unarchive @chat
```

## Создание каналов

```bash
python -m tg_agent.main dialogs create-channel --title "Мой канал" --username mychannel
```

## Кеш диалогов

```bash
python -m tg_agent.main dialogs cache-status
python -m tg_agent.main dialogs cache-clear --phone +79001234567
```
