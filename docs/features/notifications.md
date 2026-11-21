# Уведомления

Личный бот для получения уведомлений о совпадениях поисковых запросов.

## Настройка

```bash
python -m tg_agent.main notification setup      # создать бота через BotFather
python -m tg_agent.main notification status     # статус бота
python -m tg_agent.main notification test       # тестовое сообщение
python -m tg_agent.main notification dry-run    # превью совпадений без отправки
python -m tg_agent.main notification delete     # удалить бота
```

## Как работает

1. Создаётся личный бот через BotFather (используя подключённый Telegram аккаунт). Диалог с BotFather (`notification setup` / `delete`) идёт на владельце сессии — воркер-демоне или общем gateway, если он работает, — потому что только он получает ответы BotFather (#1868)
2. Планировщик проверяет активные поисковые запросы при каждом запуске
3. При новых совпадениях — отправляется уведомление с текстом и ссылкой `t.me/channel/msg_id`
4. `notification delete` удаляет бота через BotFather. Если бота в Telegram уже нет (удалён вручную или у аккаунта вообще нет ботов), команда просто удаляет устаревшую запись и завершается успешно. Ошибки команд `setup` / `status` / `delete` / `test` — одной строкой в stderr, код выхода 1

## Dry-run

Проверить текущие совпадения без отправки уведомлений:

```bash
python -m tg_agent.main notification dry-run
```
