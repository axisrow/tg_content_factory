# Планировщик

Автоматический периодический сбор сообщений и проверка поисковых запросов. Планировщик работает внутри воркер-демона (`tg-agent restart`); CLI-команды ниже меняют настройки и будят живой воркер.

## Управление

```bash
python -m tg_agent.main scheduler start          # включить автостарт (+ reconcile живого воркера)
python -m tg_agent.main scheduler stop           # отключить автостарт
python -m tg_agent.main scheduler trigger        # разовый запуск коллекции
python -m tg_agent.main scheduler status         # статус и конфигурация
python -m tg_agent.main scheduler job-toggle collect_all
python -m tg_agent.main scheduler set-interval collect_all 60
python -m tg_agent.main scheduler task-cancel 42
python -m tg_agent.main scheduler clear-pending
```

## Jobs

| Job | Описание |
|-----|----------|
| `collect_all` | Инкрементальный сбор всех каналов |
| `sq_<id>` | Периодический прогон сохранённого поискового запроса |
| `content_generate_<id>` | Генерация контента пайплайна (задача `CONTENT_GENERATE`) |
| `photo_due` | Отправка запланированных фото |
| `warm_all_dialogs` | Прогрев кеша диалогов |

## Уведомления

Тестирование:

```bash
python -m tg_agent.main notification dry-run    # превью совпадений без отправки
python -m tg_agent.main notification test       # тестовое уведомление
```

## Задачи коллекции

Статус задач (`pending/running/completed/failed/cancelled`) отслеживается в БД через `CollectionQueue`.
