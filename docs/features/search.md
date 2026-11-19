# Поиск

Поиск по собранным сообщениям (FTS5, семантический, гибридный) и живой поиск в Telegram.

## Режимы

### FTS5 (по умолчанию)
Полнотекстовый поиск с поддержкой wildcard. Индекс хранится в SQLite FTS5 таблице.

```bash
python -m tg_agent.main search "ключевое слово"
python -m tg_agent.main search "искусственный интелл*"
```

### Семантический
Векторные эмбеддинги через NumPy KNN (без внешних зависимостей). Находит семантически близкие сообщения.

```bash
python -m tg_agent.main search "запрос" --mode semantic
```

Для активации нужно сначала настроить провайдера эмбеддингов и проиндексировать сообщения:

```bash
python -m tg_agent.main settings semantic --provider PROVIDER --model MODEL --api-key KEY
python -m tg_agent.main search --index-now            # проиндексировать новые сообщения
python -m tg_agent.main search "запрос" --mode hybrid  # FTS5 + семантика
```

### Живой поиск в Telegram
Через Telegram API, без локальной БД. Пока работает воркер-демон, запрос идёт на его подключении через локальный relay.

```bash
python -m tg_agent.main search "запрос" --mode telegram               # глобальный поиск (Premium)
python -m tg_agent.main search "запрос" --mode my_chats               # по своим чатам
python -m tg_agent.main search "запрос" --mode channel --channel-id ID
```

### AI-поиск
Отдельного AI-режима в CLI нет: LLM-поиск был доступен через веб-панель и встроенный агент-чат, удалённые в #1868. Агент кодирования ищет сам, комбинируя `search --mode semantic|hybrid` и `messages read`.

## Поисковые запросы (уведомления)

Сохранённые запросы, которые проверяются планировщиком и отправляют уведомления при новых совпадениях.

```bash
python -m tg_agent.main search-query list
python -m tg_agent.main search-query add "ключевое слово"
python -m tg_agent.main search-query add "ключевое слово" --chats "@chat1, -1001234567890"
python -m tg_agent.main search-query run 1         # разовый запуск
python -m tg_agent.main search-query stats 1       # статистика совпадений
```

Поле `Чаты` у сохранённого запроса необязательно. Пустое значение ищет по всем чатам; непустое ограничивает поиск списком `channel_id`, `@username`, `username` или ссылок `t.me`, разделённых пробелами или запятыми.

Уведомления содержат ссылку на оригинальное сообщение (`t.me/channel/message_id`).
