# Быстрый старт

## 1. Добавить Telegram аккаунт

Откройте Web UI → Settings → Auth и введите номер телефона. После получения кода подтвердите вход.

Или через CLI (интерактивно в браузере):

```
http://localhost:8000/auth/login
```

## 2. Добавить каналы

=== "CLI"
    ```bash
    # Один канал
    python -m src.main channel add @durov

    # Импорт из файла
    python -m src.main channel import channels.txt
    ```

=== "Web"
    Channels → Add Channel / Import

## 3. Собрать сообщения

=== "CLI"
    ```bash
    # Все каналы
    python -m src.main collect

    # Один канал
    python -m src.main channel collect --channel-id -1001234567890
    ```

=== "Web"
    Channels → Collect All

## 4. Поиск

=== "CLI"
    ```bash
    python -m src.main search "ключевое слово" --limit 20
    python -m src.main search "запрос" --mode semantic
    ```

=== "Web"
    Главная страница (`/`) — поле поиска

=== "Agent"
    ```bash
    python -m src.main agent chat "найди сообщения про AI за последнюю неделю"
    ```

## 5. Настроить планировщик

=== "CLI"
    ```bash
    python -m src.main scheduler start
    ```

=== "Web"
    Scheduler → Start

Планировщик будет автоматически собирать новые сообщения и проверять поисковые запросы.
