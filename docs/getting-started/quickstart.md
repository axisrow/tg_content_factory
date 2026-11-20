# Быстрый старт

Основной сценарий: агент кодирования (Claude Code, OpenCode, Codex) действует в Telegram через `tg-agent` CLI.

## 1. Установить CLI и авторизовать аккаунт

```bash
pip install tg-agent
tg-agent account add      # интерактивно: телефон + код входа — вводит человек
tg-agent account list     # проверка
```

`.env` с `TG_API_ID` / `TG_API_HASH` подхватывается автоматически. Подробности — [Установка](installation.md).

## 2. Добавить скилл агенту

Claude Code: `/plugin marketplace add https://github.com/axisrow/tg_content_factory` → `/plugin install tg-agent@tg-agent-marketplace`, либо `cp -r skills/tg-agent ~/.claude/skills/tg-agent`. OpenCode / Codex и другие — отдайте агенту `skills/tg-agent/SKILL.md` как инструкцию. Детали — [Установка](installation.md#для-агентов-кодирования).

## 3. Дать агенту задачу

- «покажи последние сообщения из @durov»
- «собери новые посты из всех каналов и найди упоминания X»
- «отправь этот текст в Избранное»

## Команды напрямую

```bash
# Каналы
tg-agent channel add @durov
tg-agent channel import channels.txt
tg-agent channel collect -1001234567890   # при работающем демоне — в его очередь сбора

# Поиск
tg-agent search "ключевое слово" --limit 20
tg-agent messages read @durov --limit 50 --format json

# Планировщик
tg-agent scheduler start
```

Полный каталог — [`skills/tg-agent/reference.md`](https://github.com/axisrow/tg_content_factory/blob/main/skills/tg-agent/reference.md) или `tg-agent --help`.

## Сбор по расписанию

Разовым действиям демон не нужен. Непрерывный сбор и очереди — фоновый воркер-демон:

```bash
tg-agent restart          # остановить прежний демон и запустить воркер (держите в фоне / tmux / systemd)
tg-agent daemon status    # read-only: поднят ли демон
tg-agent scheduler status # состояние планировщика
```

Пока демон работает, он владеет Telegram-подключениями аккаунтов, а живые команды (`messages read --live`, все `dialogs`, `account info`, `search --mode telegram`, …) выполняются на его подключении через локальный relay — тот же вывод, без второго входа. После обновления tg-agent перезапустите демон один раз (`tg-agent restart`).

## Удалённые поверхности

Веб-панель (`serve`, пакет `tg-webui`) и встроенный агент-чат с TUI (`agent …`, TUI — пакет `tg-tui`) — устаревшие опциональные поверхности; основной интерфейс — CLI + скилл. MCP-сервер (`mcp-server`) удалён в #1868. См. [Агенты и Telegram](../features/agent.md).
