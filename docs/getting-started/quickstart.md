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

Claude Code: `/plugin marketplace add axisrow/tg_content_factory` → `/plugin install tg-agent@tg-agent-marketplace`, либо `cp -r skills/tg-agent ~/.claude/skills/tg-agent`. OpenCode / Codex и другие — отдайте агенту `skills/tg-agent/SKILL.md` как инструкцию. Детали — [Установка](installation.md#для-агентов-кодирования).

## 3. Дать агенту задачу

- «покажи последние сообщения из @durov»
- «собери новые посты из всех каналов и найди упоминания X»
- «отправь этот текст в Избранное»

## Команды напрямую

```bash
# Каналы
tg-agent channel add @durov
tg-agent channel import channels.txt
tg-agent channel collect --channel-id -1001234567890

# Поиск
tg-agent search "ключевое слово" --limit 20
tg-agent messages read @durov --limit 50 --format json

# Планировщик
tg-agent scheduler start
```

Полный каталог — [`skills/tg-agent/reference.md`](https://github.com/axisrow/tg_content_factory/blob/main/skills/tg-agent/reference.md) или `tg-agent --help`.

## Сбор по расписанию

Разовые действия агенту-демону не нужны. Непрерывный сбор и очереди — фоновый процесс:

```bash
tg-agent worker           # шедулер, очереди, диспетчеры
tg-agent scheduler status # проверка состояния
```

## Легаси-пути

Веб-панель, TUI, встроенный агент-чат и MCP-сервер — легаси: работают, но заморожены (bug fixes only). См. [Агенты и Telegram](../features/agent.md).
