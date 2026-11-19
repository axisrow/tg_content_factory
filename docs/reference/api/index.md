# API Reference

Автоматически сгенерированная документация Python-API из docstring через
[mkdocstrings](https://mkdocstrings.github.io/). Эти страницы покрывают
**не-FastAPI поверхности**, которые не попадают в FastAPI `/docs`
(см. [parity-матрицу](../parity.md)):

- **[Services](services.md)** — сервисный слой (`tg_agent/services/`): оркестрация
  сбора, пайплайнов, генерации контента, изображений, уведомлений, провайдеров.
- **[Agent Tools](agent-tools-api.md)** — инструменты AI-агента
  (`tg_agent/agent/tools/`): каналы, поиск, пайплайны, изображения и др.
- **[CLI Internals](cli-internals.md)** — внутренности CLI (`tg_agent/cli/`):
  runtime-обвязка и обработчики команд.

!!! note "Покрытие docstring"
    Страницы рендерят и символы без docstring (`show_if_no_docstring`), поэтому
    список API полон, но описания появляются по мере наполнения docstring.
    Поднятие покрытия отслеживается отдельно — interrogate, issue #1072.

!!! tip "Контракт для агентов"
    HTTP API нет: веб-панель удалена в #1868. Внешний контракт — CLI
    `tg-agent` ([CLI Reference](../cli.md)) и скилл `skills/tg-agent/`.
