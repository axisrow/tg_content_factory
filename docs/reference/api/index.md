# API Reference

Автоматически сгенерированная документация Python-API из docstring через
[mkdocstrings](https://mkdocstrings.github.io/). Эти страницы покрывают
внутренние Python-поверхности ядра (см. [CLI / Agent Tool Parity](../parity.md)):

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

!!! tip "Где смотреть REST API"
    HTTP-эндпоинты FastAPI документируются автоматически в Swagger UI по адресу
    `/docs` запущенного веб-сервера; ручной обзор — в
    [Web API Reference](../web-api.md).
