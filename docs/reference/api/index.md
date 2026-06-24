# API Reference

Автоматически сгенерированная документация Python-API из docstring через
[mkdocstrings](https://mkdocstrings.github.io/). Эти страницы покрывают
**не-FastAPI поверхности**, которые не попадают в FastAPI `/docs`
(см. [parity-матрицу](../parity.md)):

- **[Services](services.md)** — сервисный слой (`src/services/`): оркестрация
  сбора, пайплайнов, генерации контента, изображений, уведомлений, провайдеров.
- **[Agent Tools](agent-tools-api.md)** — инструменты AI-агента
  (`src/agent/tools/`): каналы, поиск, пайплайны, изображения и др.
- **[CLI Internals](cli-internals.md)** — внутренности CLI (`src/cli/`):
  runtime-обвязка и обработчики команд.

!!! note "Покрытие docstring"
    Страницы рендерят и символы без docstring (`show_if_no_docstring`), поэтому
    список API полон, но описания появляются по мере наполнения docstring.
    Поднятие покрытия отслеживается отдельно — interrogate, issue #1072.

!!! tip "Где смотреть REST API"
    HTTP-эндпоинты FastAPI документируются автоматически в Swagger UI по адресу
    `/docs` запущенного веб-сервера; ручной обзор — в
    [Web API Reference](../web-api.md).
