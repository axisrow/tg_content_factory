# Agent Tools API

Инструменты AI-агента (`src/agent/tools/`) — модульные файлы, каждый из которых
регистрирует группу `@tool()`-функций через `register()`. Декларации категорий
(`TOOL_GROUPS`) — единый источник истины для прав доступа (READ/WRITE/DELETE),
из которого `permissions.py` выводит ACL.

!!! note
    Описания самих инструментов задаются строковыми аргументами декоратора
    `@tool("name", "description", ...)`, а не Python docstring, поэтому autodoc
    показывает структуру модуля (`register`, `TOOL_GROUPS`), а полный
    человекочитаемый каталог инструментов — в
    [Agent Tools Reference](../agent-tools.md).

## Каналы

::: src.agent.tools.channels

## Поиск

::: src.agent.tools.search

## Контент-пайплайны

::: src.agent.tools.pipelines

## Изображения

::: src.agent.tools.images

## Сообщения

::: src.agent.tools.messaging

## Коллекция

::: src.agent.tools.collection

## Аналитика

::: src.agent.tools.analytics

## Модерация

::: src.agent.tools.moderation

## Фильтры

::: src.agent.tools.filters

## Права доступа

::: src.agent.tools.permissions
