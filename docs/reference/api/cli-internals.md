# CLI Internals

Внутренности CLI (`src/cli/`) — обвязка runtime (логирование, PII-редакция),
разбор аргументов и обработчики команд. Человекочитаемый справочник команд —
в [CLI Reference](../cli.md); эта страница документирует Python-API из docstring.

!!! note
    Многие обработчики команд и парсеры пока без docstring — autodoc показывает
    их сигнатуры, описания появятся по мере наполнения (issue #1072).

## Runtime

Логирование и PII-редакция вывода CLI.

::: src.cli.runtime

## Команды (Typer)

Единая точка объявления CLI — приложение Typer (`app`). Каждая команда
объявляет свои флаги/аргументы как параметры Typer и зовёт общее тело
`*_impl` из `src/cli/commands/`. argparse-каркас удалён в #1125.

::: src.cli.typer_app

::: src.cli.typer_commands

## Точка входа

::: src.cli.main

## Обработчики команд

### Каналы

::: src.cli.commands.channel

### Сбор

::: src.cli.commands.collect

### Пайплайны

::: src.cli.commands.pipeline

### Поиск

::: src.cli.commands.search
