# Doc-coverage (долг документации)

Отчёт о покрытии кода docstring'ами — какие функции, классы и модули **не
задокументированы**. Это «долг документации»: что стоит описать, чтобы
авто-доки (mkdocstrings, Swagger) были полнее. Часть эпика автодокументации
([#1022](https://github.com/axisrow/tg_content_factory/issues/1022)).

Под капотом — [`interrogate`](https://interrogate.readthedocs.io/), обёрнутый в
`scripts/doc_coverage.py` по образцу `scripts/code_health.py`.

## Что это НЕ

!!! warning "Doc-coverage ≠ мёртвый код"
    «Нет docstring» (этот отчёт, interrogate) и «не вызывается» (мёртвый код,
    vulture в `scripts/code_health.py`) — **независимые сигналы**.

    - Недокументированная, но используемая функция — кандидат на docstring.
    - Документированная, но никем не вызываемая — кандидат на удаление.
    - Пересечение «нет docstring **И** не вызывается» — кандидат на внимание:
      либо задокументировать, либо удалить.

    Не путайте эти две оси. Они закрываются разными PR и разными инструментами.

## Запуск

```bash
# Информационный отчёт по src/ (общий %, худшие файлы, список недокументированного)
python scripts/doc_coverage.py

# Другой путь / больше строк в топах
python scripts/doc_coverage.py --path src/web --top 40

# CI-гейт: ненулевой exit, если покрытие ниже порога
python scripts/doc_coverage.py --fail-under 23

# Отключить гейт явно (только отчёт), даже если в конфиге задан fail_under
python scripts/doc_coverage.py --fail-under 0
```

Без `--fail-under` берётся `fail_under` из `[tool.interrogate]` в
`pyproject.toml` — единый источник истины и для скрипта, и для CLI-гейта
`interrogate -c pyproject.toml`.

## Конфигурация

Политика (исключения, ignore-флаги, baseline-порог) живёт в одном месте —
`[tool.interrogate]` в `pyproject.toml`:

```toml
[tool.interrogate]
fail_under = 23            # мягкий baseline (ratchet floor)
ignore_init_method = true
ignore_magic = true
ignore_private = true
exclude = ["tests", "scripts", "build", "dist", "site"]
```

`fail_under` — **мягкий** baseline. Сегодня покрытие ~24%; порог `23` ловит
регрессии, не заставляя документировать всё разом. Поднимайте его по мере роста
покрытия.

## CI

Шаг `Doc-coverage report (advisory)` в `static-checks` (`.github/workflows/ci.yml`)
прогоняет отчёт. Он **advisory** (`continue-on-error: true`), как `pip-audit` и
`bandit`: показывает регрессии, но сам по себе не «краснит» CI. Когда покрытие
вырастет — можно сделать гейт блокирующим и поднять baseline.
