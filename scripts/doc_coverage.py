#!/usr/bin/env python3
"""Отчёт о doc-coverage (долге документации) по дереву исходников.

Запуск:
    python scripts/doc_coverage.py [--path src] [--top 20] [--fail-under 23]

Оборачивает `interrogate` (% функций/классов/модулей с docstring) и печатает
воспроизводимую сводку:
  * общий процент покрытия документацией и счётчики covered/missing;
  * худшие по покрытию файлы (топ);
  * плоский список недокументированного (файл:строка — имя) — это и есть
    «долг документации»: где не хватает docstring.

С `--fail-under N` возвращает ненулевой exit code, когда покрытие ниже N —
для опционального (мягкого) CI-гейта. Настройки самого interrogate
(исключения, baseline `fail_under`) живут в `[tool.interrogate]` в
pyproject.toml — единый источник истины.

NB: doc-coverage — это НЕ мёртвый код. «Нет docstring» (interrogate, здесь) и
«не вызывается» (vulture, scripts/code_health.py) — независимые сигналы.
Пересечение «нет docstring И не вызывается» — кандидат на внимание (либо
задокументировать, либо удалить), но сами по себе эти отчёты не путать.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# interrogate ставится из [dev] extra (`pip install -e ".[dev]"`). Импорт —
# опциональный: без инструмента скрипт падает с понятной подсказкой, а не
# трейсбеком (как ensure_tools в scripts/code_health.py).
try:
    from interrogate import config as interrogate_config  # type: ignore[import-untyped]
    from interrogate.coverage import InterrogateCoverage  # type: ignore[import-untyped]
except ImportError:  # pragma: no cover - проверяется через подсказку ниже
    InterrogateCoverage = None  # type: ignore[assignment, misc]
    interrogate_config = None  # type: ignore[assignment]

# Ключи [tool.interrogate], которые НЕ являются полями InterrogateConfig:
# `exclude` уходит отдельным аргументом, `fail_under`/`verbose` к подсчёту
# покрытия отношения не имеют (их трогает только сам отчёт/гейт).
_NON_CONF_KEYS = {"exclude", "fail_under", "verbose", "paths"}

_COLOR = sys.stdout.isatty()


def _c(text: str, code: str) -> str:
    return f"\033[{code}m{text}\033[0m" if _COLOR else text


def bold(text: str) -> str:
    return _c(text, "1")


def dim(text: str) -> str:
    return _c(text, "2")


def red(text: str) -> str:
    return _c(text, "31")


def warn(text: str) -> str:
    return _c(text, "33")


def good(text: str) -> str:
    return _c(text, "32")


def header(title: str) -> None:
    print()
    print(bold(f"=== {title} ==="))


def format_percent(perc: float) -> str:
    """Процент покрытия с одним знаком, покрашенный по уровню (зелёный/жёлтый/красный)."""
    text = f"{perc:.1f}%"
    if perc >= 80.0:
        return good(text)
    if perc >= 50.0:
        return warn(text)
    return red(text)


# --------------------------------------------------------------------------- #
# Чистая логика отчёта (тестируется на фейках формы interrogate)
# --------------------------------------------------------------------------- #


def worst_files(results: object, top: int) -> list:
    """Файлы с неполным покрытием, от худшего к лучшему, не более `top`.

    Полностью задокументированные файлы (100%) опускаются — в отчёте о долге
    им не место.
    """
    incomplete = [f for f in results.file_results if f.perc_covered < 100.0]  # type: ignore[attr-defined]
    incomplete.sort(key=lambda f: (f.perc_covered, -f.missing))
    return incomplete[:top]


def undocumented_nodes(results: object) -> list[tuple[str, int | None, str, str]]:
    """Плоский список недокументированного: (файл, строка, имя, тип-узла).

    Пробегает все узлы всех файлов и оставляет только `covered == False` —
    это конкретные функции/классы/модули без docstring.
    """
    out: list[tuple[str, int | None, str, str]] = []
    for file_result in results.file_results:  # type: ignore[attr-defined]
        for node in file_result.nodes:
            if not node.covered:
                out.append((file_result.filename, node.lineno, node.name, node.node_type))
    return out


def gate_exit_code(perc_covered: float, fail_under: float | None) -> int:
    """Exit code для CI-гейта: 1, если покрытие строго ниже порога, иначе 0.

    `fail_under is None` — информационный прогон без гейта, всегда 0.
    """
    if fail_under is None:
        return 0
    return 1 if perc_covered < fail_under else 0


def split_pyproject_config(raw: dict) -> tuple[dict, list[str], float | None]:
    """Разложить сырой `[tool.interrogate]` на (поля conf, excluded, fail_under).

    interrogate складывает в pyproject и поля `InterrogateConfig`, и служебные
    ключи (`exclude`, `fail_under`, `verbose`). Чтобы наш отчёт считал покрытие
    ровно как CLI-гейт (`interrogate -c pyproject.toml`), мы должны прокинуть в
    `InterrogateCoverage` те же `conf` и `excluded`, а `fail_under` использовать
    как дефолтный порог гейта.
    """
    conf_kwargs = {k: v for k, v in raw.items() if k not in _NON_CONF_KEYS}
    excluded = list(raw.get("exclude") or [])
    fail_under = raw.get("fail_under")
    return conf_kwargs, excluded, (float(fail_under) if fail_under is not None else None)


# --------------------------------------------------------------------------- #
# Секции отчёта
# --------------------------------------------------------------------------- #


def section_summary(results: object) -> None:
    header("Doc-coverage (interrogate)")
    perc = results.perc_covered  # type: ignore[attr-defined]
    print(
        f"Покрытие документацией: {bold(format_percent(perc))}    "
        f"задокументировано {bold(str(results.covered))} из {bold(str(results.total))}    "  # type: ignore[attr-defined]
        f"без docstring: {bold(str(results.missing))}"  # type: ignore[attr-defined]
    )


def section_worst_files(results: object, top: int) -> None:
    header("Худшие по покрытию файлы")
    worst = worst_files(results, top)
    if not worst:
        print(dim("  все файлы задокументированы на 100% — отлично"))
        return
    print(dim(f"Топ-{len(worst)} файлов с наибольшим долгом документации:"))
    for f in worst:
        perc = format_percent(f.perc_covered)
        print(f"  {perc:>16}  {f.missing:>4} без docstring  {f.filename}")


def section_undocumented(results: object, top: int) -> None:
    header("Недокументированные функции/классы/модули")
    items = undocumented_nodes(results)
    print(f"Всего без docstring: {bold(str(len(items)))}")
    if not items:
        return
    print(dim(f"\nПервые {min(top, len(items))} (файл:строка — имя):"))
    for filename, lineno, name, node_type in items[:top]:
        loc = f"{filename}:{lineno}" if lineno else filename
        print(f"  {dim(node_type):<22} {name:<32} {dim(loc)}")


# --------------------------------------------------------------------------- #
# main
# --------------------------------------------------------------------------- #


def main() -> int:
    parser = argparse.ArgumentParser(description="Отчёт о doc-coverage (долге документации).")
    parser.add_argument("--path", default="src", help="Путь для анализа (по умолчанию src)")
    parser.add_argument("--top", type=int, default=20, help="Сколько строк показывать в топах")
    parser.add_argument(
        "--fail-under",
        type=float,
        default=None,
        metavar="N",
        help=(
            "Вернуть ненулевой exit code, если покрытие ниже N%% (для опционального CI-гейта). "
            "По умолчанию берётся fail_under из [tool.interrogate]; передайте 0, чтобы отключить."
        ),
    )
    args = parser.parse_args()

    if InterrogateCoverage is None or interrogate_config is None:
        print(bold("Не найден инструмент interrogate."), file=sys.stderr)
        print("Установите его: pip install interrogate  (входит в [dev] extra)", file=sys.stderr)
        return 1

    path = Path(args.path)
    if not path.exists():
        print(f"Путь не найден: {path}", file=sys.stderr)
        return 2

    print(bold(f"Анализ doc-coverage: {path}"))

    # Читаем [tool.interrogate] из pyproject.toml, чтобы наш отчёт считал
    # покрытие ровно как CLI-гейт `interrogate -c pyproject.toml` (те же
    # ignore-флаги и exclude). Без этого total/процент расходятся.
    raw = interrogate_config.parse_pyproject_toml("pyproject.toml") or {}
    conf_kwargs, excluded, cfg_fail_under = split_pyproject_config(raw)
    conf = interrogate_config.InterrogateConfig(**conf_kwargs)

    # interrogate конкатенирует excluded как tuple — list туда передавать нельзя.
    coverage = InterrogateCoverage(paths=[str(path)], conf=conf, excluded=tuple(excluded) or None)
    results = coverage.get_coverage()

    # Порог гейта: явный --fail-under важнее конфигового fail_under.
    fail_under = args.fail_under if args.fail_under is not None else cfg_fail_under

    section_summary(results)
    section_worst_files(results, args.top)
    section_undocumented(results, args.top)

    header("Итог")
    perc = results.perc_covered
    if fail_under is not None and fail_under > 0:
        if perc < fail_under:
            print(red(f"Покрытие {perc:.1f}% ниже порога {fail_under:.1f}% → провал (fail-under)"))
        else:
            print(good(f"Покрытие {perc:.1f}% не ниже порога {fail_under:.1f}% → ок"))
    else:
        print(dim("Информационный прогон (exit 0). Для CI-гейта используйте --fail-under N."))
    print(dim("NB: doc-coverage ≠ мёртвый код (vulture, scripts/code_health.py) — это разные сигналы."))

    # fail_under == 0 явно отключает гейт (gate_exit_code тогда вернёт 0).
    return gate_exit_code(perc, fail_under if fail_under else None)


if __name__ == "__main__":
    sys.exit(main())
