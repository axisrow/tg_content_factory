#!/usr/bin/env python3
"""Оценка «здоровья кода» (он же замер техдолга / говнокода) по дереву исходников.

Запуск:
    python scripts/code_health.py [--path src] [--top 20] [--fail-on {C,D,E,F}]

Считает воспроизводимую сводку:
  * размер базы и крупнейшие файлы;
  * маркеры мусора (TODO/FIXME, голый except, проглоченные исключения, забытый print, …);
  * цикломатическую сложность функций (radon cc) — топ худших;
  * индекс поддерживаемости файлов (radon mi) — распределение по рангам;
  * мёртвый код (vulture, high-confidence).

Инструменты radon / ruff / vulture обязательны — при их отсутствии скрипт падает
с подсказкой по установке. ruff входит в [dev]; radon и vulture — нет.
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import sys
from collections import Counter
from pathlib import Path

REQUIRED_TOOLS = ("radon", "ruff", "vulture")

# Ранги radon в порядке от лучшего к худшему — единственный источник истины
# про состав/порядок рангов. RANKS и сравнения по порогу выводятся отсюда.
RANK_ORDER = {"A": 0, "B": 1, "C": 2, "D": 3, "E": 4, "F": 5}
RANKS = "".join(RANK_ORDER)

# Пороги политики (что считать «плохим») — собраны в одном месте.
BIG_FILE_LINES = 1000  # файл крупнее → пометка-предупреждение
WORST_CC_FROM = "C"  # с какого ранга функция попадает в топ худших по сложности
BAD_MI_FROM = "B"  # с какого ранга файл считается труднее поддерживаемым
VULTURE_MIN_CONFIDENCE = 80  # порог уверенности vulture для мёртвого кода

# Каталог, где print() — легитимный CLI-вывод, а не забытый дебаг.
CLI_DIR_MARKER = "/cli/"

# Маркеры мусора: подпись секции -> regex.
TRASH_PATTERNS: dict[str, str] = {
    "TODO/FIXME/HACK/XXX": r"\b(?:TODO|FIXME|HACK|XXX)\b",
    "голый except (except:)": r"except\s*:",
    "except Exception (широкий перехват)": r"except Exception",
    "# type: ignore": r"#\s*type:\s*ignore",
    "# noqa": r"#\s*noqa",
}
# Подписи маркеров, которые всегда плохи (красим их счётчик красным).
ALWAYS_BAD_MARKERS = {"голый except (except:)"}

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


# Цвет по рангу radon — единая палитра, привязанная к RANK_ORDER.
_RANK_PALETTE = {"A": good, "B": good, "C": warn, "D": warn, "E": red, "F": red}


def rank_color(rank: str, text: str | None = None) -> str:
    """Покрасить текст (по умолчанию сам ранг) в цвет ранга radon."""
    paint = _RANK_PALETTE.get(rank, lambda t: t)
    return paint(rank if text is None else text)


def rank_histogram(rank_counts: Counter[str]) -> str:
    """Строка вида 'A:325  B:9  C:10  …' с покраской по рангу."""
    return "  ".join(rank_color(r, f"{r}:{rank_counts.get(r, 0)}") for r in RANKS)


def header(title: str) -> None:
    print()
    print(bold(f"=== {title} ==="))


def ensure_tools() -> None:
    """Жёсткая проверка наличия внешних инструментов."""
    missing = [tool for tool in REQUIRED_TOOLS if shutil.which(tool) is None]
    if missing:
        print(bold("Не найдены обязательные инструменты:"), ", ".join(missing), file=sys.stderr)
        print(f"Установите их: pip install {' '.join(missing)}", file=sys.stderr)
        sys.exit(1)


def run_tool(args: list[str]) -> tuple[str, str, int]:
    """Запустить внешний инструмент, вернуть (stdout, stderr, returncode)."""
    proc = subprocess.run(args, capture_output=True, text=True)
    return proc.stdout, proc.stderr, proc.returncode


def radon_json(subcmd: str, path: Path, *extra: str) -> dict | None:
    """Запустить `radon <subcmd> <path> -j …`, вернуть разобранный JSON или None."""
    out, err, code = run_tool(["radon", subcmd, str(path), "-j", *extra])
    if code != 0 or not out.strip():
        print(dim(f"radon {subcmd} завершился с ошибкой: {err.strip() or code}"))
        return None
    return json.loads(out)


def iter_py_files(root: Path) -> list[Path]:
    if root.is_file():
        return [root] if root.suffix == ".py" else []
    return sorted(root.rglob("*.py"))


def read_sources(files: list[Path]) -> dict[Path, str]:
    """Прочитать каждый файл ровно один раз; нечитаемые пропустить."""
    sources: dict[Path, str] = {}
    for path in files:
        try:
            sources[path] = path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
    return sources


# --------------------------------------------------------------------------- #
# Секции отчёта
# --------------------------------------------------------------------------- #


def section_size(sources: dict[Path, str], top: int) -> None:
    header("Размер кодовой базы")
    line_counts = sorted(((text.count("\n") + 1, path) for path, text in sources.items()), reverse=True)
    total = sum(n for n, _ in line_counts)

    print(f"Файлов .py: {bold(str(len(sources)))}    Строк всего: {bold(str(total))}")
    print(dim(f"\nТоп-{top} крупнейших файлов:"))
    for n, path in line_counts[:top]:
        flag = "  ⚠" if n >= BIG_FILE_LINES else ""
        print(f"  {n:>6}  {path}{flag}")


def section_trash(sources: dict[Path, str], top: int) -> None:
    header("Маркеры мусора")
    compiled = {name: re.compile(pat) for name, pat in TRASH_PATTERNS.items()}
    counts: Counter[str] = Counter()
    broad_files: Counter[Path] = Counter()  # только по широкому перехвату — единственное, что выводим
    broad_marker = "except Exception (широкий перехват)"
    print_in_cli = 0
    print_outside_cli = 0
    print_outside_files: Counter[Path] = Counter()
    print_re = re.compile(r"\bprint\(")

    for path, text in sources.items():
        in_cli = CLI_DIR_MARKER in path.as_posix()
        for line in text.splitlines():
            for name, rx in compiled.items():
                if rx.search(line):
                    counts[name] += 1
                    if name == broad_marker:
                        broad_files[path] += 1
            if print_re.search(line):
                if in_cli:
                    print_in_cli += 1
                else:
                    print_outside_cli += 1
                    print_outside_files[path] += 1

    for name in TRASH_PATTERNS:
        n = counts[name]
        value = red(str(n)) if n and name in ALWAYS_BAD_MARKERS else str(n)
        label = f"  {name}:"
        print(f"{label:<42} {value}")

    print(f"  {'print() в src/cli/ (легитимный вывод):':<42} {print_in_cli}")
    suspicious = red(str(print_outside_cli)) if print_outside_cli else str(print_outside_cli)
    print(f"  {'print() вне cli/ (подозрительно):':<42} {suspicious}")

    if print_outside_cli:
        print(dim("\n  Где print() вне CLI:"))
        for path, n in print_outside_files.most_common(top):
            print(f"    {n:>4}  {path}")

    # Топ файлов с широким перехватом — частый источник скрытых багов.
    if broad_files:
        print(dim(f"\n  Топ-{min(top, len(broad_files))} файлов по 'except Exception':"))
        for path, n in broad_files.most_common(top):
            print(f"    {n:>4}  {path}")


def section_complexity(path: Path, top: int, fail_on: str | None) -> int:
    """Возвращает число функций с рангом >= fail_on (для exit code)."""
    header("Цикломатическая сложность (radon cc)")
    data = radon_json("cc", path, "-a")
    if data is None:
        return 0

    blocks: list[dict] = []
    for file_path, items in data.items():
        for item in items:
            item["_file"] = file_path
            blocks.append(item)

    if not blocks:
        print(dim("блоков не найдено"))
        return 0

    avg = sum(b["complexity"] for b in blocks) / len(blocks)
    print(f"Проанализировано блоков: {bold(str(len(blocks)))}    Средняя сложность: {bold(f'{avg:.2f}')}")
    print(f"Распределение: {rank_histogram(Counter(b['rank'] for b in blocks))}")

    worst = sorted(
        (b for b in blocks if RANK_ORDER.get(b["rank"], 0) >= RANK_ORDER[WORST_CC_FROM]),
        key=lambda b: b["complexity"],
        reverse=True,
    )
    print(dim(f"\nТоп-{top} самых сложных функций (ранг {WORST_CC_FROM} и хуже):"))
    if not worst:
        print(dim("  таких нет — отлично"))
    for b in worst[:top]:
        loc = f"{b['_file']}:{b['lineno']}"
        name = b.get("classname", "")
        name = f"{name}.{b['name']}" if name else b["name"]
        rank = rank_color(b["rank"], f"{b['rank']}({b['complexity']})")
        print(f"  {rank:<18} {name}  {dim(loc)}")

    if fail_on is None:
        return 0
    threshold = RANK_ORDER[fail_on]
    return sum(1 for b in blocks if RANK_ORDER.get(b["rank"], 0) >= threshold)


def section_maintainability(path: Path, top: int) -> None:
    header("Индекс поддерживаемости (radon mi)")
    data = radon_json("mi", path)
    if data is None:
        return

    rank_counts: Counter[str] = Counter()
    bad: list[tuple[str, str, float]] = []
    for file_path, info in data.items():
        if not isinstance(info, dict) or "rank" not in info:
            continue
        rank = info["rank"]
        rank_counts[rank] += 1
        if RANK_ORDER.get(rank, 0) >= RANK_ORDER[BAD_MI_FROM]:
            bad.append((file_path, rank, info.get("mi", 0.0)))

    total = sum(rank_counts.values()) or 1
    a_share = 100 * rank_counts.get("A", 0) / total
    print(f"Распределение по рангам: {rank_histogram(rank_counts)}   ({a_share:.0f}% в ранге A)")

    bad.sort(key=lambda x: (RANK_ORDER.get(x[1], 0), -x[2]), reverse=True)
    print(dim(f"\nФайлы хуже ранга A (труднее поддерживать), топ-{top}:"))
    if not bad:
        print(dim("  таких нет — отлично"))
    for file_path, rank, mi in bad[:top]:
        print(f"  {rank_color(rank):<3} {mi:>6.2f}  {file_path}")


def section_dead_code(path: Path, top: int) -> None:
    header(f"Мёртвый код (vulture, confidence ≥ {VULTURE_MIN_CONFIDENCE}%)")
    out, err, code = run_tool(["vulture", str(path), "--min-confidence", str(VULTURE_MIN_CONFIDENCE)])
    # vulture возвращает ненулевой код, когда находки есть — это норма.
    lines = [ln for ln in out.splitlines() if ln.strip()]
    if not lines and code not in (0, 1):
        print(dim(f"vulture завершился с ошибкой: {err.strip() or code}"))
        return
    print(f"Находок (high-confidence): {bold(str(len(lines)))}")
    if lines:
        print(dim(f"\nПервые {min(top, len(lines))}:"))
        for ln in lines[:top]:
            print(f"  {ln}")


# --------------------------------------------------------------------------- #
# main
# --------------------------------------------------------------------------- #


def main() -> int:
    parser = argparse.ArgumentParser(description="Оценка здоровья кода / замер техдолга.")
    parser.add_argument("--path", default="src", help="Путь для анализа (по умолчанию src)")
    parser.add_argument("--top", type=int, default=20, help="Сколько строк показывать в топах")
    parser.add_argument(
        "--fail-on",
        choices=["C", "D", "E", "F"],
        default=None,
        help="Вернуть ненулевой exit code, если есть функции этого ранга или хуже (для CI)",
    )
    args = parser.parse_args()

    ensure_tools()

    path = Path(args.path)
    if not path.exists():
        print(f"Путь не найден: {path}", file=sys.stderr)
        return 2

    files = iter_py_files(path)
    if not files:
        print(f"В {path} нет .py-файлов", file=sys.stderr)
        return 2

    print(bold(f"Анализ: {path}  ({len(files)} файлов)"))

    sources = read_sources(files)
    section_size(sources, args.top)
    section_trash(sources, args.top)
    over_threshold = section_complexity(path, args.top, args.fail_on)
    section_maintainability(path, args.top)
    section_dead_code(path, args.top)

    header("Итог")
    if args.fail_on:
        if over_threshold:
            print(red(f"Функций ранга {args.fail_on} и хуже: {over_threshold} → провал (--fail-on)"))
            return 1
        print(good(f"Функций ранга {args.fail_on} и хуже не найдено → ок"))
    else:
        print(dim("Информационный прогон (exit 0). Для CI используйте --fail-on F."))
    return 0


if __name__ == "__main__":
    sys.exit(main())
