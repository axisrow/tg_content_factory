#!/usr/bin/env python3
"""Детектор «изобретённого велосипеда»: самописный код, для которого есть
battle-tested стандартная замена (stdlib или уже-установленная зависимость).

Запуск:
    python scripts/detect_reinvented.py [--path src] [--json] [--update-baseline] [--fail-on-new]
    python scripts/detect_reinvented.py --journal-rows        # строки для журнала #782 по НОВЫМ находкам
    python scripts/detect_reinvented.py --create-issue        # ручной запуск → авто-issue с находками

Ядро эпика #1083 (детектор кастомных функций → стандартные библиотеки). Скрипт
прогоняет AST-эвристики по дереву исходников и печатает список кандидатов в
формате:

    файл:строка — что делает — предлагаемая стандартная замена — уверенность

Эвристики (AST-based, не regex — `ast.walk`/`ast.NodeVisitor`):
  * ручная HMAC/JWT-подпись токенов (есть `itsdangerous`/`hmac`);
  * ручной base64-паддинг (есть `base64.urlsafe_b64decode` без ручной добивки `=`);
  * самописный retry/backoff-цикл (есть `tenacity`/`aiolimiter`);
  * ручной разбор email через `split('@')` (есть `email.utils.parseaddr`);
  * самописный счётчик/частотный словарь (есть `collections.Counter`).

Разбор URL строками (`split('/')`/`'?'`/`'#'`) намеренно НЕ ловится: эти
разделители встречаются повсеместно вне URL-контекста, и эвристика на них
завалила бы отчёт false-positive'ами — против философии «лучше пропуск, чем
ложная находка». Для URL остаётся `urllib.parse`, но детектор его не флагует.

Эвристики намеренно консервативны: лучше пропустить сомнительный кейс, чем
завалить отчёт false-positive'ами (см. отрицательные фикстуры в тестах).

Baseline (`scripts/reinvented_baseline.json`): первый прогон фиксирует текущие
находки в снимок; последующие прогоны диффят против него и подсвечивают
**НОВЫЕ** велосипеды — так в регрессию ловятся свежие самописные реализации, а
накопленный долг не шумит каждый раз. `--update-baseline` пересоздаёт снимок.

Advisory по умолчанию: exit 0 (как scripts/doc_coverage.py). `--fail-on-new`
включает гейт — ненулевой код, если относительно baseline появились НОВЫЕ
находки (для опционального CI-шага, расписание — отдельный sub, #1097).

Ручной запуск (#1109, решение владельца в #1097: НЕ cron, НЕ CI-шаг):
  * `--journal-rows` печатает строки для механической дозаписи в журнал-реестр
    #782 (`Дата | Находка | Замена | Решение | PR/issue`) по НОВЫМ находкам.
    Сам коммент в #782 пишет владелец — детектор только готовит строки.
  * `--create-issue` авто-создаёт GitHub issue с новыми находками через
    `gh issue create`, связывая его с эпиком #1083 и журналом #782. Тело issue
    содержит читаемую таблицу находок + готовые строки для журнала #782.
"""

from __future__ import annotations

import argparse
import ast
import json
import subprocess
import sys
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import date as _date
from pathlib import Path
from typing import Any, Protocol

# Снимок-baseline лежит рядом со скриптом (в scripts/), а не в cwd — чтобы запуск
# из любого каталога видел тот же файл. Единый источник истины про путь.
BASELINE_PATH = Path(__file__).resolve().parent / "reinvented_baseline.json"

Confidence = str  # "high" | "med" | "low" — порядок ниже
_CONFIDENCE_ORDER = {"high": 0, "med": 1, "low": 2}

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


_CONFIDENCE_PAINT = {"high": red, "med": warn, "low": dim}


def confidence_color(confidence: Confidence) -> str:
    paint = _CONFIDENCE_PAINT.get(confidence, lambda t: t)
    return paint(confidence)


# --------------------------------------------------------------------------- #
# Модель находки
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class Finding:
    """Один кандидат на замену стандартной библиотекой.

    `kind` — стабильный машинный ключ эвристики (используется в baseline-диффе,
    не зависит от человекочитаемого текста). `what` — что делает код,
    `replacement` — предлагаемая стандартная замена, `confidence` — уверенность.
    """

    file: str
    line: int
    kind: str
    what: str
    replacement: str
    confidence: Confidence

    def key(self) -> tuple[str, str, int]:
        """Ключ дедупа В РАМКАХ ОДНОГО прогона: (файл, тип, строка).

        Включает строку намеренно — две разные находки одного типа в одном файле
        (на разных строках) должны остаться обе. Для диффа МЕЖДУ прогонами строка
        не годится (см. identity): вставка строки выше сдвинула бы номер.
        """
        return (self.file, self.kind, self.line)

    def identity(self) -> tuple[str, str, str]:
        """Стабильный отпечаток для baseline-диффа: (файл, тип, что).

        НЕ зависит от номера строки — `what` содержит имя функции (например
        «…цикл в collect_all_channels()»), что переживает вставку/удаление строк
        выше находки и при этом различает разные функции одного типа в файле.
        Без этого `--fail-on-new` перепомечал бы неизменный код как «новый» после
        любого рефакторинга, сдвигающего строки (находка ревью Codex на #1110).
        """
        return (self.file, self.kind, self.what)

    def human(self) -> str:
        """Строка отчёта: `файл:строка — что — замена — уверенность`."""
        loc = f"{self.file}:{self.line}"
        return f"{loc} — {self.what} — → {self.replacement} — {self.confidence}"


# --------------------------------------------------------------------------- #
# Чистая логика эвристик (тестируется на фикстурах, без файловой системы)
# --------------------------------------------------------------------------- #


def _attr_chain(node: ast.AST) -> str:
    """Восстановить пунктирное имя из узла-вызова: `hmac.new` → 'hmac.new'.

    Возвращает '' для всего, что не разворачивается в простую цепочку имён.
    """
    parts: list[str] = []
    cur: ast.AST | None = node
    while isinstance(cur, ast.Attribute):
        parts.append(cur.attr)
        cur = cur.value
    if isinstance(cur, ast.Name):
        parts.append(cur.id)
    else:
        return ""
    return ".".join(reversed(parts))


def _called_name(call: ast.Call) -> str:
    """Имя вызываемого: `hmac.new(...)` → 'hmac.new', `range(...)` → 'range'."""
    if isinstance(call.func, ast.Name):
        return call.func.id
    if isinstance(call.func, ast.Attribute):
        return _attr_chain(call.func)
    return ""


def _calls_in(node: ast.AST) -> list[ast.Call]:
    return [n for n in ast.walk(node) if isinstance(n, ast.Call)]


def _call_names_in(node: ast.AST) -> set[str]:
    return {_called_name(c) for c in _calls_in(node)}


def _func_defs(tree: ast.AST) -> list[ast.FunctionDef | ast.AsyncFunctionDef]:
    return [n for n in ast.walk(tree) if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))]


def _detect_handrolled_signing(
    func: ast.FunctionDef | ast.AsyncFunctionDef, filename: str
) -> Finding | None:
    """Ручная криптоподпись токена: `hmac.new`/`hashlib.*` + base64 + json в одной функции.

    Эталон эпика — hand-rolled HMAC-JWT из `src/web/session.py` (#782 / #1): функция
    собирала `base64url(HMAC-SHA256(secret, payload))` руками. Зрелая замена —
    `itsdangerous.Signer` (на которую session.py и переехал в #953), либо
    PyJWT/`hmac.compare_digest`. High-confidence, потому что комбинация
    «своя крипта поверх json-payload» почти всегда — велосипед.
    """
    names = _call_names_in(func)
    has_hmac = any(n.startswith("hmac.") for n in names)
    has_hashlib_digest = any(n.startswith("hashlib.") for n in names)
    has_b64 = any("b64" in n or "base64" in n for n in names)
    has_json = any(n.startswith("json.") for n in names)
    # Подпись = крипто-примитив (hmac или сырой hashlib-дайджест) + сериализация
    # payload в base64/json. Без payload-части это просто хеширование, не подпись.
    crypto = has_hmac or has_hashlib_digest
    if crypto and has_b64 and has_json:
        confidence = "high" if has_hmac else "med"
        return Finding(
            file=filename,
            line=func.lineno,
            kind="handrolled-token-signing",
            what=f"ручная HMAC/JWT-подпись токена в {func.name}()",
            replacement="itsdangerous.Signer / PyJWT (+ hmac.compare_digest)",
            confidence=confidence,
        )
    return None


def _detect_manual_b64_padding(tree: ast.AST, filename: str) -> list[Finding]:
    """Ручная добивка base64-паддинга `=` перед urlsafe_b64decode.

    Паттерн `s += "=" * (...)` / `padding = 4 - len(s) % 4` ради ручного
    выравнивания длины — частый спутник самописных токенов. `base64` умеет
    декодировать без ручной добивки, если использовать `b64decode(..., validate)`
    с уже выровненной строкой, а для urlsafe есть готовые рецепты.

    Флагим `"=" * n` ТОЛЬКО рядом (в пределах нескольких строк) с b64-вызовом:
    без этого `print("=" * 80)` и прочие разделители давали бы сплошной low-шум
    (находка ревью на #1110). Раз b64-контекст обязателен — уверенность med.
    """
    findings: list[Finding] = []
    b64_lines = {
        c.lineno
        for c in _calls_in(tree)
        if "b64" in _called_name(c) or "base64" in _called_name(c)
    }
    if not b64_lines:  # нет ни одного base64-вызова — '=' * n заведомо не паддинг
        return findings
    for node in ast.walk(tree):
        # `"=" * expr` — умножение строки из одного `=` на число: кандидат в паддинг.
        if (
            isinstance(node, ast.BinOp)
            and isinstance(node.op, ast.Mult)
            and isinstance(node.left, ast.Constant)
            and node.left.value == "="
        ):
            # Только рядом с b64-вызовом — иначе это разделитель, а не паддинг.
            if not any(abs(node.lineno - bl) <= 8 for bl in b64_lines):
                continue
            findings.append(
                Finding(
                    file=filename,
                    line=node.lineno,
                    kind="manual-base64-padding",
                    what="ручная добивка base64-паддинга '='",
                    replacement="base64.urlsafe_b64decode с корректным выравниванием",
                    confidence="med",
                )
            )
    return findings


def _detect_retry_loop(
    func: ast.FunctionDef | ast.AsyncFunctionDef, filename: str
) -> Finding | None:
    """Самописный retry/backoff: цикл с try/except + sleep внутри тела.

    Паттерн `for _ in range(retries): try: ... except: sleep(backoff)` — ровно то,
    что инкапсулируют `tenacity` (синхронно/async) и `aiolimiter` (rate-limit).
    Med-confidence: цикл-с-перехватом-и-задержкой почти всегда повторная попытка,
    но бывают легитимные poll-циклы — поэтому не high.
    """
    for loop in ast.walk(func):
        if not isinstance(loop, (ast.For, ast.While)):
            continue
        has_try = any(isinstance(n, ast.Try) for n in ast.walk(loop))
        sleeps = {
            _called_name(c)
            for c in _calls_in(loop)
            if _called_name(c) in ("time.sleep", "asyncio.sleep", "sleep")
        }
        if has_try and sleeps:
            return Finding(
                file=filename,
                line=loop.lineno,
                kind="handrolled-retry-loop",
                what=f"самописный retry/backoff-цикл в {func.name}()",
                replacement="tenacity.retry / aiolimiter (для rate-limit)",
                confidence="med",
            )
    return None


def _detect_manual_email_parse(tree: ast.AST, filename: str) -> list[Finding]:
    """Ручной разбор email через `addr.split('@')` → `email.utils.parseaddr`.

    Low-confidence: `split('@')` встречается и вне email-контекста, поэтому это
    слабый сигнал. Разбор URL по `/`/`?`/`#` сознательно НЕ ловим — эти
    разделители слишком частые вне URL, эвристика на них была бы сплошным
    false-positive (см. модульный docstring).
    """
    findings: list[Finding] = []
    for node in ast.walk(tree):
        if not (isinstance(node, ast.Call) and _called_name(node).endswith(".split")):
            continue
        if not (node.args and isinstance(node.args[0], ast.Constant)):
            continue
        sep = node.args[0].value
        if sep == "@":
            findings.append(
                Finding(
                    file=filename,
                    line=node.lineno,
                    kind="manual-email-parse",
                    what="ручной разбор email через split('@')",
                    replacement="email.utils.parseaddr",
                    confidence="low",
                )
            )
    return findings


def _detect_manual_counter(
    func: ast.FunctionDef | ast.AsyncFunctionDef, filename: str
) -> Finding | None:
    """Самописный частотный счётчик `d[k] = d.get(k, 0) + 1`.

    Классический велосипед поверх `collections.Counter`. Ищем присваивание в
    индекс, где правая часть — `<subscript-или-get> + 1`. Med-confidence: паттерн
    узкий и почти не даёт ложных срабатываний.
    """
    for node in ast.walk(func):
        if not (isinstance(node, ast.Assign) and len(node.targets) == 1):
            continue
        target = node.targets[0]
        if not isinstance(target, ast.Subscript):
            continue
        value = node.value
        # RHS вида `<что-то> + 1` (или `1 + <что-то>`).
        if isinstance(value, ast.BinOp) and isinstance(value.op, ast.Add):
            operands = [value.left, value.right]
            has_one = any(isinstance(o, ast.Constant) and o.value == 1 for o in operands)
            # один из операндов — обращение к тому же контейнеру (subscript или .get)
            has_lookup = any(
                isinstance(o, ast.Subscript)
                or (isinstance(o, ast.Call) and _called_name(o).endswith(".get"))
                for o in operands
            )
            if has_one and has_lookup:
                return Finding(
                    file=filename,
                    line=node.lineno,
                    kind="manual-counter",
                    what=f"самописный частотный счётчик в {func.name}()",
                    replacement="collections.Counter",
                    confidence="med",
                )
    return None


def detect_in_source(source: str, filename: str) -> list[Finding]:
    """Прогнать все эвристики по одному исходнику. Чистая функция — ядро тестов.

    Невалидный Python (SyntaxError) даёт пустой список, а не исключение: отчёт
    advisory, один битый файл не должен ронять прогон.
    """
    try:
        tree = ast.parse(source, filename=filename)
    except SyntaxError:
        return []

    findings: list[Finding] = []
    for func in _func_defs(tree):
        signing = _detect_handrolled_signing(func, filename)
        if signing:
            findings.append(signing)
        retry = _detect_retry_loop(func, filename)
        if retry:
            findings.append(retry)
        counter = _detect_manual_counter(func, filename)
        if counter:
            findings.append(counter)

    findings.extend(_detect_manual_b64_padding(tree, filename))
    findings.extend(_detect_manual_email_parse(tree, filename))

    # Дедуп по (файл, тип, строка): вложенная функция и её внешняя обёртка обе
    # видят один и тот же цикл через ast.walk → одна находка, не две с разными
    # именами функций. Оставляем первую после сортировки (детерминированно).
    return _dedup_sorted(findings)


def _dedup_sorted(findings: list[Finding]) -> list[Finding]:
    """Отсортировать детерминированно и убрать дубли по key() (файл, тип, строка)."""
    findings.sort(key=lambda f: (f.file, f.line, _CONFIDENCE_ORDER.get(f.confidence, 9), f.kind))
    seen: set[tuple[str, str, int]] = set()
    unique: list[Finding] = []
    for f in findings:
        if f.key() in seen:
            continue
        seen.add(f.key())
        unique.append(f)
    return unique


# --------------------------------------------------------------------------- #
# Baseline-логика (чистая, тестируется на списках Finding)
# --------------------------------------------------------------------------- #


def findings_to_baseline(findings: list[Finding]) -> list[dict]:
    """Сериализовать находки в стабильный список dict'ов для JSON-снимка."""
    return [asdict(f) for f in sorted(findings, key=lambda f: f.key())]


def load_baseline(path: Path) -> set[tuple[str, str, str]] | None:
    """Прочитать снимок и вернуть множество identity-отпечатков. None — снимка нет.

    Отпечаток — `(file, kind, what)` (см. Finding.identity), а НЕ номер строки:
    дифф должен переживать сдвиг строк после рефакторингов.
    """
    if not path.exists():
        return None
    raw = json.loads(path.read_text(encoding="utf-8"))
    items = raw.get("findings", []) if isinstance(raw, dict) else raw
    return {(item["file"], item["kind"], item["what"]) for item in items}


def new_findings(
    findings: list[Finding], baseline_ids: set[tuple[str, str, str]] | None
) -> list[Finding]:
    """Находки, которых нет в baseline (по identity). Без baseline — все новые.

    Сравнение по identity (file/kind/what), не по строке: неизменный код,
    сдвинутый вставкой строк выше, не считается новым велосипедом.
    """
    if baseline_ids is None:
        return list(findings)
    return [f for f in findings if f.identity() not in baseline_ids]


# --------------------------------------------------------------------------- #
# Формат журнала #782 + тело GitHub issue (чистые функции, тестируются без сети)
#
# Журнал-реестр #782 — markdown-таблица с шапкой:
#   | Дата | Находка (файл) | Стандартная замена | Решение | PR/issue |
# Детектор готовит строки в этом формате для МЕХАНИЧЕСКОЙ дозаписи владельцем
# (`Решение = _ожидает решения_`, `PR/issue = —`). Сам коммент в #782 пишет
# владелец/процесс — детектор только формирует строки (см. #1109).
# --------------------------------------------------------------------------- #

_JOURNAL_DECISION_PLACEHOLDER = "_ожидает решения_"
_JOURNAL_PR_PLACEHOLDER = "—"

# Зонтичные issue эпика: #1083 — детектор-эпик, #782 — журнал-реестр находок.
# Тело авто-issue ссылается на них, чтобы новые находки связывались с зонтиком.
_PARENT_EPIC = 1083
_REGISTRY_JOURNAL = 782


def _md_cell(text: str) -> str:
    """Экранировать текст для ячейки markdown-таблицы: '|' → '\\|', срезать переводы строк."""
    return text.replace("\n", " ").replace("|", "\\|").strip()


def journal_row(finding: Finding, date: str) -> str:
    """Одна строка таблицы журнала #782 для находки.

    Формат колонок журнала-реестра #782:
        | Дата | Находка (файл:строка) | Стандартная замена | Решение | PR/issue |
    «Находка» включает файл:строку, чтобы строка была самодостаточной при вставке
    в журнал. `Решение`/`PR` — заглушки: решение принимает владелец, не детектор.
    """
    # Дата тоже экранируется: `--date` приходит от оператора без валидации, '|' в
    # нём ломал бы таблицу журнала так же, как в любой другой ячейке (ревью #1113).
    date_cell = _md_cell(date)
    finding_cell = _md_cell(f"{finding.what} ({finding.file}:{finding.line})")
    replacement_cell = _md_cell(finding.replacement)
    return (
        f"| {date_cell} | {finding_cell} | {replacement_cell} "
        f"| {_JOURNAL_DECISION_PLACEHOLDER} | {_JOURNAL_PR_PLACEHOLDER} |"
    )


def findings_to_journal_rows(findings: list[Finding], date: str) -> list[str]:
    """Строки журнала #782 для каждой находки (детерминированный порядок входа)."""
    return [journal_row(f, date) for f in findings]


def _findings_report_table(findings: list[Finding]) -> str:
    """Человекочитаемая markdown-таблица находок для тела issue (единый блок).

    Колонки соответствуют формату отчёта из #1109:
        Находка (файл:строка) | Что делает | Стандартная замена | Уверенность
    Возвращается ОДНОЙ строкой с `\\n` между рядами — пустые строки внутри
    таблицы ломают рендеринг markdown-таблицы в GitHub, поэтому секции склеиваются
    в теле через `\\n\\n`, а ряды одной таблицы — через `\\n`.
    """
    lines = [
        "| Находка (файл:строка) | Что делает | Стандартная замена | Уверенность |",
        "|---|---|---|---|",
    ]
    for f in findings:
        loc = _md_cell(f"{f.file}:{f.line}")
        lines.append(
            f"| {loc} | {_md_cell(f.what)} | {_md_cell(f.replacement)} | {f.confidence} |"
        )
    return "\n".join(lines)


def build_issue_title(findings: list[Finding], date: str) -> str:
    """Заголовок авто-issue: число новых велосипедов + дата прогона."""
    return f"detect-reinvented: {len(findings)} новых кандидатов на замену ({date})"


def build_issue_body(findings: list[Finding], date: str) -> str:
    """Тело авто-issue с новыми находками детектора.

    Содержит: (1) связь с зонтичным эпиком #1083 и журналом #782; (2) читаемую
    таблицу находок; (3) готовые строки для механической дозаписи в журнал #782.

    Пустой список — ошибка: issue без находок не создаётся (вызывающий код гвардит
    `if not new`), а тело с header-only таблицами бессмысленно (ревью #1113).
    """
    if not findings:
        raise ValueError("build_issue_body: пустой список находок — issue без находок не создаётся")
    parts: list[str] = []
    parts.append(
        f"Автоматический прогон детектора «изобретённого велосипеда» "
        f"(`scripts/detect_reinvented.py`, #{_PARENT_EPIC}) от {date} нашёл "
        f"**{len(findings)}** новых кандидатов на замену стандартной библиотекой "
        f"относительно baseline."
    )
    parts.append(
        f"Часть эпика #{_PARENT_EPIC}. Находки ниже предлагаются к дозаписи в "
        f"журнал-реестр #{_REGISTRY_JOURNAL} — решение по каждой (заменять / "
        f"оставить / отложить) принимает владелец, не детектор."
    )
    parts.append("## Новые находки\n\n" + _findings_report_table(findings))
    journal_block = "\n".join(
        [
            "| Дата | Находка (файл) | Стандартная замена | Решение | PR/issue |",
            "|------|----------------|--------------------|---------|----------|",
            *findings_to_journal_rows(findings, date),
        ]
    )
    parts.append(
        f"## Строки для журнала #{_REGISTRY_JOURNAL}\n\n"
        f"Скопировать в таблицу журнала #{_REGISTRY_JOURNAL} "
        f"(`Решение = {_JOURNAL_DECISION_PLACEHOLDER}`, `PR/issue = {_JOURNAL_PR_PLACEHOLDER}`):\n\n"
        f"{journal_block}"
    )
    return "\n\n".join(parts) + "\n"


# --------------------------------------------------------------------------- #
# Создание GitHub issue через gh (раннер инъектируется — мокается в тестах,
# реальные issue в тестах НЕ плодятся).
# --------------------------------------------------------------------------- #


class _CompletedProcess(Protocol):
    """Минимальная форма результата раннера (subprocess.CompletedProcess-совместимая)."""

    returncode: int
    stdout: str
    stderr: str


Runner = Callable[..., _CompletedProcess]


def _default_runner(cmd: list[str], **kwargs: Any) -> Any:
    """Реальный раннер: subprocess.run с захватом текста. По умолчанию для gh."""
    return subprocess.run(cmd, capture_output=True, text=True, **kwargs)


def create_github_issue(
    title: str,
    body: str,
    *,
    labels: list[str] | None = None,
    repo: str | None = None,
    runner: Runner = _default_runner,
) -> str:
    """Создать GitHub issue через `gh issue create`; вернуть URL созданного issue.

    `runner` инъектируется (по умолчанию реальный subprocess.run) — в тестах
    подменяется фейком, чтобы не создавать реальные issue. Ненулевой код gh →
    RuntimeError со stderr (не молчаливое проглатывание).
    """
    cmd = ["gh", "issue", "create", "--title", title, "--body", body]
    for label in labels or []:
        cmd += ["--label", label]
    if repo:
        cmd += ["--repo", repo]
    result = runner(cmd)
    if result.returncode != 0:
        raise RuntimeError(f"gh issue create провалился: {result.stderr.strip()}")
    return result.stdout.strip()


def maybe_create_issue_for_new(
    new: list[Finding],
    date: str,
    *,
    labels: list[str] | None = None,
    repo: str | None = None,
    runner: Runner = _default_runner,
) -> str | None:
    """Создать issue с новыми находками, если они есть. Нет находок → None (issue не плодим)."""
    if not new:
        return None
    title = build_issue_title(new, date)
    body = build_issue_body(new, date)
    return create_github_issue(title, body, labels=labels, repo=repo, runner=runner)


# --------------------------------------------------------------------------- #
# Файловый слой
# --------------------------------------------------------------------------- #


def iter_py_files(root: Path) -> list[Path]:
    if root.is_file():
        return [root] if root.suffix == ".py" else []
    return sorted(root.rglob("*.py"))


def scan_path(root: Path) -> list[Finding]:
    """Прогнать детектор по всем .py под root. Нечитаемые файлы пропускаются."""
    findings: list[Finding] = []
    for path in iter_py_files(root):
        try:
            source = path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
        findings.extend(detect_in_source(source, path.as_posix()))
    return _dedup_sorted(findings)


def write_baseline(path: Path, findings: list[Finding]) -> None:
    payload = {
        "_comment": (
            "Baseline-снимок scripts/detect_reinvented.py (#1108). Фиксирует "
            "текущие находки-велосипеды, чтобы прогоны диффили и подсвечивали НОВЫЕ. "
            "Пересоздать: python scripts/detect_reinvented.py --update-baseline."
        ),
        "findings": findings_to_baseline(findings),
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


# --------------------------------------------------------------------------- #
# Отчёт
# --------------------------------------------------------------------------- #


def header(title: str) -> None:
    print()
    print(bold(f"=== {title} ==="))


def print_report(findings: list[Finding], new: list[Finding], baseline_existed: bool) -> None:
    print(bold(f"Детектор «изобретённого велосипеда» — находок: {len(findings)}"))

    by_conf = {c: [f for f in findings if f.confidence == c] for c in ("high", "med", "low")}
    header("Все находки (файл:строка — что — замена — уверенность)")
    if not findings:
        print(dim("  кандидатов не найдено — отлично"))
    for conf in ("high", "med", "low"):
        group = by_conf[conf]
        if not group:
            continue
        print(dim(f"\n  {confidence_color(conf)} ({len(group)}):"))
        for f in group:
            print(f"    {f.human()}")

    header("Новые велосипеды относительно baseline")
    if not baseline_existed:
        print(warn("  baseline ещё нет — этот прогон станет снимком (запустите --update-baseline)"))
    elif not new:
        print(good("  новых находок нет — регрессий не внесено"))
    else:
        print(red(f"  НОВЫХ находок: {len(new)}"))
        for f in new:
            print(f"    {red('NEW')} {f.human()}")


# --------------------------------------------------------------------------- #
# main
# --------------------------------------------------------------------------- #


def main() -> int:
    parser = argparse.ArgumentParser(description="Детектор кастомных функций → стандартные библиотеки.")
    parser.add_argument("--path", default="src", help="Путь для анализа (по умолчанию src)")
    parser.add_argument("--json", action="store_true", help="Машинный JSON-вывод вместо человекочитаемого отчёта")
    parser.add_argument(
        "--update-baseline",
        action="store_true",
        help=f"Пересоздать baseline-снимок ({BASELINE_PATH.name}) по текущим находкам и выйти",
    )
    parser.add_argument(
        "--fail-on-new",
        action="store_true",
        help="Вернуть ненулевой exit code, если относительно baseline появились НОВЫЕ находки (для CI)",
    )
    parser.add_argument(
        "--journal-rows",
        action="store_true",
        help=f"Напечатать готовые строки для журнала-реестра #{_REGISTRY_JOURNAL} по НОВЫМ находкам и выйти",
    )
    parser.add_argument(
        "--create-issue",
        action="store_true",
        help=(
            f"Создать GitHub issue с НОВЫМИ находками через `gh issue create` "
            f"(связать с эпиком #{_PARENT_EPIC} / журналом #{_REGISTRY_JOURNAL}). "
            "Ручной запуск — не CI, не cron (решение #1097)."
        ),
    )
    parser.add_argument(
        "--issue-label",
        action="append",
        default=None,
        metavar="LABEL",
        help="Метка для создаваемого issue (можно повторять); по умолчанию без меток",
    )
    parser.add_argument(
        "--repo",
        default=None,
        help="OWNER/REPO для `gh issue create` (по умолчанию репозиторий текущего каталога)",
    )
    parser.add_argument(
        "--date",
        default=None,
        help="Дата прогона YYYY-MM-DD для строк журнала/issue (по умолчанию сегодня)",
    )
    args = parser.parse_args()

    # Дата фиксируется здесь (impure-слой) и прокидывается в чистые функции —
    # формирование строк/тела остаётся детерминируемым и тестируемым.
    run_date = args.date or _date.today().isoformat()

    path = Path(args.path)
    if not path.exists():
        print(f"Путь не найден: {path}", file=sys.stderr)
        return 2

    files = iter_py_files(path)
    if not files:
        print(f"В {path} нет .py-файлов", file=sys.stderr)
        return 2

    findings = scan_path(path)

    if args.update_baseline:
        write_baseline(BASELINE_PATH, findings)
        print(good(f"Baseline обновлён: {BASELINE_PATH} ({len(findings)} находок)"))
        return 0

    baseline_keys = load_baseline(BASELINE_PATH)
    new = new_findings(findings, baseline_keys)

    # --journal-rows: только готовые строки для механической дозаписи в #782.
    if args.journal_rows:
        rows = findings_to_journal_rows(new, run_date)
        if not rows:
            print(dim("Новых находок нет — строк для журнала #782 нет."), file=sys.stderr)
            return 0
        for row in rows:
            print(row)
        return 0

    # --create-issue: ручной запуск авто-создаёт GitHub issue с новыми находками.
    if args.create_issue:
        if not new:
            print(good("Новых велосипедов нет — issue не создаётся."))
            return 0
        try:
            url = maybe_create_issue_for_new(
                new, run_date, labels=args.issue_label, repo=args.repo
            )
        except (RuntimeError, OSError) as exc:
            # Advisory-инструмент: сбой gh (в т.ч. DNS) не должен валить прогон жёстко,
            # но и не «зелёный» — печатаем строки журнала как fallback и код 1.
            print(red(f"Не удалось создать issue: {exc}"), file=sys.stderr)
            print(warn("Строки для ручной дозаписи в журнал #782:"), file=sys.stderr)
            for row in findings_to_journal_rows(new, run_date):
                print(row)
            return 1
        print(good(f"Создан issue с {len(new)} новыми находками: {url}"))
        return 0

    if args.json:
        print(
            json.dumps(
                {
                    "path": str(path),
                    "total": len(findings),
                    "findings": findings_to_baseline(findings),
                    "new": findings_to_baseline(new),
                    "baseline_exists": baseline_keys is not None,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
    else:
        print(bold(f"Анализ: {path}  ({len(files)} файлов)"))
        print_report(findings, new, baseline_existed=baseline_keys is not None)
        header("Итог")
        if args.fail_on_new and baseline_keys is not None and new:
            print(red(f"Новых велосипедов: {len(new)} → провал (--fail-on-new)"))
        else:
            print(dim("Информационный прогон (exit 0). Для CI-гейта используйте --fail-on-new."))

    if args.fail_on_new and baseline_keys is not None and new:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
