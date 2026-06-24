"""Тесты для scripts/detect_reinvented.py — детектора «изобретённого велосипеда» (#1108).

Чистая логика эвристик проверяется на мини-фикстурах «велосипед vs идиоматичный
код» (по образцу того, как tests/test_doc_coverage.py тестирует обёртку над
interrogate на фейках формы, а не на реальном инструменте). Каждая эвристика
имеет:
  * положительную фикстуру — самописный код, который ДОЛЖЕН поймать детектор;
  * отрицательную — идиоматичный / уже-на-stdlib код, который НЕ должен давать
    false-positive.

Верификационный кейс эпика (#1083: «детектор находит ≥1 известный кейс из #782»)
— hand-rolled HMAC-JWT из src/web/session.py (#782 / пункт #1). Эталонная
функция собирала `base64url(HMAC-SHA256(secret, payload))` руками; детектор
обязан её поймать. При этом ТЕКУЩИЙ session.py уже переехал на itsdangerous.Signer
(#953) — и на нём детектор молчать НЕ обязан по этой эвристике, что тоже
зафиксировано отдельным тестом (отрицательный по handrolled-signing).
"""

from __future__ import annotations

import importlib.util
import re
import sys
import textwrap
from pathlib import Path

import pytest

# scripts/ — не пакет; грузим модуль по пути, как tests/test_doc_coverage.py.
# Модуль регистрируется в sys.modules ДО exec_module: его @dataclass со
# строковыми аннотациями (from __future__ import annotations) обращается к
# sys.modules[cls.__module__] при разборе типов — без регистрации падает с
# AttributeError на NoneType.
_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "detect_reinvented.py"
_spec = importlib.util.spec_from_file_location("detect_reinvented", _SCRIPT)
assert _spec and _spec.loader
detect_reinvented = importlib.util.module_from_spec(_spec)
sys.modules["detect_reinvented"] = detect_reinvented
_spec.loader.exec_module(detect_reinvented)

detect_in_source = detect_reinvented.detect_in_source
Finding = detect_reinvented.Finding


def _kinds(source: str) -> set[str]:
    return {f.kind for f in detect_in_source(textwrap.dedent(source), "fixture.py")}


# --------------------------------------------------------------------------- #
# Верификационный кейс эпика: hand-rolled HMAC-JWT (#782 / session.py #1)
# --------------------------------------------------------------------------- #

# Исторический паттерн session.py ДО рефакторинга на itsdangerous (#953):
# своя HMAC-SHA256 подпись поверх base64url(json-payload). Это и есть эталон,
# который детектор обязан поймать (verification «≥1 известный кейс из #782»).
HANDROLLED_HMAC_JWT = """
    import base64
    import hashlib
    import hmac
    import json
    import time

    def create_session_token(username, secret, ttl=3600):
        payload = json.dumps({"user": username, "exp": int(time.time()) + ttl})
        payload_b64 = base64.urlsafe_b64encode(payload.encode()).rstrip(b"=").decode()
        sig = hmac.new(secret.encode(), payload_b64.encode(), hashlib.sha256).digest()
        sig_b64 = base64.urlsafe_b64encode(sig).rstrip(b"=").decode()
        return f"{payload_b64}.{sig_b64}"
"""

# Текущий session.py: подпись делегирована itsdangerous.Signer — никакого
# hmac.new/собственной крипты в теле. По эвристике handrolled-signing молчим.
IDIOMATIC_ITSDANGEROUS = """
    import base64
    import hashlib
    import json
    import time

    from itsdangerous import Signer

    def create_session_token(username, secret, ttl=3600):
        payload = json.dumps({"user": username, "exp": int(time.time()) + ttl})
        payload_b64 = base64.urlsafe_b64encode(payload.encode()).rstrip(b"=").decode()
        return Signer(secret, sep=".", digest_method=hashlib.sha256).sign(payload_b64).decode()
"""


def test_handrolled_hmac_jwt_is_caught() -> None:
    """Verification-кейс #1083: эталонный hand-rolled HMAC-JWT (#782) ловится, high."""
    findings = detect_in_source(textwrap.dedent(HANDROLLED_HMAC_JWT), "src/web/session.py")
    signing = [f for f in findings if f.kind == "handrolled-token-signing"]
    assert len(signing) == 1, "детектор обязан поймать самописную HMAC-JWT подпись (#782 эталон)"
    assert signing[0].confidence == "high"
    assert "session.py" in signing[0].file
    assert "itsdangerous" in signing[0].replacement


def test_itsdangerous_signing_not_flagged() -> None:
    """Идиоматичная подпись через itsdangerous.Signer НЕ даёт handrolled-находки."""
    assert "handrolled-token-signing" not in _kinds(IDIOMATIC_ITSDANGEROUS)


# --------------------------------------------------------------------------- #
# Эвристика: самописный retry/backoff (#782 контекст; есть tenacity/aiolimiter)
# --------------------------------------------------------------------------- #


def test_retry_loop_with_sleep_is_caught() -> None:
    """Цикл с try/except + sleep — самописный retry, должен ловиться (med)."""
    src = """
        import time

        def fetch(url, retries=3):
            for attempt in range(retries):
                try:
                    return do_request(url)
                except ConnectionError:
                    time.sleep(2 ** attempt)
            raise RuntimeError("failed")
    """
    findings = [f for f in detect_in_source(textwrap.dedent(src), "f.py") if f.kind == "handrolled-retry-loop"]
    assert len(findings) == 1
    assert findings[0].confidence == "med"
    assert "tenacity" in findings[0].replacement


def test_async_retry_loop_is_caught() -> None:
    """async-вариант: while + try/except + asyncio.sleep тоже ловится."""
    src = """
        import asyncio

        async def fetch(url):
            attempt = 0
            while attempt < 5:
                try:
                    return await do_request(url)
                except TimeoutError:
                    await asyncio.sleep(1)
                attempt += 1
    """
    assert "handrolled-retry-loop" in _kinds(src)


def test_nested_function_retry_deduped() -> None:
    """Вложенная функция и её обёртка видят один цикл через ast.walk → одна находка.

    Без дедупа по (файл, тип, строка) один и тот же retry-цикл вышел бы дважды с
    именами обеих функций (внешней и вложенной).
    """
    src = """
        import asyncio

        async def outer():
            async def inner():
                while True:
                    try:
                        return await work()
                    except TimeoutError:
                        await asyncio.sleep(1)
            return await inner()
    """
    retries = [f for f in detect_in_source(textwrap.dedent(src), "f.py") if f.kind == "handrolled-retry-loop"]
    assert len(retries) == 1, "один цикл — одна находка, дубли по вложенности дедуплицированы"


def test_plain_loop_without_try_is_not_flagged() -> None:
    """Обычный цикл без try/except (и без sleep) — не retry, не находка."""
    src = """
        def total(items):
            acc = 0
            for x in items:
                acc += x
            return acc
    """
    assert "handrolled-retry-loop" not in _kinds(src)


def test_loop_with_sleep_but_no_try_is_not_flagged() -> None:
    """Poll-цикл со sleep, но БЕЗ try/except — не считаем retry (нет перехвата)."""
    src = """
        import time

        def wait_ready(check):
            while not check():
                time.sleep(1)
    """
    assert "handrolled-retry-loop" not in _kinds(src)


# --------------------------------------------------------------------------- #
# Эвристика: самописный частотный счётчик (есть collections.Counter)
# --------------------------------------------------------------------------- #


def test_manual_counter_is_caught() -> None:
    """`d[k] = d.get(k, 0) + 1` — велосипед поверх Counter, ловится (med)."""
    src = """
        def histogram(items):
            counts = {}
            for x in items:
                counts[x] = counts.get(x, 0) + 1
            return counts
    """
    findings = [f for f in detect_in_source(textwrap.dedent(src), "f.py") if f.kind == "manual-counter"]
    assert len(findings) == 1
    assert "Counter" in findings[0].replacement


def test_manual_counter_subscript_increment_is_caught() -> None:
    """Вариант `d[k] = d[k] + 1` (через subscript, не .get) тоже ловится."""
    src = """
        def histogram(items, counts):
            for x in items:
                counts[x] = counts[x] + 1
            return counts
    """
    assert "manual-counter" in _kinds(src)


def test_collections_counter_not_flagged() -> None:
    """Идиоматичный Counter — никаких manual-counter находок."""
    src = """
        from collections import Counter

        def histogram(items):
            return Counter(items)
    """
    assert "manual-counter" not in _kinds(src)


def test_augmented_assign_to_var_not_flagged() -> None:
    """`acc = acc + 1` по обычной переменной (не subscript) — не счётчик-словарь."""
    src = """
        def count_positive(items):
            acc = 0
            for x in items:
                if x > 0:
                    acc = acc + 1
            return acc
    """
    assert "manual-counter" not in _kinds(src)


# --------------------------------------------------------------------------- #
# Эвристика: ручной парсинг email (есть email.utils)
# --------------------------------------------------------------------------- #


def test_manual_email_split_is_caught() -> None:
    """`addr.split('@')` — ручной разбор email, ловится (low)."""
    src = """
        def domain_of(addr):
            return addr.split("@")[1]
    """
    findings = [f for f in detect_in_source(textwrap.dedent(src), "f.py") if f.kind == "manual-email-parse"]
    assert len(findings) == 1
    assert findings[0].confidence == "low"
    assert "email.utils" in findings[0].replacement


def test_split_on_other_sep_not_flagged() -> None:
    """split по другому разделителю (не '@') — не email-эвристика."""
    src = """
        def first_csv(line):
            return line.split(",")[0]
    """
    assert "manual-email-parse" not in _kinds(src)


def test_url_split_intentionally_not_flagged() -> None:
    """Регресс (Codex #1110): разбор URL через split('/')/'?'/'#' НЕ ловится.

    Эти разделители слишком частые вне URL — эвристика на них была бы сплошным
    false-positive. Решение намеренное; детектор не заявляет URL-покрытие.
    """
    src = """
        def path_of(url):
            return url.split("/")[2], url.split("?")[0], url.split("#")[0]
    """
    assert _kinds(src) == set(), "URL-split не должен давать никаких находок"


# --------------------------------------------------------------------------- #
# Эвристика: ручная добивка base64-паддинга
# --------------------------------------------------------------------------- #


def test_manual_base64_padding_is_caught() -> None:
    """`s += '=' * padding` рядом с b64decode — ручной паддинг, ловится (med)."""
    src = """
        import base64

        def b64url_decode(s):
            padding = 4 - len(s) % 4
            if padding != 4:
                s += "=" * padding
            return base64.urlsafe_b64decode(s)
    """
    findings = [f for f in detect_in_source(textwrap.dedent(src), "f.py") if f.kind == "manual-base64-padding"]
    assert len(findings) == 1
    assert findings[0].confidence == "med"


def test_string_multiply_unrelated_not_flagged() -> None:
    """`'-' * width` (рисование разделителя) — не base64-паддинг."""
    src = """
        def rule(width):
            return "-" * width
    """
    assert "manual-base64-padding" not in _kinds(src)


def test_equals_multiply_without_b64_not_flagged() -> None:
    """Регресс (ревью #1110): `print('=' * 80)` без b64-вызова рядом — НЕ паддинг.

    Без b64-контекста `'=' * n` — обычный разделитель; флагить его = low-шум.
    """
    src = """
        def banner():
            print("=" * 80)
            print("REPORT")
            print("=" * 80)
    """
    assert "manual-base64-padding" not in _kinds(src)


# --------------------------------------------------------------------------- #
# Свойства модели / устойчивость
# --------------------------------------------------------------------------- #


def test_syntax_error_yields_no_findings() -> None:
    """Битый Python не роняет детектор (advisory): пустой список."""
    assert detect_in_source("def broken(:\n    pass", "bad.py") == []


def test_findings_are_sorted_deterministically() -> None:
    """Порядок находок стабилен (по файлу/строке) — для воспроизводимого baseline."""
    src = textwrap.dedent(HANDROLLED_HMAC_JWT)
    first = detect_in_source(src, "a.py")
    second = detect_in_source(src, "a.py")
    assert [f.key() for f in first] == [f.key() for f in second]


def test_human_format_has_all_fields() -> None:
    """Человекочитаемая строка содержит файл:строку, что, замену, уверенность."""
    f = Finding(
        file="src/x.py", line=42, kind="manual-counter",
        what="самописный счётчик", replacement="collections.Counter", confidence="med",
    )
    human = f.human()
    assert "src/x.py:42" in human
    assert "самописный счётчик" in human
    assert "collections.Counter" in human
    assert "med" in human


# --------------------------------------------------------------------------- #
# Baseline-логика (дифф НОВЫХ велосипедов)
# --------------------------------------------------------------------------- #


def _finding(file: str, line: int, kind: str = "manual-counter", what: str = "w"):  # type: ignore[no-untyped-def]
    return Finding(file=file, line=line, kind=kind, what=what, replacement="r", confidence="med")


def test_no_baseline_means_all_new() -> None:
    """Без baseline (None) все находки считаются новыми."""
    findings = [_finding("a.py", 1, what="x"), _finding("b.py", 2, what="y")]
    new = detect_reinvented.new_findings(findings, None)
    assert len(new) == 2


def test_new_findings_diffs_against_baseline() -> None:
    """В baseline зафиксирован один identity; новой считается только отсутствующая находка."""
    known = _finding("a.py", 1, what="known")
    fresh = _finding("b.py", 2, what="fresh")
    baseline_ids = {known.identity()}
    new = detect_reinvented.new_findings([known, fresh], baseline_ids)
    assert [f.identity() for f in new] == [fresh.identity()]


def test_new_findings_stable_across_line_shift() -> None:
    """Регресс (Codex #1110): сдвиг строки НЕ делает неизменную находку «новой».

    Baseline зафиксирован по identity (file/kind/what). Та же находка, всплывшая
    на другой строке после вставки кода выше, не считается новым велосипедом —
    иначе --fail-on-new шумел бы после любого рефакторинга.
    """
    original = _finding("a.py", line=10, what="самописный счётчик в f()")
    shifted = _finding("a.py", line=42, what="самописный счётчик в f()")  # та же находка, +строки выше
    baseline_ids = {original.identity()}
    assert detect_reinvented.new_findings([shifted], baseline_ids) == []


def test_baseline_roundtrip(tmp_path: Path) -> None:
    """write_baseline → load_baseline восстанавливает множество identity-отпечатков."""
    findings = [
        _finding("a.py", 1, "manual-counter", what="счётчик в a()"),
        _finding("b.py", 9, "handrolled-retry-loop", what="retry в b()"),
    ]
    snapshot = tmp_path / "baseline.json"
    detect_reinvented.write_baseline(snapshot, findings)
    ids = detect_reinvented.load_baseline(snapshot)
    assert ids == {f.identity() for f in findings}


def test_load_baseline_missing_returns_none(tmp_path: Path) -> None:
    """Отсутствующий снимок → None (а не пустое множество): «baseline ещё нет»."""
    assert detect_reinvented.load_baseline(tmp_path / "nope.json") is None


# --------------------------------------------------------------------------- #
# Прогон по реальному src/web/session.py: текущий код НЕ должен давать
# handrolled-signing (он на itsdangerous). Защищает от регресса эвристики.
# --------------------------------------------------------------------------- #


def test_real_session_py_not_flagged_as_handrolled() -> None:
    """Текущий src/web/session.py (itsdangerous) не ловится как hand-rolled signing."""
    session_py = Path(__file__).resolve().parents[1] / "src" / "web" / "session.py"
    if not session_py.exists():  # pragma: no cover - файл есть в репозитории
        pytest.skip("src/web/session.py отсутствует")
    findings = detect_in_source(session_py.read_text(encoding="utf-8"), "src/web/session.py")
    assert "handrolled-token-signing" not in {f.kind for f in findings}


# --------------------------------------------------------------------------- #
# Формат журнала #782: дозапись находок строками таблицы реестра (#1109)
#
# Реальная шапка журнала в issue #782:
#   | Дата | Находка (файл) | Стандартная замена | Решение | PR/issue |
# Детектор готовит строки в ровно этом формате (Решение = _ожидает решения_,
# PR/issue = —), владелец механически вставляет их в журнал. Детектор НЕ пишет
# в #782 сам — только формирует строки.
# --------------------------------------------------------------------------- #


def test_journal_row_matches_782_columns() -> None:
    """Строка журнала #782: 5 колонок, дата, находка(файл:строка), замена, заглушки."""
    f = _finding(
        "src/web/session.py", line=12, kind="handrolled-token-signing",
        what="ручная HMAC/JWT-подпись токена в create_session_token()",
    )
    row = detect_reinvented.journal_row(f, date="2026-06-24")
    # Ровно 5 значимых ячеек между ведущим и хвостовым '|'.
    cells = [c.strip() for c in row.strip().strip("|").split("|")]
    assert len(cells) == 5
    date, finding_cell, replacement_cell, decision_cell, pr_cell = cells
    assert date == "2026-06-24"
    assert "create_session_token" in finding_cell
    assert "src/web/session.py:12" in finding_cell  # файл:строка внутри находки
    assert replacement_cell == "r"
    assert decision_cell == "_ожидает решения_"
    assert pr_cell == "—"


def test_journal_rows_one_per_finding_and_deterministic() -> None:
    """findings_to_journal_rows: по строке на находку, порядок стабилен."""
    findings = [
        _finding("a.py", 1, what="счётчик в a()"),
        _finding("b.py", 2, what="retry в b()"),
    ]
    rows = detect_reinvented.findings_to_journal_rows(findings, date="2026-06-24")
    assert len(rows) == 2
    assert rows == detect_reinvented.findings_to_journal_rows(findings, date="2026-06-24")


def test_journal_row_escapes_pipe_in_text() -> None:
    """Вертикальная черта в тексте находки экранируется, чтобы не ломать таблицу."""
    f = _finding("a.py", 1, what="разбор a|b через split")
    row = detect_reinvented.journal_row(f, date="2026-06-24")
    # Сырой '|' внутри ячейки экранирован (\|), поэтому не плодит колонку.
    assert "a\\|b" in row
    # Сплит по НЕэкранированному разделителю даёт ровно 5 значимых ячеек.
    cells = [c.strip() for c in re.split(r"(?<!\\)\|", row.strip().strip("|"))]
    assert len(cells) == 5


def test_journal_row_escapes_pipe_in_date() -> None:
    """Регресс (ревью #1113): '|' в дате тоже экранируется — иначе ломает таблицу.

    `--date` приходит от оператора без валидации; если в нём окажется '|',
    ячейка даты не должна плодить лишнюю колонку (как и все прочие ячейки).
    """
    f = _finding("a.py", 1, what="счётчик")
    row = detect_reinvented.journal_row(f, date="бяка | injected")
    cells = [c.strip() for c in re.split(r"(?<!\\)\|", row.strip().strip("|"))]
    assert len(cells) == 5


# --------------------------------------------------------------------------- #
# Тело и заголовок авто-создаваемого GitHub issue с находками (#1109)
# --------------------------------------------------------------------------- #


def test_issue_body_contains_findings_table_and_journal_rows() -> None:
    """Тело issue: человекочитаемая таблица находок + готовые строки журнала #782."""
    findings = [
        _finding("src/x.py", 10, kind="manual-counter", what="самописный счётчик в f()"),
    ]
    body = detect_reinvented.build_issue_body(findings, date="2026-06-24")
    # Человекочитаемая таблица находок (5 колонок отчёта).
    assert "Уверенность" in body
    assert "src/x.py:10" in body
    assert "самописный счётчик в f()" in body
    # Готовая строка для дозаписи в журнал #782.
    assert "_ожидает решения_" in body


def test_issue_body_mentions_parent_epics() -> None:
    """Тело issue упоминает зонтичный эпик #1083 и журнал-реестр #782 (связь)."""
    body = detect_reinvented.build_issue_body([_finding("a.py", 1)], date="2026-06-24")
    assert "#1083" in body
    assert "#782" in body


def test_issue_title_summarizes_count_and_date() -> None:
    """Заголовок issue содержит число новых находок и дату прогона."""
    title = detect_reinvented.build_issue_title([_finding("a.py", 1), _finding("b.py", 2)], date="2026-06-24")
    assert "2" in title
    assert "2026-06-24" in title


def test_issue_body_empty_findings_raises() -> None:
    """Регресс (ревью #1113): пустой список → ValueError, а не header-only таблицы."""
    with pytest.raises(ValueError, match="пустой список"):
        detect_reinvented.build_issue_body([], date="2026-06-24")


def test_issue_body_tables_have_no_blank_rows() -> None:
    """Регресс-гард: внутри markdown-таблиц нет пустых строк (иначе GitHub их рвёт).

    Пустая строка между рядами таблицы прерывает её рендеринг в GitHub. Каждый
    непустой ряд таблицы (строка, начинающаяся с '|') должен соседствовать с
    другим рядом без разрыва — проверяем, что между двумя соседними '|'-рядами
    одной таблицы нет пустой строки.
    """
    findings = [_finding("a.py", 1, what="f1"), _finding("b.py", 2, what="f2")]
    body = detect_reinvented.build_issue_body(findings, date="2026-06-24")
    lines = body.split("\n")
    for i in range(1, len(lines)):
        prev, cur = lines[i - 1], lines[i]
        # Если предыдущая и следующая непустые строки обе ряды таблицы — между
        # ними не должно быть пустой строки (тут они соседи, проверяем оба ряда).
        if prev.startswith("|") and cur.strip() == "":
            nxt = lines[i + 1] if i + 1 < len(lines) else ""
            assert not nxt.startswith("|"), f"пустая строка внутри таблицы у ряда: {prev!r}"


# --------------------------------------------------------------------------- #
# Создание issue через gh — раннер инъектируется, реальные issue в тестах НЕ
# плодятся (gh мокается фейковым раннером, как сетевые вызовы в проекте).
# --------------------------------------------------------------------------- #


def test_create_github_issue_invokes_gh_and_returns_url() -> None:
    """create_github_issue вызывает `gh issue create` и возвращает URL из stdout."""
    calls: list[list[str]] = []

    class _Result:
        returncode = 0
        stdout = "https://github.com/axisrow/tg_content_factory/issues/1234\n"
        stderr = ""

    def fake_runner(cmd: list[str], **_kwargs: object) -> "_Result":
        calls.append(cmd)
        return _Result()

    url = detect_reinvented.create_github_issue(
        title="t", body="b", labels=["priority/medium"], repo="owner/repo", runner=fake_runner
    )
    assert url == "https://github.com/axisrow/tg_content_factory/issues/1234"
    assert len(calls) == 1
    cmd = calls[0]
    assert cmd[:3] == ["gh", "issue", "create"]
    assert "--title" in cmd and "t" in cmd
    assert "--body" in cmd and "b" in cmd
    assert "--label" in cmd and "priority/medium" in cmd
    # --repo прокидывается в argv (ревью #1113: ветка repo была непокрыта).
    assert "--repo" in cmd and "owner/repo" in cmd


def test_create_github_issue_raises_on_failure() -> None:
    """Ненулевой код gh → RuntimeError со stderr (не молчаливое проглатывание)."""

    class _Result:
        returncode = 1
        stdout = ""
        stderr = "could not resolve host"

    def failing_runner(_cmd: list[str], **_kwargs: object) -> "_Result":
        return _Result()

    with pytest.raises(RuntimeError, match="could not resolve host"):
        detect_reinvented.create_github_issue(title="t", body="b", runner=failing_runner)


def test_create_github_issue_no_findings_returns_none() -> None:
    """maybe_create_issue_for_new: нет новых находок → issue не создаётся (None)."""
    called = False

    def runner(_cmd: list[str], **_kwargs: object) -> object:  # pragma: no cover - не вызывается
        nonlocal called
        called = True
        raise AssertionError("раннер не должен вызываться без новых находок")

    result = detect_reinvented.maybe_create_issue_for_new([], date="2026-06-24", runner=runner)
    assert result is None
    assert called is False
