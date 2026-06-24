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


def _finding(file: str, line: int, kind: str = "manual-counter"):  # type: ignore[no-untyped-def]
    return Finding(file=file, line=line, kind=kind, what="w", replacement="r", confidence="med")


def test_no_baseline_means_all_new() -> None:
    """Без baseline (None) все находки считаются новыми."""
    findings = [_finding("a.py", 1), _finding("b.py", 2)]
    new = detect_reinvented.new_findings(findings, None)
    assert len(new) == 2


def test_new_findings_diffs_against_baseline() -> None:
    """В baseline зафиксирован один ключ; новой считается только отсутствующая находка."""
    known = _finding("a.py", 1)
    fresh = _finding("b.py", 2)
    baseline_keys = {known.key()}
    new = detect_reinvented.new_findings([known, fresh], baseline_keys)
    assert [f.key() for f in new] == [fresh.key()]


def test_baseline_roundtrip(tmp_path: Path) -> None:
    """write_baseline → load_baseline восстанавливает множество ключей находок."""
    findings = [_finding("a.py", 1, "manual-counter"), _finding("b.py", 9, "handrolled-retry-loop")]
    snapshot = tmp_path / "baseline.json"
    detect_reinvented.write_baseline(snapshot, findings)
    keys = detect_reinvented.load_baseline(snapshot)
    assert keys == {f.key() for f in findings}


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
