"""Регресс-гард: warm_all_dialogs()/leave_channels() — НЕ самописный retry.

Детектор «изобретённого велосипеда» (`scripts/detect_reinvented.py`, эпик #1083)
помечает обе функции med-confidence находкой «самописный retry/backoff-цикл →
tenacity.retry / aiolimiter» (issue #1114). Это **false-positive** эвристики
`_detect_retry_loop`: она ловит «цикл + try/except + asyncio.sleep» и по своему
же docstring предупреждает про «легитимные poll-циклы».

Семантически оба цикла — НЕ повторная попытка одной операции с backoff:

* `warm_all_dialogs()` итерирует по РАЗНЫМ аккаунтам (`_connected_phones()`),
  греет кэш каждого ровно один раз; `asyncio.sleep(WARM_STAGGER_DELAY_SEC)` —
  фиксированный inter-account стаггер (rate-limit вежливость), а не backoff. На
  ошибке аккаунт пропускается — не повторяется. FloodWait делегирован
  `run_with_flood_wait` (single-shot, намеренно fail-fast — см. docstring).
* `leave_channels()` итерирует по РАЗНЫМ диалогам, покидает каждый один раз;
  `asyncio.sleep(0.3)` — стаггер на success-пути. На FloodWait — `break`
  (мутирующие leave-флоу НЕ авто-ретраятся, правило проекта). Тоже делегирует
  FloodWait в `run_with_flood_wait`.

Замена этих циклов на `tenacity.retry` была бы поведенческой ошибкой: tenacity
повторно вызывает ОДИН callable на ошибке, а здесь на ошибке нужно перейти к
СЛЕДУЮЩЕМУ элементу. Реальный retry/backoff в проекте уже централизован в
`src/telegram/flood_wait.py::run_with_flood_wait_retry`.

Гард фиксирует инвариант «делегирование, а не велосипед», чтобы будущий рефактор
не превратил эти методы в настоящий hand-rolled retry-цикл (тогда тест краснеет).
Решение по находке #1114 («оставить» как false-positive) — за владельцем (#782).
"""

from __future__ import annotations

import ast
import inspect
from pathlib import Path

import pytest

from src.telegram.pool_dialogs import DialogsMixin

# Централизованные FloodWait-абстракции проекта — единственный допустимый способ
# обработки backoff/retry в telegram-слое. Любой из них = «делегирование».
_FLOOD_WAIT_HELPERS = {"run_with_flood_wait", "run_with_flood_wait_retry"}


def _method_ast(name: str) -> ast.AsyncFunctionDef:
    """AST-узел метода миксина по имени (через inspect.getsource — точный исходник)."""
    func = getattr(DialogsMixin, name)
    source = inspect.getsource(func)
    # getsource сохраняет отступ метода класса — dedent для парсинга.
    node = ast.parse(_dedent(source)).body[0]
    assert isinstance(node, ast.AsyncFunctionDef), f"{name} должен быть async def"
    return node


def _dedent(source: str) -> str:
    import textwrap

    return textwrap.dedent(source)


def _called_names(node: ast.AST) -> set[str]:
    """Имена вызываемых функций (по `.id`/`.attr`) во всём поддереве."""
    names: set[str] = set()
    for call in ast.walk(node):
        if not isinstance(call, ast.Call):
            continue
        func = call.func
        if isinstance(func, ast.Name):
            names.add(func.id)
        elif isinstance(func, ast.Attribute):
            names.add(func.attr)
    return names


@pytest.mark.parametrize("method_name", ["warm_all_dialogs", "leave_channels"])
def test_method_delegates_floodwait_to_central_helper(method_name: str) -> None:
    """Метод обрабатывает FloodWait через централизованный helper, а не сам.

    Это и есть «не велосипед»: backoff/retry для Telegram единообразно живёт в
    flood_wait.py. Если метод перестанет звать run_with_flood_wait* (например
    кто-то впишет ручной `while attempt < N: ... sleep(backoff)`), гард краснеет.
    """
    node = _method_ast(method_name)
    called = _called_names(node)
    assert called & _FLOOD_WAIT_HELPERS, (
        f"{method_name}() обязан делегировать FloodWait в одну из "
        f"{_FLOOD_WAIT_HELPERS}; найдено вызовов: {sorted(called)}. "
        "Самописный retry-цикл здесь запрещён — см. модульный docstring и #1114."
    )


@pytest.mark.parametrize("method_name", ["warm_all_dialogs", "leave_channels"])
def test_method_has_no_nested_retry_loop(method_name: str) -> None:
    """Внутри основного цикла нет вложенного цикла со sleep — признака backoff.

    Самописный retry — это «цикл попыток ВОКРУГ одной операции» (вложенный
    `while`/`for` с `sleep` внутри тела внешнего цикла). Внешний цикл здесь
    итерирует по коллекции (аккаунты/диалоги) — это легитимно. Появление
    ВЛОЖЕННОГО цикла со sleep сигналит про hand-rolled backoff → краснеет.
    """
    node = _method_ast(method_name)
    loops = [n for n in ast.walk(node) if isinstance(n, (ast.For, ast.While))]
    assert loops, f"{method_name}() ожидаемо содержит цикл по коллекции"

    for outer in loops:
        for inner in ast.walk(outer):
            if inner is outer or not isinstance(inner, (ast.For, ast.While)):
                continue
            inner_sleeps = {
                name for name in _called_names(inner) if name == "sleep"
            }
            assert not inner_sleeps, (
                f"{method_name}(): вложенный цикл со sleep на строке "
                f"{inner.lineno} — это самописный retry/backoff. Делегируй в "
                "run_with_flood_wait* вместо ручного цикла попыток (#1114)."
            )


def test_handrolled_retry_loop_would_be_caught_by_detector() -> None:
    """Мутационная страховка: НАСТОЯЩИЙ самописный retry детектор обязан поймать.

    Доказывает, что инвариант не вакуумный: если бы метод реально содержал
    retry-велосипед (цикл попыток вокруг одной операции с backoff-sleep), его
    эвристика `_detect_retry_loop` пометила бы — то есть «red» достижим.
    """
    import importlib.util

    script = Path(__file__).resolve().parents[1] / "scripts" / "detect_reinvented.py"
    spec = importlib.util.spec_from_file_location("detect_reinvented_guard", script)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    import sys

    sys.modules["detect_reinvented_guard"] = module
    spec.loader.exec_module(module)

    handrolled_retry = '''
        import asyncio

        async def leave_channels(self, phone, dialogs):
            for cid, ctype in dialogs:
                attempt = 0
                while attempt < 3:
                    try:
                        await self._remove(cid)
                        break
                    except Exception:
                        attempt += 1
                        await asyncio.sleep(2 ** attempt)
    '''
    import textwrap

    findings = module.detect_in_source(textwrap.dedent(handrolled_retry), "mutated.py")
    assert "handrolled-retry-loop" in {f.kind for f in findings}, (
        "Эвристика обязана ловить настоящий retry-велосипед — иначе гард вакуумен."
    )
