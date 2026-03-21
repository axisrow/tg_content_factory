from __future__ import annotations

import argparse
import asyncio
import logging
import os
import shlex
import shutil
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path

from src.cli import runtime
from src.database import Database
from src.filters.analyzer import ChannelAnalyzer
from src.models import Message, SearchQuery
from src.telegram.backends import adapt_transport_session
from src.telegram.flood_wait import FloodWaitInfo, HandledFloodWaitError, run_with_flood_wait

logger = logging.getLogger(__name__)

TELEGRAM_TIMEOUT = 30
TELEGRAM_DIALOG_TIMEOUT = 120
TELEGRAM_SEARCH_TIMEOUT = 120
SHORT_FLOOD_WAIT_RETRY_SEC = 30
_TG_CHECKS_AFTER_POOL = [
    "tg_users_info",
    "tg_get_dialogs",
    "tg_resolve_channel",
    "tg_warm_dialog_cache",
    "tg_iter_messages",
    "tg_channel_stats",
    "tg_search_my_chats",
    "tg_search_in_channel",
    "tg_search_premium",
    "tg_search_quota",
]
# src/cli/commands/test.py → src/cli/commands → src/cli → src → repo root
REPO_ROOT = Path(__file__).resolve().parents[3]
SERIAL_PYTEST_COMMAND = (sys.executable, "-m", "pytest", "tests", "-q")
PARALLEL_SAFE_PYTEST_COMMAND = (
    sys.executable,
    "-m",
    "pytest",
    "tests",
    "-q",
    "-m",
    "not aiosqlite_serial",
    "-n",
    "auto",
)
AIOSQLITE_SERIAL_PYTEST_COMMAND = (
    sys.executable,
    "-m",
    "pytest",
    "tests",
    "-q",
    "-m",
    "aiosqlite_serial",
)


class Status(Enum):
    PASS = "PASS"
    FAIL = "FAIL"
    SKIP = "SKIP"


@dataclass
class CheckResult:
    name: str
    status: Status
    detail: str


@dataclass(frozen=True)
class BenchmarkStep:
    name: str
    command: tuple[str, ...]


@dataclass(frozen=True)
class TelegramLiveFloodDecision:
    action: str
    detail: str
    retry_after_sec: int | None = None
    next_available_at_utc: datetime | None = None


class TelegramLiveStepSkipError(RuntimeError):
    """Internal control-flow exception used to stop live checks on long flood waits."""


def _print_result(result: CheckResult) -> None:
    tag = {
        Status.PASS: "\033[32m[PASS]\033[0m",
        Status.FAIL: "\033[31m[FAIL]\033[0m",
        Status.SKIP: "\033[33m[SKIP]\033[0m",
    }[result.status]
    print(f"{tag} {result.name:<22} {result.detail}")


def _run_benchmark_step(step: BenchmarkStep) -> float:
    print(f"\n=== {step.name} ===")
    print(f"$ {shlex.join(step.command)}")
    started = time.perf_counter()
    completed = subprocess.run(step.command, cwd=REPO_ROOT, check=False)
    elapsed = time.perf_counter() - started
    if completed.returncode != 0:
        print(f"\nBenchmark step failed: {step.name} exited with code {completed.returncode}")
        raise SystemExit(completed.returncode)
    print(f"Completed in {elapsed:.2f}s")
    return elapsed


def _run_pytest_benchmark() -> None:
    serial_elapsed = _run_benchmark_step(
        BenchmarkStep("serial_full_suite", SERIAL_PYTEST_COMMAND),
    )
    parallel_safe_elapsed = _run_benchmark_step(
        BenchmarkStep("parallel_safe_suite", PARALLEL_SAFE_PYTEST_COMMAND),
    )
    aiosqlite_serial_elapsed = _run_benchmark_step(
        BenchmarkStep("aiosqlite_serial_suite", AIOSQLITE_SERIAL_PYTEST_COMMAND),
    )

    # Both passes run sequentially; speedup comes from xdist within the first pass.
    two_pass_total = parallel_safe_elapsed + aiosqlite_serial_elapsed
    speedup = serial_elapsed / two_pass_total if two_pass_total else float("inf")

    print("\n--- Benchmark Summary ---")
    print(f"serial_full_suite: {serial_elapsed:.2f}s")
    print(f"parallel_safe_suite: {parallel_safe_elapsed:.2f}s")
    print(f"aiosqlite_serial_suite: {aiosqlite_serial_elapsed:.2f}s")
    print(f"two_pass_total: {two_pass_total:.2f}s")
    print(f"speedup_vs_serial: {speedup:.2f}x")


def _format_exception(exc: BaseException) -> str:
    detail = str(exc).strip()
    return detail or exc.__class__.__name__


async def _disable_flood_auto_sleep(pool) -> None:
    clients = getattr(pool, "clients", None) or {}
    for phone, client in clients.items():
        session = adapt_transport_session(client, disconnect_on_close=False)
        raw_client = getattr(session, "raw_client", None)
        if raw_client is None or not hasattr(raw_client, "flood_sleep_threshold"):
            continue
        raw_client.flood_sleep_threshold = 0
        logger.info(
            "Telegram live checks: disabled flood auto-sleep for %s; flood waits will surface immediately",
            phone,
        )


def _format_all_flooded_detail(
    base_detail: str,
    *,
    retry_after_sec: int | None,
    next_available_at_utc: datetime | None,
) -> str:
    if retry_after_sec is None:
        return f"{base_detail}; all clients are flood-waited"
    if next_available_at_utc is None:
        return f"{base_detail}; all clients are flood-waited, retry after about {retry_after_sec}s"
    return (
        f"{base_detail}; all clients are flood-waited, retry after about {retry_after_sec}s "
        f"until {next_available_at_utc.isoformat()}"
    )


def _is_premium_flood(info: FloodWaitInfo) -> bool:
    return info.operation in {
        "check_search_quota",
        "search_telegram_check_quota",
        "search_telegram",
    }


async def _get_live_flood_availability(pool, info: FloodWaitInfo):
    if _is_premium_flood(info):
        availability_getter = getattr(pool, "get_premium_stats_availability", None)
        if callable(availability_getter):
            return await availability_getter()
    availability_getter = getattr(pool, "get_stats_availability", None)
    if callable(availability_getter):
        return await availability_getter()
    return None


async def _decide_live_test_flood_action(
    pool,
    info: FloodWaitInfo,
) -> TelegramLiveFloodDecision:
    availability = await _get_live_flood_availability(pool, info)
    if availability is None:
        return TelegramLiveFloodDecision(action="skip", detail=info.detail)
    if availability.state != "all_flooded":
        return TelegramLiveFloodDecision(action="rotate", detail=info.detail)

    detail = _format_all_flooded_detail(
        info.detail,
        retry_after_sec=availability.retry_after_sec,
        next_available_at_utc=availability.next_available_at_utc,
    )
    if (
        availability.retry_after_sec is not None
        and availability.retry_after_sec <= SHORT_FLOOD_WAIT_RETRY_SEC
    ):
        return TelegramLiveFloodDecision(
            action="wait_retry",
            detail=detail,
            retry_after_sec=availability.retry_after_sec,
            next_available_at_utc=availability.next_available_at_utc,
        )
    return TelegramLiveFloodDecision(
        action="skip",
        detail=detail,
        retry_after_sec=availability.retry_after_sec,
        next_available_at_utc=availability.next_available_at_utc,
    )


async def _handle_live_flood_wait(pool, check_name: str, info: FloodWaitInfo) -> None:
    decision = await _decide_live_test_flood_action(pool, info)
    if decision.action == "rotate":
        logger.info(
            "%s: %s flooded for %ss; retrying with another available account",
            check_name,
            info.phone or "<unknown>",
            info.wait_seconds,
        )
        return
    if decision.action == "wait_retry" and decision.retry_after_sec is not None:
        until = (
            decision.next_available_at_utc.isoformat()
            if decision.next_available_at_utc is not None
            else "unknown"
        )
        logger.info(
            "%s: all clients flood-waited; waiting %ss until %s before retry",
            check_name,
            decision.retry_after_sec,
            until,
        )
        await asyncio.sleep(decision.retry_after_sec + 1)
        return
    logger.warning("Skipping %s: %s", check_name, decision.detail)
    raise TelegramLiveStepSkipError(decision.detail)


def _get_search_result_flood_wait(result) -> FloodWaitInfo | None:
    flood_wait = getattr(result, "flood_wait", None)
    return flood_wait if isinstance(flood_wait, FloodWaitInfo) else None


# ---------------------------------------------------------------------------
# Read checks
# ---------------------------------------------------------------------------


async def _check_get_stats(db) -> CheckResult:
    try:
        stats = await db.get_stats()
        parts = ", ".join(f"{k}={v}" for k, v in stats.items())
        return CheckResult("get_stats", Status.PASS, parts)
    except Exception as exc:
        return CheckResult("get_stats", Status.FAIL, str(exc))


async def _check_account_list(db) -> CheckResult:
    try:
        accounts = await db.get_accounts()
        return CheckResult("account_list", Status.PASS, f"{len(accounts)} accounts")
    except Exception as exc:
        return CheckResult("account_list", Status.FAIL, str(exc))


async def _check_channel_list(db) -> CheckResult:
    try:
        channels = await db.get_channels_with_counts()
        return CheckResult("channel_list", Status.PASS, f"{len(channels)} channels")
    except Exception as exc:
        return CheckResult("channel_list", Status.FAIL, str(exc))


async def _check_notification_queries(db) -> CheckResult:
    try:
        queries = await db.get_notification_queries(active_only=False)
        if not queries:
            return CheckResult("notification_queries", Status.SKIP, "No notification queries")
        return CheckResult("notification_queries", Status.PASS, f"{len(queries)} queries")
    except Exception as exc:
        return CheckResult("notification_queries", Status.FAIL, str(exc))


async def _check_local_search(db) -> CheckResult:
    try:
        messages, total = await db.search_messages("test", limit=5)
        return CheckResult("local_search", Status.PASS, f"Query OK ({total} results)")
    except Exception as exc:
        return CheckResult("local_search", Status.FAIL, str(exc))


async def _check_collection_tasks(db) -> CheckResult:
    try:
        tasks = await db.get_collection_tasks(limit=5)
        return CheckResult("collection_tasks", Status.PASS, f"{len(tasks)} tasks")
    except Exception as exc:
        return CheckResult("collection_tasks", Status.FAIL, str(exc))


async def _check_recent_searches(db) -> CheckResult:
    try:
        searches = await db.get_recent_searches(limit=5)
        if not searches:
            return CheckResult("recent_searches", Status.SKIP, "No search history")
        return CheckResult("recent_searches", Status.PASS, f"{len(searches)} entries")
    except Exception as exc:
        return CheckResult("recent_searches", Status.FAIL, str(exc))


async def _check_pipeline_list(db: Database) -> CheckResult:
    try:
        pipelines = await db.repos.content_pipelines.get_all()
        return CheckResult("pipeline_list", Status.PASS, f"{len(pipelines)} pipelines")
    except Exception as exc:
        return CheckResult("pipeline_list", Status.FAIL, str(exc))


async def _check_notification_bot(db: Database) -> CheckResult:
    try:
        cur = await db.repos.notification_bots._db.execute("SELECT COUNT(*) FROM notification_bots")
        row = await cur.fetchone()
        count = row[0] if row else 0
        detail = f"{count} configured" if count else "none configured"
        return CheckResult("notification_bot", Status.PASS, detail)
    except Exception as exc:
        return CheckResult("notification_bot", Status.FAIL, str(exc))


async def _check_photo_tasks(db: Database) -> CheckResult:
    try:
        batches = await db.repos.photo_loader.list_batches()
        return CheckResult("photo_tasks", Status.PASS, f"{len(batches)} batches")
    except Exception as exc:
        return CheckResult("photo_tasks", Status.FAIL, str(exc))


# ---------------------------------------------------------------------------
# Write checks (operate on a temporary copy of the DB)
# ---------------------------------------------------------------------------


async def _init_db_copy(config_path: str) -> tuple[Database, str, object]:
    """Copy live DB to a temp file, return (copy_db, tmp_path, config)."""
    config, live_db = await runtime.init_db(config_path)
    live_path = live_db._db_path
    encryption_secret = live_db._session_encryption_secret
    await live_db.close()

    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    tmp.close()
    try:
        shutil.copy2(live_path, tmp.name)
        copy_db = Database(tmp.name, session_encryption_secret=encryption_secret)
        await copy_db.initialize()
        return copy_db, tmp.name, config
    except Exception:
        if os.path.exists(tmp.name):
            os.unlink(tmp.name)
        raise


async def _run_write_checks(config_path: str) -> list[CheckResult]:
    results: list[CheckResult] = []
    tmp_path: str | None = None
    copy_db: Database | None = None

    # 1. write_db_copy
    try:
        copy_db, tmp_path, _ = await _init_db_copy(config_path)
        stats = await copy_db.get_stats()
        parts = ", ".join(f"{k}={v}" for k, v in stats.items())
        results.append(
            CheckResult("write_db_copy", Status.PASS, f"Copied to {tmp_path} ({parts})"),
        )
    except Exception as exc:
        results.append(CheckResult("write_db_copy", Status.FAIL, str(exc)))
        return results

    try:
        # 2. account_toggle
        try:
            accounts = await copy_db.get_accounts()
            if not accounts:
                results.append(
                    CheckResult("account_toggle", Status.SKIP, "No accounts in DB"),
                )
            else:
                acc = accounts[0]
                original = acc.is_active
                await copy_db.set_account_active(acc.id, not original)
                refreshed = await copy_db.get_accounts()
                toggled = next(a for a in refreshed if a.id == acc.id)
                if toggled.is_active is not (not original):
                    raise RuntimeError("account active state did not change")
                results.append(
                    CheckResult(
                        "account_toggle",
                        Status.PASS,
                        f"id={acc.id} active: {original} -> {not original}",
                    )
                )
        except Exception as exc:
            results.append(CheckResult("account_toggle", Status.FAIL, str(exc)))

        # 3. search_query_add
        added_sq_id: int | None = None
        try:
            sq_repo = copy_db.repos.search_queries
            added_sq_id = await sq_repo.add(
                SearchQuery(name="__test_cli__", query="__test_cli__"),
            )
            queries = await sq_repo.get_all()
            found = any(q.id == added_sq_id for q in queries)
            if not found:
                raise RuntimeError("search query not found after add")
            results.append(
                CheckResult(
                    "search_query_add",
                    Status.PASS,
                    f'Added id={added_sq_id} query="__test_cli__"',
                )
            )
        except Exception as exc:
            results.append(CheckResult("search_query_add", Status.FAIL, str(exc)))

        # 4. search_query_toggle
        if added_sq_id is not None:
            try:
                await sq_repo.set_active(added_sq_id, False)
                queries = await sq_repo.get_all()
                sq = next(q for q in queries if q.id == added_sq_id)
                if sq.is_active is not False:
                    raise RuntimeError("search query active state did not change")
                results.append(
                    CheckResult(
                        "search_query_toggle",
                        Status.PASS,
                        f"id={added_sq_id} active: True -> False",
                    )
                )
            except Exception as exc:
                results.append(CheckResult("search_query_toggle", Status.FAIL, str(exc)))
        else:
            results.append(
                CheckResult("search_query_toggle", Status.SKIP, "search_query_add failed"),
            )

        # 5. search_query_delete
        if added_sq_id is not None:
            try:
                await sq_repo.delete(added_sq_id)
                queries = await sq_repo.get_all()
                found = any(q.id == added_sq_id for q in queries)
                if found:
                    raise RuntimeError("search query still present after delete")
                results.append(
                    CheckResult(
                        "search_query_delete",
                        Status.PASS,
                        f"id={added_sq_id} deleted, verified absent",
                    )
                )
            except Exception as exc:
                results.append(CheckResult("search_query_delete", Status.FAIL, str(exc)))
        else:
            results.append(
                CheckResult("search_query_delete", Status.SKIP, "search_query_add failed"),
            )

        # 6. channel_toggle
        try:
            channels = await copy_db.get_channels_with_counts()
            if not channels:
                results.append(
                    CheckResult("channel_toggle", Status.SKIP, "No channels in DB"),
                )
            else:
                ch = channels[0]
                original = ch.is_active
                await copy_db.set_channel_active(ch.id, not original)
                refreshed = await copy_db.get_channels_with_counts()
                toggled = next(c for c in refreshed if c.id == ch.id)
                if toggled.is_active is not (not original):
                    raise RuntimeError("channel active state did not change")
                results.append(
                    CheckResult(
                        "channel_toggle",
                        Status.PASS,
                        f"id={ch.id} active: {original} -> {not original}",
                    )
                )
        except Exception as exc:
            results.append(CheckResult("channel_toggle", Status.FAIL, str(exc)))

        # 7. filter_apply
        try:
            analyzer = ChannelAnalyzer(copy_db)
            report = await analyzer.analyze_all()
            count = await analyzer.apply_filters(report)
            results.append(
                CheckResult(
                    "filter_apply",
                    Status.PASS,
                    f"{count} channels filtered",
                )
            )
        except Exception as exc:
            results.append(CheckResult("filter_apply", Status.FAIL, str(exc)))

        # 8. filter_reset
        try:
            analyzer = ChannelAnalyzer(copy_db)
            await analyzer.reset_filters()
            results.append(CheckResult("filter_reset", Status.PASS, "Filters cleared"))
        except Exception as exc:
            results.append(CheckResult("filter_reset", Status.FAIL, str(exc)))

    finally:
        if copy_db:
            await copy_db.close()

    # 9. write_cleanup
    try:
        if tmp_path:
            os.unlink(tmp_path)
            if os.path.exists(tmp_path):
                raise RuntimeError(f"Temp file still exists after unlink: {tmp_path}")
        results.append(CheckResult("write_cleanup", Status.PASS, "Temp DB removed"))
    except Exception as exc:
        results.append(CheckResult("write_cleanup", Status.FAIL, str(exc)))

    return results


# ---------------------------------------------------------------------------
# Telegram live checks (operate on a temporary copy of the DB)
# ---------------------------------------------------------------------------


async def _tg_call(
    coro,
    timeout: int = TELEGRAM_TIMEOUT,
    *,
    pool=None,
    phone: str | None = None,
    check_name: str | None = None,
):
    """Wrap a Telegram API call with timeout and centralized flood-wait handling."""
    try:
        return await run_with_flood_wait(
            coro,
            operation=check_name or "telegram_call",
            phone=phone,
            pool=pool,
            logger_=logger,
            timeout=float(timeout),
        )
    except asyncio.TimeoutError as exc:
        raise TimeoutError(f"Timed out after {timeout}s") from exc


async def _wait_for_available_client_window(
    pool,
    check_name: str,
    *,
    premium: bool = False,
    base_detail: str | None = None,
) -> str | None:
    availability_method = (
        "get_premium_stats_availability" if premium else "get_stats_availability"
    )
    availability_getter = getattr(pool, availability_method, None)
    if not callable(availability_getter):
        return base_detail or "No available client"

    availability = await availability_getter()
    if getattr(availability, "state", None) != "all_flooded":
        return base_detail or "No available client"

    detail = _format_all_flooded_detail(
        base_detail or f"{check_name}: no available client",
        retry_after_sec=getattr(availability, "retry_after_sec", None),
        next_available_at_utc=getattr(availability, "next_available_at_utc", None),
    )
    if (
        getattr(availability, "retry_after_sec", None) is not None
        and availability.retry_after_sec <= SHORT_FLOOD_WAIT_RETRY_SEC
    ):
        until = (
            availability.next_available_at_utc.isoformat()
            if availability.next_available_at_utc is not None
            else "unknown"
        )
        logger.info(
            "%s: all clients flood-waited; waiting %ss until %s before retry",
            check_name,
            availability.retry_after_sec,
            until,
        )
        await asyncio.sleep(availability.retry_after_sec + 1)
        return None
    return detail


def _is_regular_search_client_unavailable_error(detail: str | None) -> bool:
    return detail == "Нет доступных Telegram-аккаунтов. Проверьте подключение."


def _is_premium_flood_unavailable_error(detail: str | None) -> bool:
    return detail == "Premium-аккаунты временно недоступны из-за Flood Wait."


async def _run_operation_with_flood_policy(
    operation_factory,
    *,
    pool,
    check_name: str,
    timeout: int = TELEGRAM_TIMEOUT,
):
    while True:
        try:
            return await asyncio.wait_for(operation_factory(), timeout=timeout)
        except HandledFloodWaitError as exc:
            await _handle_live_flood_wait(pool, check_name, exc.info)


async def _run_search_operation(
    operation_factory,
    *,
    pool,
    check_name: str,
    success_detail_factory,
    timeout: int = TELEGRAM_SEARCH_TIMEOUT,
    premium_error_is_skip: bool = False,
) -> CheckResult:
    while True:
        result = await asyncio.wait_for(operation_factory(), timeout=timeout)
        flood_wait = _get_search_result_flood_wait(result)
        if flood_wait is not None:
            await _handle_live_flood_wait(pool, check_name, flood_wait)
            continue
        if result.error:
            if _is_regular_search_client_unavailable_error(result.error):
                detail = await _wait_for_available_client_window(
                    pool,
                    check_name,
                    base_detail=result.error,
                )
                if detail is None:
                    continue
                return CheckResult(check_name, Status.SKIP, detail)
            if _is_premium_flood_unavailable_error(result.error):
                detail = await _wait_for_available_client_window(
                    pool,
                    check_name,
                    premium=True,
                    base_detail=result.error,
                )
                if detail is None:
                    continue
                return CheckResult(check_name, Status.SKIP, detail)
            if premium_error_is_skip and "Premium" in result.error:
                return CheckResult(check_name, Status.SKIP, result.error)
            return CheckResult(check_name, Status.FAIL, result.error)
        return CheckResult(check_name, Status.PASS, success_detail_factory(result))


async def _run_warm_dialog_cache_step(pool) -> CheckResult:
    check_name = "tg_warm_dialog_cache"
    while True:
        client_tuple = await pool.get_available_client()
        if not client_tuple:
            detail = await _wait_for_available_client_window(pool, check_name)
            if detail is None:
                continue
            return CheckResult(check_name, Status.SKIP, detail)

        session, phone = client_tuple
        session = adapt_transport_session(session, disconnect_on_close=False)
        try:
            await _tg_call(
                session.warm_dialog_cache(),
                TELEGRAM_DIALOG_TIMEOUT,
                pool=pool,
                phone=phone,
                check_name=check_name,
            )
            marker = getattr(pool, "mark_dialogs_fetched", None)
            if callable(marker):
                marker_result = marker(phone)
                if asyncio.iscoroutine(marker_result):
                    await marker_result
            return CheckResult(check_name, Status.PASS, f"OK via {phone}")
        except HandledFloodWaitError as exc:
            try:
                await _handle_live_flood_wait(pool, check_name, exc.info)
            except TelegramLiveStepSkipError as stop_exc:
                return CheckResult(check_name, Status.SKIP, str(stop_exc))
            continue
        except Exception as exc:
            return CheckResult(check_name, Status.FAIL, _format_exception(exc))
        finally:
            await pool.release_client(phone)


def _skip_remaining_tg_checks(results: list, reason: str, names: list[str]) -> None:
    for name in names:
        results.append(CheckResult(name, Status.SKIP, reason))


async def _run_telegram_live_checks(config_path: str) -> list[CheckResult]:
    from src.search.engine import SearchEngine
    from src.telegram.collector import Collector

    results: list[CheckResult] = []
    copy_db: Database | None = None
    tmp_path: str | None = None
    pool = None

    # 1. tg_db_copy
    try:
        copy_db, tmp_path, config = await _init_db_copy(config_path)
        results.append(CheckResult("tg_db_copy", Status.PASS, f"Copied to {tmp_path}"))
    except Exception as exc:
        results.append(CheckResult("tg_db_copy", Status.FAIL, str(exc)))
        return results

    # 2. tg_pool_init
    try:
        _, pool = await _tg_call(
            runtime.init_pool(config, copy_db),
            check_name="tg_pool_init",
        )
        clients = pool.clients if hasattr(pool, "clients") else {}
        if not clients:
            results.append(CheckResult("tg_pool_init", Status.SKIP, "No accounts connected"))
            _skip_remaining_tg_checks(results, "pool init skipped", _TG_CHECKS_AFTER_POOL)
            await _cleanup_telegram(pool, copy_db, tmp_path, results)
            return results
        await _disable_flood_auto_sleep(pool)
        results.append(
            CheckResult("tg_pool_init", Status.PASS, f"{len(clients)} clients connected"),
        )
    except HandledFloodWaitError as exc:
        results.append(CheckResult("tg_pool_init", Status.SKIP, exc.info.detail))
        _skip_remaining_tg_checks(results, "pool init skipped", _TG_CHECKS_AFTER_POOL)
        await _cleanup_telegram(pool, copy_db, tmp_path, results)
        return results
    except Exception as exc:
        results.append(CheckResult("tg_pool_init", Status.FAIL, _format_exception(exc)))
        _skip_remaining_tg_checks(results, "pool init failed", _TG_CHECKS_AFTER_POOL)
        await _cleanup_telegram(pool, copy_db, tmp_path, results)
        return results

    engine = SearchEngine(copy_db, pool)

    # 3. tg_users_info
    try:
        users = await _run_operation_with_flood_policy(
            lambda: pool.get_users_info(),
            pool=pool,
            check_name="tg_users_info",
        )
        names = ", ".join(u.phone for u in users)
        results.append(CheckResult("tg_users_info", Status.PASS, names))
    except TelegramLiveStepSkipError as exc:
        results.append(CheckResult("tg_users_info", Status.SKIP, str(exc)))
        await _cleanup_telegram(pool, copy_db, tmp_path, results)
        return results
    except Exception as exc:
        results.append(CheckResult("tg_users_info", Status.FAIL, _format_exception(exc)))

    # 4. tg_get_dialogs
    try:
        dialogs = await _run_operation_with_flood_policy(
            lambda: pool.get_dialogs(),
            pool=pool,
            check_name="tg_get_dialogs",
        )
        results.append(
            CheckResult("tg_get_dialogs", Status.PASS, f"{len(dialogs)} dialogs"),
        )
    except TelegramLiveStepSkipError as exc:
        results.append(CheckResult("tg_get_dialogs", Status.SKIP, str(exc)))
        await _cleanup_telegram(pool, copy_db, tmp_path, results)
        return results
    except Exception as exc:
        results.append(CheckResult("tg_get_dialogs", Status.FAIL, _format_exception(exc)))

    # 5. tg_resolve_channel
    channels = await copy_db.get_channels(active_only=True)
    target_with_username = next((ch for ch in channels if ch.username), None)
    if not target_with_username:
        results.append(
            CheckResult("tg_resolve_channel", Status.SKIP, "No channels with username"),
        )
    else:
        try:
            entity = await _run_operation_with_flood_policy(
                lambda: pool.resolve_channel(target_with_username.username),
                pool=pool,
                check_name="tg_resolve_channel",
            )
            if entity:
                results.append(
                    CheckResult(
                        "tg_resolve_channel",
                        Status.PASS,
                        f"@{target_with_username.username} resolved OK",
                    )
                )
            else:
                results.append(
                    CheckResult(
                        "tg_resolve_channel",
                        Status.FAIL,
                        f"@{target_with_username.username} not resolved",
                    )
                )
        except TelegramLiveStepSkipError as exc:
            results.append(CheckResult("tg_resolve_channel", Status.SKIP, str(exc)))
            await _cleanup_telegram(pool, copy_db, tmp_path, results)
            return results
        except Exception as exc:
            results.append(CheckResult("tg_resolve_channel", Status.FAIL, _format_exception(exc)))

    # Refresh entity cache (StringSession loses it between restarts)
    warm_result = await _run_warm_dialog_cache_step(pool)
    results.append(warm_result)
    if warm_result.status == Status.SKIP:
        await _cleanup_telegram(pool, copy_db, tmp_path, results)
        return results

    # 6. tg_iter_messages — single channel, 10 messages
    active_channels = [ch for ch in channels if ch.is_active]
    if not active_channels:
        results.append(
            CheckResult("tg_iter_messages", Status.SKIP, "No active channels"),
        )
    else:
        ch = active_channels[0]
        while True:
            client_tuple = await pool.get_available_client()
            if not client_tuple:
                detail = await _wait_for_available_client_window(pool, "tg_iter_messages")
                if detail is None:
                    continue
                results.append(CheckResult("tg_iter_messages", Status.SKIP, detail))
                await _cleanup_telegram(pool, copy_db, tmp_path, results)
                return results

            session, phone = client_tuple
            session = adapt_transport_session(session, disconnect_on_close=False)
            try:
                entity = await _tg_call(
                    session.resolve_entity(ch.channel_id),
                    pool=pool,
                    phone=phone,
                    check_name="tg_iter_messages",
                )

                async def _collect_messages() -> int:
                    msg_count = 0
                    async for msg in session.stream_messages(entity, limit=10):
                        if msg.text or msg.media:
                            message = Message(
                                channel_id=ch.channel_id,
                                message_id=msg.id,
                                sender_id=msg.sender_id,
                                sender_name=Collector._get_sender_name(msg),
                                text=msg.text,
                                media_type=Collector._get_media_type(msg),
                                date=(
                                    msg.date.replace(tzinfo=timezone.utc)
                                    if msg.date and msg.date.tzinfo is None
                                    else msg.date
                                ),
                            )
                            await copy_db.insert_message(message)
                            msg_count += 1
                    return msg_count

                msg_count = await _tg_call(
                    _collect_messages(),
                    pool=pool,
                    phone=phone,
                    check_name="tg_iter_messages",
                )
                results.append(
                    CheckResult(
                        "tg_iter_messages",
                        Status.PASS,
                        f"{msg_count} msgs from ch={ch.channel_id}",
                    )
                )
                break
            except HandledFloodWaitError as exc:
                try:
                    await _handle_live_flood_wait(pool, "tg_iter_messages", exc.info)
                except TelegramLiveStepSkipError as stop_exc:
                    results.append(CheckResult("tg_iter_messages", Status.SKIP, str(stop_exc)))
                    await _cleanup_telegram(pool, copy_db, tmp_path, results)
                    return results
                continue
            except Exception as exc:
                results.append(CheckResult("tg_iter_messages", Status.FAIL, _format_exception(exc)))
                break
            finally:
                await pool.release_client(phone)

    # 7. tg_channel_stats
    if not active_channels:
        results.append(
            CheckResult("tg_channel_stats", Status.SKIP, "No active channels"),
        )
    else:
        try:
            ch = active_channels[0]
            collector = Collector(pool, copy_db, config.scheduler)
            stats = await _run_operation_with_flood_policy(
                lambda: collector.collect_channel_stats(ch),
                pool=pool,
                check_name="tg_channel_stats",
            )
            if stats:
                results.append(
                    CheckResult(
                        "tg_channel_stats",
                        Status.PASS,
                        f"ch={ch.channel_id} subs={stats.subscriber_count}",
                    )
                )
            else:
                results.append(
                    CheckResult(
                        "tg_channel_stats",
                        Status.PASS,
                        f"ch={ch.channel_id} stats=None (no data)",
                    )
                )
        except TelegramLiveStepSkipError as exc:
            results.append(CheckResult("tg_channel_stats", Status.SKIP, str(exc)))
            await _cleanup_telegram(pool, copy_db, tmp_path, results)
            return results
        except Exception as exc:
            results.append(CheckResult("tg_channel_stats", Status.FAIL, _format_exception(exc)))

    # 8. tg_search_my_chats
    try:
        results.append(
            await _run_search_operation(
                lambda: engine.search_my_chats("test", limit=5),
                pool=pool,
                check_name="tg_search_my_chats",
                success_detail_factory=lambda result: f"{result.total} results",
            )
        )
    except TelegramLiveStepSkipError as exc:
        results.append(CheckResult("tg_search_my_chats", Status.SKIP, str(exc)))
        await _cleanup_telegram(pool, copy_db, tmp_path, results)
        return results
    except Exception as exc:
        results.append(CheckResult("tg_search_my_chats", Status.FAIL, _format_exception(exc)))

    # 9. tg_search_in_channel
    if not channels:
        results.append(
            CheckResult("tg_search_in_channel", Status.SKIP, "No channels"),
        )
    else:
        try:
            ch = channels[0]
            results.append(
                await _run_search_operation(
                    lambda: engine.search_in_channel(ch.channel_id, "test", limit=5),
                    pool=pool,
                    check_name="tg_search_in_channel",
                    success_detail_factory=lambda result: f"ch={ch.channel_id}: {result.total} results",
                )
            )
        except TelegramLiveStepSkipError as exc:
            results.append(CheckResult("tg_search_in_channel", Status.SKIP, str(exc)))
            await _cleanup_telegram(pool, copy_db, tmp_path, results)
            return results
        except Exception as exc:
            results.append(CheckResult("tg_search_in_channel", Status.FAIL, _format_exception(exc)))

    # 10. tg_search_premium
    try:
        results.append(
            await _run_search_operation(
                lambda: engine.search_telegram("test", limit=5),
                pool=pool,
                check_name="tg_search_premium",
                success_detail_factory=lambda result: f"{result.total} results",
                premium_error_is_skip=True,
            )
        )
    except TelegramLiveStepSkipError as exc:
        results.append(CheckResult("tg_search_premium", Status.SKIP, str(exc)))
        await _cleanup_telegram(pool, copy_db, tmp_path, results)
        return results
    except Exception as exc:
        results.append(CheckResult("tg_search_premium", Status.FAIL, _format_exception(exc)))

    # 11. tg_search_quota
    try:
        while True:
            quota = await _run_operation_with_flood_policy(
                lambda: engine.check_search_quota("test"),
                pool=pool,
                check_name="tg_search_quota",
            )
            if quota is not None:
                detail = str(quota) if quota else "No quota info"
                results.append(CheckResult("tg_search_quota", Status.PASS, detail))
                break

            detail = await _wait_for_available_client_window(
                pool,
                "tg_search_quota",
                premium=True,
                base_detail="No premium account or quota unavailable",
            )
            if detail is None:
                continue
            results.append(CheckResult("tg_search_quota", Status.SKIP, detail))
            break
    except TelegramLiveStepSkipError as exc:
        results.append(CheckResult("tg_search_quota", Status.SKIP, str(exc)))
        await _cleanup_telegram(pool, copy_db, tmp_path, results)
        return results
    except Exception as exc:
        results.append(CheckResult("tg_search_quota", Status.FAIL, _format_exception(exc)))

    # 12. tg_cleanup
    await _cleanup_telegram(pool, copy_db, tmp_path, results)

    return results


async def _cleanup_telegram(pool, copy_db, tmp_path, results) -> None:
    try:
        if pool:
            await pool.disconnect_all()
        if copy_db:
            await copy_db.close()
        if tmp_path and os.path.exists(tmp_path):
            os.unlink(tmp_path)
        results.append(CheckResult("tg_cleanup", Status.PASS, "Resources released"))
    except Exception as exc:
        results.append(CheckResult("tg_cleanup", Status.FAIL, str(exc)))


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def run(args: argparse.Namespace) -> None:
    if args.test_action == "benchmark":
        _run_pytest_benchmark()
        return

    async def _run() -> None:
        results: list[CheckResult] = []

        action = args.test_action  # "all", "read", "write", "telegram"
        run_read = action in ("all", "read")
        run_write = action in ("all", "write")
        run_telegram = action in ("all", "telegram")

        if run_read:
            print("=== Read Tests ===")
            try:
                config, db = await runtime.init_db(args.config)
            except Exception as exc:
                check_result = CheckResult("db_init", Status.FAIL, f"Cannot init DB: {exc}")
                _print_result(check_result)
                print(
                    "\n--- Test Summary ---\n" "0 passed, 1 failed, 0 skipped (1 total)",
                )
                sys.exit(1)

            results.append(CheckResult("db_init", Status.PASS, "Database initialized"))
            _print_result(results[-1])

            try:
                db_checks = [
                    _check_get_stats(db),
                    _check_account_list(db),
                    _check_channel_list(db),
                    _check_notification_queries(db),
                    _check_local_search(db),
                    _check_collection_tasks(db),
                    _check_recent_searches(db),
                    _check_pipeline_list(db),
                    _check_notification_bot(db),
                    _check_photo_tasks(db),
                ]
                for coro in db_checks:
                    check_result = await coro
                    results.append(check_result)
                    _print_result(check_result)
            finally:
                await db.close()

        if run_write:
            print("\n=== Write Tests (on DB copy) ===")
            write_results = await _run_write_checks(args.config)
            for check_result in write_results:
                results.append(check_result)
                _print_result(check_result)

        if run_telegram:
            print("\n=== Telegram Live Tests (on DB copy) ===")
            tg_results = await _run_telegram_live_checks(args.config)
            for check_result in tg_results:
                results.append(check_result)
                _print_result(check_result)

        passed = sum(1 for check_result in results if check_result.status == Status.PASS)
        failed = sum(1 for check_result in results if check_result.status == Status.FAIL)
        skipped = sum(1 for check_result in results if check_result.status == Status.SKIP)
        total = len(results)

        print("\n--- Test Summary ---")
        print(f"{passed} passed, {failed} failed, {skipped} skipped ({total} total)")

        if failed:
            sys.exit(1)

    asyncio.run(_run())
