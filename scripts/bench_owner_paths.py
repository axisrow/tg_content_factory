"""Measure the paths between a Telegram session owner and its clients (#1868).

Offline: no Telegram, no network beyond 127.0.0.1. Every Telegram call is a fake
that answers at once, so the numbers are the *overhead* a path adds on top of
Telegram itself — what the redesign changes. Run on the old and the new code to
compare; scenarios the checked-out code does not have are reported as skipped.

    python scripts/bench_owner_paths.py --out docs/benchmarks/1868-before.json

Scenarios:
  cli_startup      cold `tg-agent --help` / `account list` (subprocess, median of N)
  cli_imports      modules loaded by the CLI entry point, web stack present or not
  command_queue    CLI-style enqueue into telegram_commands → worker marks it done
  gateway_op       one gateway read operation over loopback HTTP (old protocol)
  relay_invoke     one MTProto request through the owner relay (new protocol)
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import platform
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]


def _quantiles(samples: list[float]) -> dict:
    ordered = sorted(samples)

    def pick(q: float) -> float:
        return ordered[min(len(ordered) - 1, int(q * len(ordered)))]

    return {
        "n": len(ordered),
        "p50_ms": round(statistics.median(ordered) * 1000, 3),
        "p95_ms": round(pick(0.95) * 1000, 3),
        "max_ms": round(ordered[-1] * 1000, 3),
    }


def _project_dir() -> Path:
    root = Path(tempfile.mkdtemp(prefix="bench-1868-"))
    (root / "config.yaml").write_text(
        f'database:\n  path: "{root / "bench.db"}"\n', encoding="utf-8"
    )
    return root


def _cli_env() -> dict:
    env = dict(os.environ)
    env["TG_AGENT_SECRET_STORE"] = "off"
    env.pop("SESSION_ENCRYPTION_KEY", None)
    return env


def bench_cli_startup(runs: int) -> dict:
    project = _project_dir()
    result = {}
    for name, argv in {
        "help": ["--help"],
        "account_list": ["--config", str(project / "config.yaml"), "account", "list"],
    }.items():
        samples = []
        for _ in range(runs):
            started = time.perf_counter()
            subprocess.run(
                [sys.executable, "-m", "tg_agent.main", *argv],
                cwd=project, env=_cli_env(), capture_output=True, check=True, timeout=120,
            )
            samples.append(time.perf_counter() - started)
        result[name] = _quantiles(samples)
    return result


def bench_cli_imports() -> dict:
    code = (
        "import sys, json, tg_agent.cli.typer_commands;"
        "tops = {m.split('.')[0] for m in sys.modules};"
        "print(json.dumps({'modules': len(sys.modules),"
        " 'web_stack': sorted(t for t in ('fastapi', 'uvicorn', 'starlette', 'httpx') if t in tops),"
        " 'telethon': 'telethon' in tops}))"
    )
    proc = subprocess.run(
        [sys.executable, "-c", code], cwd=REPO, env=_cli_env(),
        capture_output=True, text=True, check=True, timeout=120,
    )
    return json.loads(proc.stdout)


async def bench_command_queue(commands: int) -> dict:
    """Enqueue like the CLI does, let the worker dispatcher claim and finish it."""
    from tg_agent.database import Database
    from tg_agent.services.telegram_command_dispatcher import TelegramCommandDispatcher
    from tg_agent.services.telegram_command_service import TelegramCommandService

    project = _project_dir()
    db = Database(str(project / "bench.db"))
    await db.initialize()
    dispatcher = TelegramCommandDispatcher(db, pool=None)

    async def instant(command_type, payload):
        return {"ok": True}

    dispatcher._dispatch = instant  # the Telegram side is not what we measure
    await dispatcher.start()
    service = TelegramCommandService(db)
    samples = []
    try:
        for index in range(commands):
            started = time.perf_counter()
            command_id = await service.enqueue(
                "bench.noop", payload={"index": index}, requested_by="bench", deduplicate=False
            )
            while True:
                command = await service.get(command_id)
                if command is not None and command.status.value in {"succeeded", "failed"}:
                    break
                await asyncio.sleep(0.005)
            samples.append(time.perf_counter() - started)
    finally:
        await dispatcher.stop()
        await db.close()
    return _quantiles(samples)


async def bench_gateway_op(operations: int, concurrency: int) -> dict:
    """Old gateway protocol: submit an operation, poll its receipt (loopback HTTP)."""
    try:
        import uvicorn

        from tg_agent.gateway.client import TelegramGatewayClient
        from tg_agent.gateway.protocol import History
        from tg_agent.gateway.server import create_app
        from tg_agent.gateway.store import GatewayStore
    except ImportError as exc:
        return {"skipped": f"old gateway protocol not present ({exc.name})"}
    import secrets
    from uuid import UUID, uuid4

    class InstantTelegram:
        async def execute(self, project, operation):
            return {"messages": []}

        async def close(self):
            pass

    root = Path(tempfile.mkdtemp(prefix="bench-gw-"))
    store = GatewayStore(root / "gateway")
    await store.open()
    account = await store.import_account("+100", "bench-session")
    token = secrets.token_urlsafe(32)
    project = str(uuid4())
    await store.register_project(project, root / project, token)
    await store.grant(project, account, primary=True)
    app = create_app(store, InstantTelegram())
    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=0, log_level="warning", lifespan="on"))
    serving = asyncio.create_task(server.serve())
    while not server.started:
        await asyncio.sleep(0.01)
    port = server.servers[0].sockets[0].getsockname()[1]
    client = TelegramGatewayClient(f"http://127.0.0.1:{port}", token)

    async def one() -> float:
        started = time.perf_counter()
        request = uuid4()
        await client.submit(request, History(kind="history", account_id=UUID(account), peer=123))
        receipt = await client.wait(request, timeout=60)
        assert receipt["state"] == "succeeded", receipt
        return time.perf_counter() - started

    try:
        sequential = [await one() for _ in range(operations)]
        started = time.perf_counter()
        semaphore = asyncio.Semaphore(concurrency)

        async def bounded():
            async with semaphore:
                return await one()

        await asyncio.gather(*(bounded() for _ in range(operations)))
        elapsed = time.perf_counter() - started
    finally:
        await client.close()
        server.should_exit = True
        await serving
        await store.close()
    return {
        "sequential": _quantiles(sequential),
        "concurrent": {"concurrency": concurrency, "ops": operations,
                       "ops_per_sec": round(operations / elapsed, 1)},
    }


async def bench_relay_invoke(operations: int, concurrency: int) -> dict:
    """New protocol: one MTProto request relayed to the owner's client and answered."""
    try:
        from telethon import functions, types

        from tg_agent.telegram.relay import (
            RELAY_PROTOCOL,
            RelayConnection,
            RelayEndpoint,
            RelayServer,
            RelayTelegramClient,
        )
    except ImportError:
        return {"skipped": "relay not present in this checkout"}

    state = bytes(types.updates.State(pts=1, qts=0, date=None, seq=0, unread_count=0))

    class InstantOwnerClient:
        """Answers like a relayed real client: the raw TL bytes of the result."""

        session = type("Session", (), {"dc_id": 2, "_entities": set()})()

        async def __call__(self, request):
            from telethon.extensions import BinaryReader

            with BinaryReader(state) as reader:
                return request.read_result(reader)

    class Owner:
        async def authorize(self, token):
            return token == "bench" or None

        async def phones(self, grant):
            return ["+100"]

        async def client(self, grant, phone):
            return InstantOwnerClient()

    server = RelayServer(Owner())
    port = await server.start()
    connection = await RelayConnection.open(
        RelayEndpoint(port=port, token="bench", pid=os.getpid(), protocol=RELAY_PROTOCOL)
    )
    client = RelayTelegramClient(connection, "+100", flood_sleep_threshold=0)
    await client.connect()

    async def one() -> float:
        started = time.perf_counter()
        await client(functions.updates.GetStateRequest())
        return time.perf_counter() - started

    try:
        sequential = [await one() for _ in range(operations)]
        started = time.perf_counter()
        semaphore = asyncio.Semaphore(concurrency)

        async def bounded():
            async with semaphore:
                return await one()

        await asyncio.gather(*(bounded() for _ in range(operations)))
        elapsed = time.perf_counter() - started
    finally:
        await connection.close()
        await server.stop()
    return {
        "sequential": _quantiles(sequential),
        "concurrent": {"concurrency": concurrency, "ops": operations,
                       "ops_per_sec": round(operations / elapsed, 1)},
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--out", type=Path, help="write the JSON report here")
    parser.add_argument("--runs", type=int, default=10, help="subprocess runs per CLI scenario")
    parser.add_argument("--ops", type=int, default=60, help="operations per protocol scenario")
    parser.add_argument("--concurrency", type=int, default=8)
    parser.add_argument("--only", nargs="*", help="scenario names to run")
    args = parser.parse_args()

    scenarios = {
        "cli_startup": lambda: bench_cli_startup(args.runs),
        "cli_imports": bench_cli_imports,
        "command_queue": lambda: asyncio.run(bench_command_queue(max(10, args.ops // 3))),
        "gateway_op": lambda: asyncio.run(bench_gateway_op(args.ops, args.concurrency)),
        "relay_invoke": lambda: asyncio.run(bench_relay_invoke(args.ops, args.concurrency)),
    }
    report = {
        "git": subprocess.run(["git", "rev-parse", "--short", "HEAD"], cwd=REPO,
                              capture_output=True, text=True).stdout.strip(),
        "python": platform.python_version(),
        "machine": platform.machine(),
        "results": {},
    }
    for name, run in scenarios.items():
        if args.only and name not in args.only:
            continue
        print(f"[bench] {name} …", file=sys.stderr, flush=True)
        report["results"][name] = run()
    text = json.dumps(report, indent=2, ensure_ascii=False)
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(text + "\n", encoding="utf-8")
    print(text)


if __name__ == "__main__":
    main()
