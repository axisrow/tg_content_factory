"""Compare legacy and current portable search on synthetic SQLite data.

Run with the project venv: python scripts/benchmark_semantic_search.py
Each size/mode runs in a fresh process. No Telegram or embedding provider is used.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import resource
import sqlite3
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from types import SimpleNamespace

_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / 'packages' / 'tg_shared' / 'src'))


async def measure(database: str, mode: str, rows: int) -> dict:
    import aiosqlite
    import httpx
    import numpy as np
    from fastapi import FastAPI
    from sklearn.neighbors import NearestNeighbors

    from tg_agent.database.repositories.messages import MessagesRepository
    from tg_agent.search.local_search import LocalSearch
    from tg_agent.search.numpy_semantic import NumpySemanticIndex
    from tg_agent.web.assembly import register_builtin_endpoints

    # Warm numerical imports equally; generation happens in the parent process.
    NearestNeighbors(metric='cosine').fit(np.ones((2, 384), dtype=np.float32))
    async with aiosqlite.connect(database) as conn:
        conn.row_factory = aiosqlite.Row
        repo = MessagesRepository(conn)
        local = LocalSearch(SimpleNamespace(messages=repo))
        queries = []
        await conn.set_trace_callback(queries.append)
        app = FastAPI()
        app.state.container = SimpleNamespace(db=conn, pool=SimpleNamespace(clients={}))
        register_builtin_endpoints(app)
        http = httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://benchmark')
        running = True
        lags = []
        health_latencies = []

        async def heartbeat():
            while running:
                start = time.perf_counter()
                await asyncio.sleep(.001)
                lags.append(max(0, time.perf_counter() - start - .001) * 1000)

        async def health_probe():
            while running:
                start = time.perf_counter()
                await asyncio.sleep(.001)
                response = await http.get('/health')
                assert response.json()['status'] == 'healthy'
                health_latencies.append(max(0, time.perf_counter() - start - .001) * 1000)

        pulse = asyncio.create_task(heartbeat())
        health = asyncio.create_task(health_probe())
        await asyncio.sleep(0)
        start = time.perf_counter()
        if mode == 'before':
            index = NumpySemanticIndex()
            embeddings = await repo.load_all_embeddings_json()
            index.load(embeddings)
            # The legacy loader retained these through index.load(), not search.
            del embeddings
        else:
            index = await local._ensure_numpy_index()
        build_ms = (time.perf_counter() - start) * 1000
        await asyncio.sleep(.005)
        build_lag = max(lags, default=0)
        build_health = max(health_latencies, default=0)
        query = np.random.default_rng(42).standard_normal(384).tolist()

        async def search(filtered):
            allowed = set(range(1, rows + 1, 10)) if filtered else None
            if mode == 'before':
                return index.search(query, k=200, allowed_ids=allowed)
            return await local._compute(index.search, query, k=200, allowed_ids=allowed)

        results = []
        for filtered in (False, True):
            await search(filtered)
            # Drain probes spanning the warmup before recording warm samples.
            await asyncio.sleep(.01)
            lags.clear()
            health_latencies.clear()
            samples = []
            for _ in range(7):
                await asyncio.sleep(.005)
                start = time.perf_counter()
                neighbors = await search(filtered)
                samples.append((time.perf_counter() - start) * 1000)
            await asyncio.sleep(.005)
            results.append(dict(
                filtered=filtered, median_ms=round(statistics.median(samples), 2),
                max_ms=round(max(samples), 2), loop_max_lag_ms=round(max(lags, default=0), 2),
                health_scheduled_max_ms=round(max(health_latencies, default=0), 2),
            ))
        ids = [mid for mid, _ in neighbors]
        queries.clear()
        start = time.perf_counter()
        if mode == 'before':
            messages = [await repo.get_by_id(mid) for mid in ids]
        else:
            messages = await repo.get_by_ids(ids)
        fetch_ms = (time.perf_counter() - start) * 1000
        selects = sum(sql.lstrip().upper().startswith('SELECT') and sql.strip() != 'SELECT 1' for sql in queries)
        assert len(messages) == len(ids)
        running = False
        await pulse
        await health
        await http.aclose()
        return dict(
            mode=mode, rows=rows, dims=384, build_ms=round(build_ms, 2),
            build_loop_max_lag_ms=round(build_lag, 2), searches=results,
            build_health_scheduled_max_ms=round(build_health, 2),
            candidate_count=len(ids), candidate_selects=selects, candidate_fetch_ms=round(fetch_ms, 2),
            peak_rss_mib=round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 1),
        )


def populate(database: str, rows: int) -> None:
    import numpy as np

    from tg_agent.database.schema import SCHEMA_SQL

    rng = np.random.default_rng(42)
    with sqlite3.connect(database) as conn:
        conn.executescript(SCHEMA_SQL)
        conn.execute("INSERT INTO channels(channel_id,title) VALUES (100,'Benchmark')")
        for start in range(0, rows, 512):
            count = min(512, rows - start)
            vectors = rng.standard_normal((count, 384), dtype=np.float32)
            conn.executemany(
                'INSERT INTO messages(id,channel_id,message_id,text,date) VALUES (?,100,?,?,?)',
                [(i + 1, i + 1, f'Message {i}', '2026-10-14T12:00:00+00:00')
                 for i in range(start, start + count)],
            )
            conn.executemany(
                'INSERT INTO message_embeddings_json(message_id,embedding,dims) VALUES (?,?,384)',
                [(start + i + 1, json.dumps(v.tolist(), separators=(',', ':')))
                 for i, v in enumerate(vectors)],
            )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--worker', choices=['before', 'after'])
    parser.add_argument('--database')
    parser.add_argument('--rows', type=int, default=1000)
    args = parser.parse_args()
    if args.worker:
        print(json.dumps(asyncio.run(measure(args.database, args.worker, args.rows))), flush=True)
        return
    env = dict(os.environ, OPENBLAS_NUM_THREADS='1', OMP_NUM_THREADS='1')
    print(json.dumps(dict(python=sys.version.split()[0], cpus=os.cpu_count(), seed=42, repeats=7)), flush=True)
    with tempfile.TemporaryDirectory(prefix='tg-semantic-bench-') as folder:
        for rows in (1000, 10000, 50000):
            database = str(Path(folder) / f'{rows}.db')
            populate(database, rows)
            for mode in ('before', 'after'):
                subprocess.run(
                    [sys.executable, __file__, '--worker', mode, '--database', database, '--rows', str(rows)],
                    env=env, check=True,
                )
            Path(database).unlink()


if __name__ == '__main__':
    main()
