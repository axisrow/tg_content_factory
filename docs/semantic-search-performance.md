# Portable semantic search performance

The fallback used when sqlite-vec is unavailable now builds and searches its
NumPy index in a worker thread. One compute slot per `LocalSearch` instance limits
CPU concurrency; cancellation retains that slot until the running thread exits.
Concurrent cache misses share a build, and invalidation prevents an in-flight
build from publishing stale data. Filtered-channel scope remains part of the cache.

Embeddings are read with keyset pagination (512 rows), decoded in the worker and
retained as float32 chunks. Finalization joins those chunks into a contiguous
matrix. This removes the full corpus of Python float lists; the matrix still
requires O(number of vectors × dimensions) memory, and concatenation temporarily
retains both chunks and the final matrix.

Ranked messages are fetched in batches of 400 IDs using the existing ordered
repository loader. Deleted messages are skipped and rank, date/length filters and
pagination are preserved. Topic-bearing results may require an additional title
query per batch.

## Reproduce

```sh
OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 \
  .venv/bin/python scripts/benchmark_semantic_search.py
```

The script creates temporary SQLite databases using the project schema. It uses
1,000 / 10,000 / 50,000 synthetic 384-dimensional vectors, seed 42, numerical
library warmup and seven search repetitions. Every size/mode runs in a fresh
process. `before` reproduces the original all-JSON loader, synchronous index
build/search and per-candidate reads; `after` uses the new index construction,
compute slot and batch loader. The embedding provider and Telegram are excluded.

A 1 ms heartbeat records scheduling delay alongside the real `/health` handler
invoked through an in-process HTTP ASGI client. `health_scheduled_max_ms` includes
scheduling delay plus HTTP/DB handling, excluding the intended 1 ms sleep. This
is a concurrency probe, not a benchmark of a fully deployed web application.
Peak RSS is for the whole worker process, including imports and temporary arrays.
Database generation occurs in the parent process outside those measurements.

The benchmark does not establish production throughput: the checked-in local
DB has no messages or embeddings. Use representative production data and actual
concurrency before choosing a larger-scale vector backend.

## Validation

Commands used (Python 3.12.14, NumPy 2.5.4, scikit-learn 1.9.1):

- `ruff check tg_agent/ tests/ conftest.py scripts/benchmark_semantic_search.py`: passed.
- `pytest tests/test_semantic_search_performance.py tests/test_semantic_search_regressions.py -q`: 18 passed.
- Related search/bootstrap coverage: 92 passed in an earlier focused run.
- `pytest tests/ -q -m aiosqlite_serial -n auto --dist=loadfile`: 1,150 passed.
- `pytest tests/ -v -m "not aiosqlite_serial" -n auto`: 10,261 passed, 152 skipped,
  355 subtests passed, 5 failed. All five failures were CLI subprocess imports of
  the missing local `tg_shared` package. The same five failures reproduced against
  unchanged HEAD in a temporary checkout. After installing the local package
  with `pip install --no-index --no-deps --no-build-isolation -e packages/tg_shared`,
  rerunning `tests/test_real_telegram_policy.py` gave 49 passed. The entire large
  group was not rerun after that environment repair.

The tests cover concurrent cache misses, scope changes, invalidation during a
build, retry after failure, cancellation without overlapping CPU jobs, off-loop
build/search execution, JSON corruption, ranking/filter/pagination compatibility,
bounded DB pages and ranked candidate reads across the SQLite parameter limit.

Tests and benchmarks ran outside the execution sandbox: its restrictions prevented
even a minimal `asyncio.to_thread` example from waking the event loop. No workaround
for that environment restriction was added to application code.

## Measurements (2026-10-28)

Raw results: [semantic-search-benchmark.jsonl](semantic-search-benchmark.jsonl).

Numbers below are before → after. Peak RSS includes the whole worker process.

| Vectors | Build, ms | Max build loop lag, ms | Search median, ms | Max search loop lag, ms | Peak RSS, MiB |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1,000 | 221.53 → 201.16 | 178.66 → 25.07 | 2.28 → 3.79 | 2.46 → 1.8 | 220.3 → 217.3 |
| 10,000 | 2183.43 → 1523.67 | 1900.49 → 21.11 | 11.68 → 14.49 | 13.01 → 42.08 | 435.4 → 253.7 |
| 50,000 | 10337.1 → 7828.77 | 8805.84 → 93.8 | 53.26 → 65.21 | 56.73 → 2.89 | 1390.1 → 438.2 |

At 50,000 vectors, loading 200 ranked candidates used 200 → 1 SELECTs and took 56.23 → 14.07 ms.
The maximum scheduled HTTP /health latency during unfiltered search was 58.31 → 3.99 ms; during the build it was 10358.19 → 100.42 ms.

The primary gain is responsiveness and memory, not faster numerical search. The host is shared, so maxima include scheduler noise; this is not a hard latency guarantee. The under-20-ms search heartbeat target was met at 50,000 vectors in this run. Builds still have occasional pauses, and the raw results retain the higher 10,000-vector search outlier.
