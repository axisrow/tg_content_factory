"""Concurrency and batching guarantees for the portable search path."""

import asyncio
import threading
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from tg_agent.models import Message
from tg_agent.search.local_search import LocalSearch
from tg_agent.search.numpy_semantic import NumpySemanticIndex

pytestmark = pytest.mark.anyio


def make_search():
    calls = []

    async def batches(include_filtered=False):
        calls.append(include_filtered)
        yield [(1, '[1.0, 0.0]'), (2, '[0.0, 1.0]')]

    repo = SimpleNamespace(iter_embedding_json_batches=batches)
    bundle = SimpleNamespace(messages=repo, vec_available=False, numpy_available=True)
    search = LocalSearch(bundle, SimpleNamespace(embed_query=AsyncMock(return_value=[1.0, 0.0])))
    return search, repo, calls


async def test_concurrent_builds_share_index_and_scope_switch_rebuilds():
    search, _, calls = make_search()
    indexes = await asyncio.gather(*(search._ensure_numpy_index() for _ in range(8)))
    assert all(index is indexes[0] for index in indexes)
    assert indexes[0].size == 2
    assert calls == [False]
    included = await search._ensure_numpy_index(True)
    assert included is not indexes[0]
    assert calls == [False, True]


async def test_invalidation_during_build_does_not_publish_stale_index():
    search, repo, calls = make_search()
    started, release = asyncio.Event(), asyncio.Event()

    async def batches(include_filtered=False):
        calls.append(include_filtered)
        if len(calls) == 1:
            started.set()
            await release.wait()
            yield [(1, '[1.0, 0.0]')]
        else:
            yield [(2, '[0.0, 1.0]')]

    repo.iter_embedding_json_batches = batches
    task = asyncio.create_task(search._ensure_numpy_index())
    await asyncio.wait_for(started.wait(), 2)
    search.invalidate_numpy_index()
    release.set()
    index = await asyncio.wait_for(task, 5)
    assert index.search([0.0, 1.0], k=1)[0][0] == 2
    assert calls == [False, False]


async def test_failed_build_can_be_retried():
    search, repo, _ = make_search()
    original = repo.iter_embedding_json_batches

    async def broken(**kwargs):
        raise RuntimeError('read failed')
        yield

    repo.iter_embedding_json_batches = broken
    with pytest.raises(RuntimeError, match='read failed'):
        await search._ensure_numpy_index()
    assert not search._numpy_index_loaded
    repo.iter_embedding_json_batches = original
    assert (await search._ensure_numpy_index()).size == 2


async def test_search_runs_off_loop_and_loads_candidates_in_one_batch(monkeypatch):
    search, repo, _ = make_search()
    await search._ensure_numpy_index()
    started, release = threading.Event(), threading.Event()
    original = NumpySemanticIndex.search

    def slow_search(self, *args, **kwargs):
        started.set()
        assert release.wait(3), 'event loop failed to release worker'
        return original(self, *args, **kwargs)

    monkeypatch.setattr(NumpySemanticIndex, 'search', slow_search)
    repo.get_by_ids = AsyncMock(return_value=[
        Message(id=1, channel_id=100, message_id=1, text='first', date='2026-10-14'),
        Message(id=2, channel_id=100, message_id=2, text='second', date='2026-10-14'),
    ])
    task = asyncio.create_task(search.search_semantic('query', limit=1, offset=1))
    try:
        for _ in range(200):
            if started.is_set():
                break
            await asyncio.sleep(.005)
        assert started.is_set()
    finally:
        release.set()
    result = await asyncio.wait_for(task, 5)
    assert [m.text for m in result.messages] == ['second']
    assert result.total == 2
    repo.get_by_ids.assert_awaited_once_with([1, 2])


async def test_cancelled_compute_retains_slot_until_thread_finishes():
    search, _, _ = make_search()
    started, release, second_started = threading.Event(), threading.Event(), threading.Event()

    def first():
        started.set()
        assert release.wait(3)

    task = asyncio.create_task(search._compute(first))
    for _ in range(200):
        if started.is_set():
            break
        await asyncio.sleep(.005)
    assert started.is_set()
    task.cancel()
    second = asyncio.create_task(search._compute(second_started.set))
    try:
        await asyncio.sleep(.02)
        assert not second_started.is_set()
    finally:
        release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    await second
    assert second_started.is_set()


async def test_build_runs_off_loop(monkeypatch):
    search, _, _ = make_search()
    started, release = threading.Event(), threading.Event()
    original = NumpySemanticIndex.add_json_batch

    def slow_decode(self, batch):
        started.set()
        assert release.wait(3), 'event loop failed to release index builder'
        return original(self, batch)

    monkeypatch.setattr(NumpySemanticIndex, 'add_json_batch', slow_decode)
    task = asyncio.create_task(search._ensure_numpy_index())
    try:
        for _ in range(200):
            if started.is_set():
                break
            await asyncio.sleep(.005)
        assert started.is_set()
    finally:
        release.set()
    assert (await asyncio.wait_for(task, 5)).size == 2


def test_chunked_index_skips_invalid_json_and_matches_legacy_ranking():
    legacy = NumpySemanticIndex()
    legacy.load([(1, [1.0, 0.0]), (2, [0.0, 1.0])])
    chunked = NumpySemanticIndex()
    chunked.add_json_batch([(99, 'broken'), (1, '[1.0,0.0]')])
    chunked.add_json_batch([(2, '[0.0,1.0]')])
    chunked.finish_loading()
    assert chunked.size == 2
    assert chunked.search([1.0, 0.0]) == legacy.search([1.0, 0.0])
