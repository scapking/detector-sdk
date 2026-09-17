"""Async API: same results as the sync client, plus concurrency behaviour."""

from __future__ import annotations

import asyncio

import pytest

from detector import AsyncDetector, adistance, alookup, alookup_many

from .conftest import V4_ALIBABA, V4_CHINA, V4_CLOUDFLARE, V4_GOOGLE, V6_GOOGLE


def test_alookup_module_helper() -> None:
    info = asyncio.run(alookup(V4_GOOGLE))
    assert info.found is True
    assert info.country.iso_code == "US"


def test_async_client_matches_sync(detector) -> None:
    async def scenario() -> None:
        async with await AsyncDetector.create(cache_size=0) as async_detector:
            first = await async_detector.lookup(V4_GOOGLE)
            assert first.to_dict() == detector.lookup(V4_GOOGLE).to_dict()
            assert await async_detector.describe() == detector.describe()

    asyncio.run(scenario())


def test_async_batch_and_stream() -> None:
    async def scenario() -> None:
        async with await AsyncDetector.create(cache_size=0, window=2) as async_detector:
            inputs = [V4_GOOGLE, V4_CLOUDFLARE, V4_CHINA, V6_GOOGLE, "bad"]
            results = await async_detector.lookup_many(inputs)
            assert [item.ip for item in results] == inputs
            assert [item.found for item in results] == [True, True, True, True, False]

            streamed = [item.ip async for item in async_detector.stream(inputs)]
            assert streamed == inputs

    asyncio.run(scenario())


def test_async_distance() -> None:
    async def scenario() -> None:
        async with await AsyncDetector.create(cache_size=0) as async_detector:
            single = await async_detector.distance(V4_GOOGLE, V4_CLOUDFLARE)
            assert single.available
            many = await async_detector.distance(V4_GOOGLE, [V4_CLOUDFLARE, V4_ALIBABA])
            assert len(many) == 2
            ranked = await async_detector.nearest(V4_CHINA, [V4_GOOGLE, V4_ALIBABA], limit=1)
            assert len(ranked) == 1

    asyncio.run(scenario())


def test_async_protocol() -> None:
    async def scenario() -> None:
        async with await AsyncDetector.create(cache_size=0) as async_detector:
            response = await async_detector.request(
                {"type": "ipv4", "action": "info", "data": {"ip": V4_GOOGLE}}
            )
            assert response.ok
            text = await async_detector.request_json(
                '{"type":"auto","action":"distance","data":{"ip":"8.8.8.8","list":["1.1.1.1"]}}'
            )
            assert '"status": "ok"' in text

    asyncio.run(scenario())


def test_async_module_helpers_share_client() -> None:
    async def scenario() -> None:
        infos = await alookup_many([V4_GOOGLE, V4_CHINA])
        assert [item.country.iso_code for item in infos] == ["US", "CN"]
        rows = await adistance(V4_GOOGLE, V4_CHINA)
        assert rows.available

    asyncio.run(scenario())


def test_async_does_not_block_the_loop() -> None:
    """A concurrent ticker must keep running while a batch is processed."""

    async def scenario() -> int:
        ticks = 0

        async def ticker() -> None:
            nonlocal ticks
            while True:
                await asyncio.sleep(0.01)
                ticks += 1

        task = asyncio.create_task(ticker())
        async with await AsyncDetector.create(cache_size=0) as async_detector:
            await async_detector.lookup_many([V4_GOOGLE, V4_CLOUDFLARE, V4_CHINA] * 20)
        task.cancel()
        return ticks

    assert asyncio.run(scenario()) >= 1


def test_async_close_blocks_reuse() -> None:
    async def scenario() -> None:
        async_detector = await AsyncDetector.create(cache_size=0)
        await async_detector.lookup(V4_GOOGLE)
        await async_detector.aclose()
        with pytest.raises(RuntimeError):
            await async_detector.lookup(V4_GOOGLE)

    asyncio.run(scenario())


def test_windowed_batches_match_single_batch() -> None:
    """Windowing is a memory bound, not a behaviour change."""

    async def scenario() -> None:
        inputs = [V4_GOOGLE, V4_CLOUDFLARE, V4_CHINA, V4_ALIBABA, V6_GOOGLE] * 20
        async with await AsyncDetector.create(cache_size=0, window=7) as windowed:
            chunked = await windowed.lookup_many(inputs)
        async with await AsyncDetector.create(cache_size=0, window=10000) as whole:
            single = await whole.lookup_many(inputs)
        assert [item.to_dict() for item in chunked] == [item.to_dict() for item in single]

    asyncio.run(scenario())


def test_sharded_clients_agree_with_one_client(detector) -> None:
    """The documented scaling pattern: shard input, one client per worker."""
    inputs = [V4_GOOGLE, V4_CLOUDFLARE, V4_CHINA, V4_ALIBABA, V6_GOOGLE] * 6
    shards = [inputs[index::3] for index in range(3)]
    sharded = sorted(info.ip for shard in shards for info in detector.lookup_many(shard))
    assert sharded == sorted(info.ip for info in detector.lookup_many(inputs))
