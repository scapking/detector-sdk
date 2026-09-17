"""The two public coroutines and the async client underneath them."""

from __future__ import annotations

import asyncio
import time

import pytest

from detector import AsyncDetector, close, configure, distance, info

from .conftest import V4_ALIBABA, V4_CHINA, V4_CLOUDFLARE, V4_GOOGLE, V6_GOOGLE


async def _info(target, **kwargs):
    """The public default is JSON; these tests exercise the model objects."""
    return await info(target, as_object=True, **kwargs)


async def _distance(source, targets=None, **kwargs):
    return await distance(source, targets, as_object=True, **kwargs)


def test_info_single_and_batch() -> None:
    async def scenario() -> None:
        one = await _info(V4_GOOGLE)
        assert one.found is True
        assert one.country.iso_code == "US"

        many = await _info([V4_GOOGLE, V4_CHINA, "not-an-ip"])
        assert [row.ip for row in many] == [V4_GOOGLE, V4_CHINA, "not-an-ip"]
        assert [row.found for row in many] == [True, True, False]

        streamed = await _info(ip for ip in (V4_GOOGLE, V6_GOOGLE))
        assert [row.ip for row in streamed] == [V4_GOOGLE, V6_GOOGLE]

        # default output is the standard JSON document
        document = await info(V4_GOOGLE)
        assert isinstance(document, dict)
        assert document["country"]["iso_code"] == "US"
        assert isinstance(await info([V4_GOOGLE, V4_CHINA]), list)
        assert isinstance((await info([V4_GOOGLE]))[0], dict)

    asyncio.run(scenario())


def test_distance_single_pair_and_matrix() -> None:
    async def scenario() -> None:
        single = await _distance(V4_GOOGLE, V4_CLOUDFLARE)
        assert single.available and single.km > 1000

        one_to_n = await _distance(V4_GOOGLE, [V4_CLOUDFLARE, V4_ALIBABA])
        assert [row.target for row in one_to_n] == [V4_CLOUDFLARE, V4_ALIBABA]

        matrix = await _distance([V4_GOOGLE, V4_CHINA], [V4_CLOUDFLARE])
        assert [(row.source, row.target) for row in matrix] == [
            (V4_GOOGLE, V4_CLOUDFLARE),
            (V4_CHINA, V4_CLOUDFLARE),
        ]

        generator = await _distance(V4_GOOGLE, (ip for ip in [V4_CHINA, V6_GOOGLE]))
        assert len(generator) == 2

        document = await distance(V4_GOOGLE, V4_CLOUDFLARE)
        assert isinstance(document, dict)
        assert document["distance_km"] == single.km
        assert isinstance((await distance(V4_GOOGLE, [V4_CHINA]))[0], dict)

    asyncio.run(scenario())


def test_method_selection() -> None:
    async def scenario() -> None:
        fast = await _distance(V4_GOOGLE, V4_CHINA, method="haversine")
        exact = await _distance(V4_GOOGLE, V4_CHINA, method="vincenty")
        assert exact.method == "vincenty"
        assert abs(fast.km - exact.km) / exact.km < 0.01

    asyncio.run(scenario())


def test_options_per_call_and_via_configure() -> None:
    async def scenario() -> None:
        only_city = await _info(V4_GOOGLE, datasets=["dbip-city"], include_raw=False)
        assert only_city.country.iso_code == "US"
        assert only_city.asn is None          # no ASN dataset loaded
        assert only_city.raw == {}

        configure(locales=("en",), include_all_names=False)
        try:
            trimmed = await _info(V4_CHINA)
            assert trimmed.to_dict()["country"]["names"] == {"en": "China"}
        finally:
            configure()

    asyncio.run(scenario())


def test_clients_are_pooled_per_option_set() -> None:
    async def scenario() -> None:
        from detector.functions import _client

        first = await _client(include_raw=False)
        second = await _client(include_raw=False)
        third = await _client(include_raw=True)
        assert first is second
        assert first is not third
        await close()

    asyncio.run(scenario())


def test_async_client_matches_sync_client(detector) -> None:
    async def scenario() -> None:
        async with await AsyncDetector.create(cache_size=0) as client:
            assert (await client.lookup(V4_GOOGLE)).to_dict() == detector.lookup(V4_GOOGLE).to_dict()
            assert await client.describe() == detector.describe()

    asyncio.run(scenario())


def test_async_stream_is_lazy_and_order_preserving() -> None:
    async def scenario() -> None:
        async with await AsyncDetector.create(cache_size=0, window=2) as client:
            inputs = [V4_GOOGLE, V4_CLOUDFLARE, V4_CHINA, V6_GOOGLE, "bad"]
            results = await client.lookup_many(inputs)
            assert [row.ip for row in results] == inputs
            assert [row.found for row in results] == [True, True, True, True, False]

            streamed = [row.ip async for row in client.stream(inputs)]
            assert streamed == inputs

            async def source():
                for ip in inputs:
                    yield ip

            from_async_source = [row.ip async for row in client.stream(source())]
            assert from_async_source == inputs

    asyncio.run(scenario())


def test_async_does_not_block_the_loop() -> None:
    async def scenario() -> int:
        ticks = 0

        async def ticker() -> None:
            nonlocal ticks
            while True:
                await asyncio.sleep(0.01)
                ticks += 1

        task = asyncio.create_task(ticker())
        await _info([V4_GOOGLE, V4_CLOUDFLARE, V4_CHINA] * 20)
        task.cancel()
        return ticks

    assert asyncio.run(scenario()) >= 1


def test_async_close_blocks_reuse() -> None:
    async def scenario() -> None:
        client = await AsyncDetector.create(cache_size=0)
        await client.lookup(V4_GOOGLE)
        await client.aclose()
        with pytest.raises(RuntimeError):
            await client.lookup(V4_GOOGLE)

    asyncio.run(scenario())


def test_windowed_batches_match_single_batch() -> None:
    async def scenario() -> None:
        inputs = [V4_GOOGLE, V4_CLOUDFLARE, V4_CHINA, V4_ALIBABA, V6_GOOGLE] * 20
        async with await AsyncDetector.create(cache_size=0, window=7) as windowed:
            chunked = await windowed.lookup_many(inputs)
        async with await AsyncDetector.create(cache_size=0, window=10000) as whole:
            single = await whole.lookup_many(inputs)
        assert [row.to_dict() for row in chunked] == [row.to_dict() for row in single]

    asyncio.run(scenario())


def test_sharded_clients_agree_with_one_client(detector) -> None:
    inputs = [V4_GOOGLE, V4_CLOUDFLARE, V4_CHINA, V4_ALIBABA, V6_GOOGLE] * 6
    shards = [inputs[index::3] for index in range(3)]
    sharded = sorted(row.ip for shard in shards for row in detector.lookup_many(shard))
    assert sharded == sorted(row.ip for row in detector.lookup_many(inputs))


def test_pooled_client_is_reused_across_calls() -> None:
    async def scenario():
        started = time.perf_counter()
        await _info(V4_GOOGLE, datasets=["dbip-city"])
        first = time.perf_counter() - started
        started = time.perf_counter()
        for _ in range(50):
            await _info(V4_GOOGLE, datasets=["dbip-city"])
        return first, (time.perf_counter() - started) / 50

    first, subsequent = asyncio.run(scenario())
    assert subsequent < first + 0.01  # no repeated database opening
