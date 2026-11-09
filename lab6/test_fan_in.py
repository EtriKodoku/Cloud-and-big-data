import asyncio

import pytest

from fan_in import Err, Ok, fanIn


async def stream(*items, delay=0):
    for item in items:
        if delay:
            await asyncio.sleep(delay)
        yield item


async def failing_stream():
    yield 1
    raise ValueError("source failed")


@pytest.mark.asyncio
async def test_merge_two_streams_preserves_each_stream_order():
    async def left():
        yield "a1"
        await asyncio.sleep(0.01)
        yield "a2"

    async def right():
        await asyncio.sleep(0)
        yield "b1"
        yield "b2"

    results = [item async for item in fanIn([left(), right()])]
    values = [item.value for item in results if isinstance(item, Ok)]

    assert set(values) == {"a1", "a2", "b1", "b2"}
    assert values.index("a1") < values.index("a2")
    assert values.index("b1") < values.index("b2")


@pytest.mark.asyncio
async def test_merge_three_streams():
    results = [item async for item in fanIn([
        stream(1, 2),
        stream(3, 4),
        stream(5, 6),
    ])]

    values = [item.value for item in results if isinstance(item, Ok)]
    assert sorted(values) == [1, 2, 3, 4, 5, 6]


@pytest.mark.asyncio
async def test_output_finishes_after_all_inputs_finish():
    results = [item async for item in fanIn([
        stream("fast"),
        stream("slow", delay=0.02),
    ])]

    values = [item.value for item in results if isinstance(item, Ok)]
    assert sorted(values) == ["fast", "slow"]


@pytest.mark.asyncio
async def test_error_in_one_input_does_not_stop_other_inputs():
    results = [item async for item in fanIn([
        failing_stream(),
        stream(10, 20, 30),
    ])]

    values = [item.value for item in results if isinstance(item, Ok)]
    errors = [item.error for item in results if isinstance(item, Err)]

    assert 1 in values
    assert 10 in values
    assert 20 in values
    assert 30 in values
    assert len(errors) == 1
    assert isinstance(errors[0], ValueError)
    assert str(errors[0]) == "source failed"
