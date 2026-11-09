
import asyncio
import time
import pytest

from fan_out import FanOut, Ok, Err


# Test 1: Parallel execution
@pytest.mark.asyncio
async def test_parallel_execution():

    async def process(task):
        await asyncio.sleep(0.1)
        return task * 2

    async def run(workers):
        fan_out = FanOut(process, workers)

        start = time.perf_counter()

        for task in range(4):
            await fan_out.submit(task)

        await fan_out.close()

        results = [
            item async for item in fan_out.results()
        ]

        elapsed = time.perf_counter() - start

        return elapsed, results

    time_one, results_one = await run(1)
    time_four, results_four = await run(4)

    assert time_four < time_one

    assert sorted(
        item.value for item in results_four
        if isinstance(item, Ok)
    ) == [0, 2, 4, 6]


# Test 2: No duplicate processing
@pytest.mark.asyncio
async def test_no_duplicate_processing():

    processed = []

    async def process(task):
        await asyncio.sleep(0.01)
        processed.append(task)
        return task

    fan_out = FanOut(process, workers=4)

    tasks = list(range(50))

    for task in tasks:
        await fan_out.submit(task)

    await fan_out.close()

    results = [
        item async for item in fan_out.results()
    ]

    assert len(processed) == 50
    assert sorted(processed) == tasks

    assert len(results) == 50


# Test 3: Graceful shutdown
@pytest.mark.asyncio
async def test_graceful_shutdown():

    processed = []

    async def process(task):
        await asyncio.sleep(0.01)
        processed.append(task)
        return task

    fan_out = FanOut(process, workers=3)

    for task in range(10):
        await fan_out.submit(task)

    await fan_out.close()

    results = [
        item async for item in fan_out.results()
    ]

    assert sorted(processed) == list(range(10))
    assert len(results) == 10

    with pytest.raises(RuntimeError):
        await fan_out.submit(100)


# Test 4: Error handling
@pytest.mark.asyncio
async def test_error_handling():

    async def process(task):
        if task == 2:
            raise ValueError("Invalid task")

        await asyncio.sleep(0.01)
        return task * 10

    fan_out = FanOut(process, workers=2)

    for task in [1, 2, 3]:
        await fan_out.submit(task)

    await fan_out.close()

    results = [
        item async for item in fan_out.results()
    ]

    successes = [
        item.value for item in results
        if isinstance(item, Ok)
    ]

    errors = [
        item.error for item in results
        if isinstance(item, Err)
    ]

    assert sorted(successes) == [10, 30]
    assert len(errors) == 1
    assert isinstance(errors[0], ValueError)
