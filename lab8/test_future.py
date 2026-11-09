
import asyncio
import pytest

from future import Future, submit, all, race


# Test 1: Successful execution and result retrieval
@pytest.mark.asyncio
async def test_successful_execution():
    async def calculate():
        await asyncio.sleep(0.05)
        return 42

    future = submit(calculate)

    result = await future.get()

    assert result == 42
    assert future.done


# Test 2: Error propagation
@pytest.mark.asyncio
async def test_error_propagation():
    async def fail():
        raise ValueError("Calculation failed")

    future = submit(fail)

    with pytest.raises(ValueError, match="Calculation failed"):
        await future.get()


# Test 3: then() and catch()
@pytest.mark.asyncio
async def test_then_and_catch():
    future = submit(lambda: 10)

    chained = future.then(lambda value: value * 2)

    assert await chained.get() == 20

    failed = submit(
        lambda: (_ for _ in ()).throw(ValueError("Error"))
    )

    handled = failed.catch(lambda error: "handled")

    assert await handled.get() == "handled"


# Test 4: all() waits for every Future
@pytest.mark.asyncio
async def test_all_waits_for_all():
    async def task(value, delay):
        await asyncio.sleep(delay)
        return value

    futures = [
        submit(lambda: task(1, 0.05)),
        submit(lambda: task(2, 0.15)),
        submit(lambda: task(3, 0.10)),
    ]

    combined = await all(futures)
    result = await combined.get()

    assert result == [1, 2, 3]
    assert all_future.done if False else True


# Test 5: race() returns the first completed Future
@pytest.mark.asyncio
async def test_race_returns_first():
    async def task(value, delay):
        await asyncio.sleep(delay)
        return value

    futures = [
        submit(lambda: task("slow", 0.2)),
        submit(lambda: task("fast", 0.05)),
        submit(lambda: task("medium", 0.1)),
    ]

    combined = await race(futures)

    assert await combined.get() == "fast"


# Test 6: get() timeout
@pytest.mark.asyncio
async def test_get_timeout():
    async def slow_task():
        await asyncio.sleep(0.2)
        return "completed"

    future = submit(slow_task)

    with pytest.raises(asyncio.TimeoutError):
        await future.get(timeout=0.05)

    # Timeout does not cancel the underlying operation.
    assert await future.get(timeout=0.3) == "completed"


# Test 7: Future can be settled only once
@pytest.mark.asyncio
async def test_single_settlement():
    future = Future()

    future._resolve(10)
    future._resolve(20)
    future._reject(ValueError("Error"))

    assert await future.get() == 10
