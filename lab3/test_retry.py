from unittest.mock import AsyncMock

import pytest

from retry import Retry, BackoffStrategy


@pytest.mark.asyncio
async def test_success_on_first_attempt():
    fn = AsyncMock(return_value="success")

    retry = Retry(
        max_attempts=5,
        delay_ms=10,
        strategy=BackoffStrategy.CONSTANT,
    )

    result = await retry.execute(fn)

    assert result == "success"
    assert fn.await_count == 1


@pytest.mark.asyncio
async def test_success_after_multiple_attempts():
    fn = AsyncMock(
        side_effect=[
            ConnectionError("temporary error"),
            ConnectionError("temporary error"),
            "success",
        ]
    )

    retry = Retry(
        max_attempts=5,
        delay_ms=0,
        strategy=BackoffStrategy.CONSTANT,
        retry_on=[ConnectionError],
    )

    result = await retry.execute(fn)

    assert result == "success"
    assert fn.await_count == 3


@pytest.mark.asyncio
async def test_stop_after_max_attempts():
    error = ConnectionError("connection failed")

    fn = AsyncMock(side_effect=error)

    retry = Retry(
        max_attempts=3,
        delay_ms=0,
        strategy=BackoffStrategy.CONSTANT,
        retry_on=[ConnectionError],
    )

    with pytest.raises(ConnectionError):
        await retry.execute(fn)

    assert fn.await_count == 3


@pytest.mark.asyncio
async def test_non_retryable_error():
    error = ValueError("invalid value")

    fn = AsyncMock(side_effect=error)

    retry = Retry(
        max_attempts=5,
        delay_ms=0,
        strategy=BackoffStrategy.CONSTANT,
        retry_on=[ConnectionError],
    )

    with pytest.raises(ValueError):
        await retry.execute(fn)

    # Помилка не є retryable,
    # тому функція викликається лише один раз
    assert fn.await_count == 1


@pytest.mark.asyncio
async def test_constant_delay(monkeypatch):
    fn = AsyncMock(
        side_effect=[
            ConnectionError(),
            ConnectionError(),
            "success",
        ]
    )

    sleep_mock = AsyncMock()
    monkeypatch.setattr("retry.asyncio.sleep", sleep_mock)

    retry = Retry(
        max_attempts=5,
        delay_ms=100,
        strategy=BackoffStrategy.CONSTANT,
        retry_on=[ConnectionError],
    )

    await retry.execute(fn)

    assert sleep_mock.await_count == 2
    sleep_mock.assert_any_await(0.1)


@pytest.mark.asyncio
async def test_exponential_delay(monkeypatch):
    fn = AsyncMock(
        side_effect=[
            ConnectionError(),
            ConnectionError(),
            ConnectionError(),
            "success",
        ]
    )

    sleep_mock = AsyncMock()
    monkeypatch.setattr("retry.asyncio.sleep", sleep_mock)

    retry = Retry(
        max_attempts=5,
        delay_ms=100,
        strategy=BackoffStrategy.EXPONENTIAL,
        retry_on=[ConnectionError],
    )

    await retry.execute(fn)

    assert sleep_mock.await_args_list == [
        ((0.1,),),
        ((0.2,),),
        ((0.4,),),
    ]


@pytest.mark.asyncio
async def test_exponential_jitter(monkeypatch):
    fn = AsyncMock(
        side_effect=[
            ConnectionError(),
            ConnectionError(),
            "success",
        ]
    )

    sleep_mock = AsyncMock()
    monkeypatch.setattr("retry.asyncio.sleep", sleep_mock)

    # Фіксуємо random.uniform для передбачуваного тесту
    monkeypatch.setattr(
        "retry.random.uniform",
        lambda min_value, max_value: max_value / 2,
    )

    retry = Retry(
        max_attempts=5,
        delay_ms=100,
        strategy=BackoffStrategy.EXPONENTIAL_JITTER,
        retry_on=[ConnectionError],
    )

    await retry.execute(fn)

    # Для першої спроби:
    # exponential delay = 100 ms
    # jitter = 50 ms
    #
    # Для другої:
    # exponential delay = 200 ms
    # jitter = 100 ms

    assert sleep_mock.await_args_list == [
        ((0.05,),),
        ((0.1,),),
    ]