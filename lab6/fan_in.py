import asyncio
from dataclasses import dataclass
from typing import AsyncIterable, AsyncIterator, Generic, TypeVar, Union

T = TypeVar("T")


@dataclass
class Ok(Generic[T]):
    value: T


@dataclass
class Err:
    error: Exception


Result = Union[Ok[T], Err]


async def fanIn(inputs: list[AsyncIterable[T]]) -> AsyncIterator[Result[T]]:
    """
    Merge multiple asynchronous input streams into one stream.

    Each input is consumed independently. Errors from an input are yielded
    as Err values, and do not prevent other inputs from continuing.
    The output ends when all inputs have ended.
    """
    queue: asyncio.Queue[tuple[str, object]] = asyncio.Queue()

    async def consume(stream: AsyncIterable[T]) -> None:
        try:
            async for item in stream:
                await queue.put(("item", Ok(item)))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await queue.put(("item", Err(exc)))
        finally:
            await queue.put(("done", None))

    tasks = [asyncio.create_task(consume(stream)) for stream in inputs]
    remaining = len(tasks)

    try:
        while remaining:
            kind, value = await queue.get()
            if kind == "done":
                remaining -= 1
            else:
                yield value  # type: ignore[misc]
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
