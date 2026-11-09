
import asyncio
import inspect
from typing import Any, Callable, Generic, TypeVar

T = TypeVar("T")
R = TypeVar("R")


class Future(Generic[T]):
    def __init__(self):
        self._future = asyncio.get_running_loop().create_future()

    @property
    def done(self) -> bool:
        return self._future.done()

    def _resolve(self, value: T):
        """Set the result only once."""
        if not self._future.done():
            self._future.set_result(value)

    def _reject(self, error: Exception):
        """Set the exception only once."""
        if not self._future.done():
            self._future.set_exception(error)

    async def get(self, timeout: float | None = None) -> T:
        """Wait for the result, optionally with a timeout."""
        return await asyncio.wait_for(
            asyncio.shield(self._future),
            timeout=timeout
        )

    def __await__(self):
        return self.get().__await__()

    def then(
        self,
        on_fulfilled: Callable[[T], Any] | None = None
    ) -> "Future":
        """Register a callback for successful completion."""
        chained = Future()

        async def handle():
            try:
                value = await self.get()

                if on_fulfilled is None:
                    chained._resolve(value)
                    return

                result = on_fulfilled(value)

                if inspect.isawaitable(result):
                    result = await result

                chained._resolve(result)

            except Exception as exc:
                chained._reject(exc)

        asyncio.create_task(handle())
        return chained

    def catch(
        self,
        on_error: Callable[[Exception], Any]
    ) -> "Future":
        """Register a callback for errors."""
        chained = Future()

        async def handle():
            try:
                value = await self.get()
                chained._resolve(value)

            except Exception as exc:
                try:
                    result = on_error(exc)

                    if inspect.isawaitable(result):
                        result = await result

                    chained._resolve(result)

                except Exception as handler_error:
                    chained._reject(handler_error)

        asyncio.create_task(handle())
        return chained

    def on_error(
        self,
        on_error: Callable[[Exception], Any]
    ) -> "Future":
        return self.catch(on_error)


def submit(fn: Callable[[], Any]) -> Future:
    """Execute a function and return its Future."""
    future = Future()

    async def execute():
        try:
            result = fn()

            if inspect.isawaitable(result):
                result = await result

            future._resolve(result)

        except Exception as exc:
            future._reject(exc)

    asyncio.create_task(execute())
    return future


async def all(futures: list[Future]) -> Future:
    """Resolve after all Futures have completed."""
    result_future = Future()

    async def collect():
        results = await asyncio.gather(
            *(future.get() for future in futures),
            return_exceptions=True
        )

        errors = [
            item for item in results
            if isinstance(item, BaseException)
        ]

        if errors:
            result_future._reject(errors[0])
        else:
            result_future._resolve(results)

    asyncio.create_task(collect())
    return result_future


async def race(futures: list[Future]) -> Future:
    """Resolve or reject with the first completed Future."""
    if not futures:
        raise ValueError("race() requires at least one Future")

    result_future = Future()

    async def wait_for_first(future: Future):
        try:
            result = await future.get()
            result_future._resolve(result)
        except Exception as exc:
            result_future._reject(exc)

    for future in futures:
        asyncio.create_task(wait_for_first(future))

    return result_future
