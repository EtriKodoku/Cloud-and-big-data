import asyncio
from functools import wraps
from typing import Optional


class Debounce:
    def __init__(self, delay_ms: float, leading: bool = False, trailing: bool = True):
        if not leading and not trailing:
            raise ValueError("Хоча б одна з опцій leading/trailing має бути True")

        self.delay = delay_ms / 1000.0
        self.leading = leading
        self.trailing = trailing

        self._task: Optional[asyncio.Task] = None
        self._last_args = ()
        self._last_kwargs = {}
        self._invoked_on_leading = False
        self._calls_in_burst = 0

    def debounce(self, fn):

        @wraps(fn)
        async def wrapper(*args, **kwargs):
            self._last_args = args
            self._last_kwargs = kwargs

            is_new_burst = self._task is None or self._task.done()

            if self._task is not None and not self._task.done():
                self._task.cancel()

            if is_new_burst:
                self._invoked_on_leading = False
                self._calls_in_burst = 0
                if self.leading:
                    self._invoked_on_leading = True
                    await fn(*args, **kwargs)

            self._calls_in_burst += 1
            self._task = asyncio.create_task(self._wait_and_call(fn))

        return wrapper

    async def _wait_and_call(self, fn):
        try:
            await asyncio.sleep(self.delay)
        except asyncio.CancelledError:
            return


        only_leading_call = self._invoked_on_leading and self._calls_in_burst == 1
        should_call_trailing = self.trailing and not only_leading_call

        args, kwargs = self._last_args, self._last_kwargs
        self._invoked_on_leading = False
        self._calls_in_burst = 0

        if should_call_trailing:
            await fn(*args, **kwargs)

    def dispose(self):
        if self._task is not None and not self._task.done():
            self._task.cancel()


async def main():
    print("=" * 5 + "trailing = true" + "=" * 5)
    d = Debounce(delay_ms=1000, trailing=True)

    async def search(msg):
        print("виконано:", msg)
        return "Результат: " + msg

    wrapped_search = d.debounce(search)

    for msg in ["a", "ap", "app", "appl"]:
        await wrapped_search(msg)
        await asyncio.sleep(0.1)

    await asyncio.sleep(1.2)

    await wrapped_search("apple")
    await asyncio.sleep(1.2)

    print("=" * 5 + "leading = true" + "=" * 5)
    d = Debounce(delay_ms=1000, leading=True, trailing= False)

    wrapped_search = d.debounce(search)

    for msg in ["a", "ap", "app", "appl"]:
        await wrapped_search(msg)
        await asyncio.sleep(0.1)

    await asyncio.sleep(1.2)

    await wrapped_search("apple")
    await asyncio.sleep(1.2)

    print("=" * 5 + "trailing = true, leading = true" + "=" * 5)
    d = Debounce(delay_ms=1000, leading=True)

    wrapped_search = d.debounce(search)

    for msg in ["a", "ap", "app", "appl"]:
        await wrapped_search(msg)
        await asyncio.sleep(0.1)

    await asyncio.sleep(1.2)

    await wrapped_search("apple")
    await asyncio.sleep(1.2)


if __name__ == "__main__":
    asyncio.run(main())
