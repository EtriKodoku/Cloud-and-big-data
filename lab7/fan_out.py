
import asyncio
import inspect
from dataclasses import dataclass
from typing import Any, Callable, Generic, TypeVar, Union, AsyncIterator

T = TypeVar("T")
R = TypeVar("R")


@dataclass
class Ok(Generic[R]):
    value: R


@dataclass
class Err:
    error: Exception


class _WorkerFinished:
    pass


_STOP = object()


class FanOut(Generic[T, R]):

    def __init__(self, process: Callable[[T], Any], workers: int):
        if workers < 1:
            raise ValueError("workers must be at least 1")

        self.process = process
        self.worker_count = workers

        self.queue = asyncio.Queue()
        self.result_queue = asyncio.Queue()

        self.worker_tasks = []
        self.started = False
        self.closed = False
        self.results_consumed = False

    async def start(self):
        """Start all workers."""
        if self.closed:
            raise RuntimeError("FanOut is closed")

        if not self.started:
            self.started = True

            self.worker_tasks = [
                asyncio.create_task(self.worker())
                for _ in range(self.worker_count)
            ]

    async def submit(self, task: T):
        """Add a task to the queue."""
        if self.closed:
            raise RuntimeError("Cannot submit after close()")

        await self.start()
        await self.queue.put(task)

    async def worker(self):
        """Worker that processes tasks from the queue."""
        try:
            while True:
                task = await self.queue.get()

                try:
                    if task is _STOP:
                        return

                    try:
                        result = self.process(task)

                        if inspect.isawaitable(result):
                            result = await result

                        await self.result_queue.put(Ok(result))

                    except Exception as exc:
                        await self.result_queue.put(Err(exc))

                finally:
                    self.queue.task_done()

        finally:
            await self.result_queue.put(_WorkerFinished())

    async def close(self):
        """
        Graceful shutdown:
        wait for queued tasks and stop all workers.
        """
        if self.closed:
            return

        self.closed = True

        if not self.started:
            return

        # Send stop signals after all submitted tasks.
        for _ in range(self.worker_count):
            await self.queue.put(_STOP)

        # Wait until all tasks and stop signals are processed.
        await self.queue.join()

        await asyncio.gather(*self.worker_tasks)

    async def results(self) -> AsyncIterator[Union[Ok[R], Err]]:
        """Yield results as soon as workers finish processing."""

        if self.results_consumed:
            raise RuntimeError("results() can only be consumed once")

        self.results_consumed = True

        if not self.started:
            return

        finished_workers = 0

        while finished_workers < self.worker_count:
            item = await self.result_queue.get()

            if isinstance(item, _WorkerFinished):
                finished_workers += 1
            else:
                yield item
