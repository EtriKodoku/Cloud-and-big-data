import asyncio
import logging
import time
from functools import wraps

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger("Throttle")


class TooManyRequest(Exception):
    pass


class Throttle:
    def __init__(self, leading=False, trailing=True, 
                 rate=None, capacity=None, refillRate=None):
        self.leading = leading
        self.trailing = trailing
        self.rate = rate
        self.last_call = time.monotonic()

        if self.rate is None:
            self.capacity = float(capacity)
            self.refill_rate = float(refillRate)
            self.tokens = float(capacity)
            logger.info(f"Ініціалізація TokenBucket: capacity={self.capacity}, refill_rate={self.refill_rate}/s, leading={self.leading}, trailing={self.trailing}")
        else:
            self.rate_limit, self.rate_interval = rate
            self.calls_history = []
            logger.info(f"Ініціалізація SlidingWindow: limit={self.rate_limit}, interval={self.rate_interval}s, leading={self.leading}, trailing={self.trailing}")

        self.last_args = None
        self.trailing_task = None

    def throttle(self, fn):
        @wraps(fn)
        async def wrapper(*args, **kwargs):
            now = time.monotonic()
            allowed = False
            wait_time = 0.0

            logger.debug(f"Вхідний виклик {fn.__name__} з args={args}, kwargs={kwargs}")

            if self.rate is None:
                elapsed = now - self.last_call
                self.last_call = now
                added_tokens = elapsed * self.refill_rate
                self.tokens = min(self.capacity, self.tokens + added_tokens)
                logger.debug(f"TokenBucket: минуло {elapsed:.4f}s (+{added_tokens:.2f} токенів). Поточний баланс: {self.tokens:.2f}/{self.capacity}")

                if self.tokens >= 1.0:
                    self.tokens -= 1.0
                    allowed = True
                    logger.info(f"TokenBucket: слот отримано. Залишок токенів: {self.tokens:.2f}")
                else:
                    wait_time = (1.0 - self.tokens) / self.refill_rate
                    logger.warning(f"TokenBucket: недостатньо токенів ({self.tokens:.2f} < 1.0). Час до поповнення: {wait_time:.4f}s")
            else:
                self.calls_history = [
                    t for t in self.calls_history if now - t < self.rate_interval
                ]
                active_calls = len(self.calls_history)
                logger.debug(f"SlidingWindow: активних викликів у вікні: {active_calls}/{self.rate_limit}")

                if active_calls < self.rate_limit:
                    self.calls_history.append(now)
                    allowed = True
                    logger.info(f"SlidingWindow: слот отримано ({active_calls + 1}/{self.rate_limit})")
                else:
                    wait_time = self.rate_interval - (now - self.calls_history[0])
                    logger.warning(f"SlidingWindow: ліміт вичерпано. Звільнення слота через: {wait_time:.4f}s")

            if allowed:
                if self.leading:
                    logger.info(f"Leading: негайне виконання {fn.__name__}")
                    return await fn(*args, **kwargs)
                logger.info(f"Leading вимкнено: виконання відкладається на кінець інтервалу")

            if self.trailing:
                self.last_args = (args, kwargs)
                logger.debug(f"Trailing: оновлено останні аргументи для відкладеного виклику")

                async def delayed_call():
                    delay = wait_time if wait_time > 0 else (1.0 / self.refill_rate if self.rate is None else self.rate_interval)
                    logger.info(f"Trailing: фоновий таймер запущено на {delay:.4f}s")
                    await asyncio.sleep(delay)

                    if self.last_args is not None:
                        latest_args, latest_kwargs = self.last_args
                        self.last_args = None
                        self.trailing_task = None
                        logger.info(f"Trailing: виконання відкладеного виклику {fn.__name__}")
                        await fn(*latest_args, **latest_kwargs)
                    else:
                        self.trailing_task = None

                if self.trailing_task is None:
                    self.trailing_task = asyncio.create_task(delayed_call())
                else:
                    logger.debug(f"Trailing: фоновий таймер уже активний, перезаписано лише аргументи")
                return None

            logger.error(f"Виклик відхилено: ліміт вичерпано, а trailing вимкнено")
            raise TooManyRequest("Rate limit exceeded")

        return wrapper