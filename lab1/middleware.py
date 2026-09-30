import logging
import time

from fastapi import Request
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

# Configure the logging format and level
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler()  # Outputs to the console
    ]
)

# Create a logger for this specific module
logger = logging.getLogger(__name__)

class CircuitBreakerMiddleware(BaseHTTPMiddleware):
    def __init__(
        self,
        app,
        failure_threshold: int = 3,
        recovery_timeout: int = 30,
    ):
        super().__init__(app)

        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout

        self.failure_count = 0
        self.last_failure_time = None
        self.state = "CLOSED"

    async def dispatch(self, request: Request, call_next):
        if self.state == "OPEN":
            if (
                self.last_failure_time
                and time.time() - self.last_failure_time
                >= self.recovery_timeout
            ):
                self.state = "HALF_OPEN"
                logger.info("Circuit breaker is in HALF_OPEN state")
            else:
                logger.info("Circuit breaker is in OPEN state")
                return JSONResponse(
                    status_code=503,
                    content={
                        "error": "Service unavailable",
                        "circuit": "OPEN",
                    },
                )

        try:
            response = await call_next(request)

            # Treat 5xx responses as failures
            if response.status_code >= 500:
                self._record_failure()
            else:
                self._record_success()

            return response

        except Exception:
            self._record_failure()
            raise

    def _record_failure(self):
        self.failure_count += 1
        self.last_failure_time = time.time()

        if self.failure_count >= self.failure_threshold:
            self.state = "OPEN"

    def _record_success(self):
        self.failure_count = 0
        self.last_failure_time = None
        self.state = "CLOSED"
