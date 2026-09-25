from fastapi import FastAPI, HTTPException
from middleware import CircuitBreakerMiddleware
from pydantic import BaseModel
from redis.exceptions import RedisError

from redis_client import redis_client

app = FastAPI()

app.add_middleware(
    CircuitBreakerMiddleware,
    failure_threshold=3,
    recovery_timeout=30,
)


class StoreRequest(BaseModel):
    data: str


@app.post("/store")
async def store(request: StoreRequest):
    try:
        await redis_client.set("data", request.data)

        return {
            "message": "Data stored successfully",
            "data": request.data,
        }

    except RedisError:
        raise HTTPException(
            status_code=500,
            detail="Redis is unavailable",
        )
