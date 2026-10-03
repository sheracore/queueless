import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.routes import businesses, queues, queue_entries
from app.events.publisher import get_publisher

logging.basicConfig(level=logging.INFO)


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    # Shutdown: send whatever is still in the producer's buffer, or it is lost.
    if get_publisher.cache_info().currsize:
        get_publisher().flush()


app = FastAPI(
    title="QueueLess Queue Service",
    version="0.1.0",
    lifespan=lifespan,
)


app.include_router(businesses.router)
app.include_router(queues.router)
app.include_router(queue_entries.router)


@app.get("/health")
def health():
    return {"status": "ok"}