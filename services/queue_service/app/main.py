import logging

from fastapi import FastAPI

from app.api.routes import businesses, queues, queue_entries

logging.basicConfig(level=logging.INFO)


app = FastAPI(
    title="QueueLess Queue Service",
    version="0.1.0",
)


app.include_router(businesses.router)
app.include_router(queues.router)
app.include_router(queue_entries.router)


@app.get("/health")
def health():
    return {"status": "ok"}
