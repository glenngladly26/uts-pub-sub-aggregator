import os
import asyncio
import logging
import json
from contextlib import asynccontextmanager
from fastapi import FastAPI, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from .models import Event, PublishBatch
from .dedup_store import DedupStore
from .consumer import ConsumerWorker
from .util import iso_now

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("aggregator")

# Lokasi default database
DEDUP_DB = os.getenv("DEDUP_DB", "./data/dedup.db")
queue: asyncio.Queue = asyncio.Queue(maxsize=10000)


def make_app(db_path: str = DEDUP_DB) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        dedup = DedupStore(db_path)
        stats = {
            "received": 0,
            "unique_processed": 0,
            "duplicate_dropped": 0,
            "topics": set(),
            "start_time": iso_now(),
        }

        worker = ConsumerWorker(queue, dedup, stats)
        worker_task = asyncio.create_task(worker.start())

        app.state.dedup = dedup
        app.state.stats = stats
        app.state.worker_task = worker_task

        try:
            yield
        finally:
            worker.stop()
            worker_task.cancel()
            await asyncio.sleep(0.1)
            dedup.close()

    app = FastAPI(title="UTS Pub-Sub Aggregator", lifespan=lifespan)

    # Middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # -------------------------------
    # Endpoint: Publish Event
    # -------------------------------
    @app.post("/publish")
    async def publish_single(evt: Event | PublishBatch, request: Request):
        body = await request.json()
        events = []

        # Tentukan apakah single event atau batch
        if isinstance(body, list):
            events = [Event(**e) for e in body]
        elif "topic" in body:
            events.append(Event(**body))
        else:
            events = PublishBatch(**body).__root__

        stats = app.state.stats
        dedup = app.state.dedup
        accepted = 0

        for e in events:
            stats["received"] += 1

            # Cek dedup (gunakan add_if_new)
            is_new = await asyncio.to_thread(
                dedup.add_if_new, e.topic, e.event_id, iso_now()
            )

            if is_new:
                await queue.put(e.model_dump())
                accepted += 1
                logger.info(f"Accepted new event: topic={e.topic}, id={e.event_id}")
            else:
                stats["duplicate_dropped"] += 1
                logger.info(f"Duplicate rejected: topic={e.topic}, id={e.event_id}")

        return {"accepted": accepted}

    # -------------------------------
    # Endpoint: Get Events
    # -------------------------------
    @app.get("/events")
    async def get_events(topic: str = Query(None)):
        ded = app.state.dedup
        rows = await asyncio.to_thread(ded.list_events, topic)
        return [
            {
                "topic": r[0],
                "event_id": r[1],
                "timestamp": r[2],
                "source": r[3],
                "payload": r[4],
            }
            for r in rows
        ]

    # -------------------------------
    # Endpoint: Get Stats
    # -------------------------------
    @app.get("/stats")
    async def get_stats():
        s = app.state.stats
        return {
            "received": s["received"],
            "unique_processed": s["unique_processed"],
            "duplicate_dropped": s["duplicate_dropped"],
            "topics": list(s["topics"]),
            "uptime": s["start_time"],
        }
    
    @app.post("/_flush")
    async def flush_queue():
        """Endpoint internal hanya untuk testing — proses semua event yang belum diproses."""
        dedup = app.state.dedup
        stats = app.state.stats
        processed = 0

        # Proses event yang masih tersisa di queue
        while not queue.empty():
            e = await queue.get()
            await asyncio.to_thread(dedup.insert_event, e["topic"], e["event_id"], e["timestamp"], e["source"], json.dumps(e["payload"]))
            stats["unique_processed"] += 1
            stats["topics"].add(e["topic"])
            processed += 1

        return {"flushed": processed}


    return app

# Entry point
app = make_app()
