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

DEDUP_DB = os.getenv("DEDUP_DB", "./data/dedup.db")
queue: asyncio.Queue = asyncio.Queue(maxsize=10000)

@asynccontextmanager
async def lifespan(app: FastAPI):
    dedup = DedupStore(DEDUP_DB)
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

def make_app(db_path: str = DEDUP_DB) -> FastAPI:
    app = FastAPI(title="UTS Pub-Sub Aggregator", lifespan=lifespan)
    
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )
    
    @app.post("/publish")
    async def publish_single(evt: Event | PublishBatch, request: Request):
        body = await request.json()
        events = []
        
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
            stats['received'] += 1
            
            # Cek apakah event sudah pernah diproses (dedup check)
            is_new = await asyncio.to_thread(
                dedup.is_duplicate, e.topic, e.event_id
            )
            
            if is_new:
                # Event baru, masukkan ke queue untuk diproses consumer
                await queue.put(e.model_dump())
                accepted += 1
            else:
                # Event duplikat, langsung update stats
                stats['duplicate_dropped'] += 1
                logger.info(f"Duplicate rejected: topic={e.topic} event_id={e.event_id}")
        
        return {"accepted": accepted}
    
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
    
    return app

app = make_app()