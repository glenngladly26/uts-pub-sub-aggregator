import asyncio
import json
import logging
from datetime import datetime
from .dedup_store import DedupStore

logger = logging.getLogger("aggregator.consumer")

class ConsumerWorker:
    def __init__(self, queue: asyncio.Queue, dedup: DedupStore, stats: dict):
        self.queue = queue
        self.dedup = dedup
        self.stats = stats
        self._running = False
    
    async def start(self):
        self._running = True
        processed_count = 0

        while self._running:
            try:
                evt = await self.queue.get()
            except asyncio.CancelledError:
                break

            try:
                topic = evt['topic']
                event_id = evt['event_id']
                ts = evt['timestamp']
                source = evt.get('source')
                payload = json.dumps(evt.get('payload', {}))
                
                # Event di queue sudah pasti unik (sudah dicek di /publish)
                await asyncio.to_thread(
                    self.dedup.insert_event_record, 
                    topic, event_id, ts, source, payload
                )

                # Update statistik di memori
                self.stats["unique_processed"] += 1
                self.stats["topics"].add(topic)
                processed_count += 1

                # Logging ringan tiap 100 event
                if processed_count % 100 == 0:
                    logger.info(f"Processed {processed_count} unique events so far...")

            except Exception as e:
                logger.exception("⚠️ Error processing event: %s", e)
            finally:
                self.queue.task_done()

        logger.info("ConsumerWorker stopped cleanly.")

    def stop(self):
        self._running = False
