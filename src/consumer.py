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
                
                # Event di queue sudah pasti unik dan sudah ditandai di /publish
                # Kita hanya perlu simpan detail event ke tabel events
                await asyncio.to_thread(
                    self.dedup.insert_event_record, 
                    topic, event_id, ts, source, payload
                )
                
                self.stats['unique_processed'] += 1
                self.stats['topics'].add(topic)
                
                logger.debug(f"Processed: topic={topic} event_id={event_id}")
                
            except Exception as e:
                logger.exception("Error processing event: %s", e)
            finally:
                self.queue.task_done()
    
    def stop(self):
        self._running = False