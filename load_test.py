import aiohttp
import asyncio
import random
import time
from datetime import datetime

API_URL = "http://localhost:8080/publish"
NUM_EVENTS = 5000
DUPLICATE_RATE = 0.2  
CONCURRENCY = 200     

event_ids = [f"evt-{i}" for i in range(NUM_EVENTS)]
duplicate_ids = random.sample(event_ids, int(NUM_EVENTS * DUPLICATE_RATE))
event_ids.extend(duplicate_ids)
random.shuffle(event_ids)


async def send_event(session: aiohttp.ClientSession, eid: str) -> bool:
    payload = {
        "topic": "load-test",
        "event_id": eid,
        "timestamp": datetime.utcnow().isoformat(),
        "source": "load_tester",
        "payload": {"value": random.randint(1, 100)}
    }

    try:
        async with session.post(API_URL, json=payload, timeout=aiohttp.ClientTimeout(total=5)) as resp:
            return resp.status == 200
    except Exception as e:
        print(f"⚠️ Error {eid}: {e}")
        return False


async def run_load_test():
    success_count = 0
    start_time = time.time()

    connector = aiohttp.TCPConnector(limit=CONCURRENCY)
    async with aiohttp.ClientSession(connector=connector) as session:
        semaphore = asyncio.Semaphore(CONCURRENCY)

        async def bounded_send(eid):
            async with semaphore:
                return await send_event(session, eid)

        tasks = [bounded_send(eid) for eid in event_ids]
        results = await asyncio.gather(*tasks)

        success_count = sum(results)

    duration = time.time() - start_time
    print(f"✅ Sukses kirim {success_count} event dalam {duration:.2f} detik")
    print(f"Rata-rata: {duration/len(event_ids):.4f} detik/event")


if __name__ == "__main__":
    asyncio.run(run_load_test())
