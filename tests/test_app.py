import os
import time
import json
import tempfile
import shutil
import pytest
from fastapi.testclient import TestClient
from src.main import make_app
from src.util import iso_now


@pytest.fixture()
def client():
    """Setiap test menggunakan DB baru (tidak saling ganggu)."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "dedup.db")
        app = make_app(db_path=db_path)
        with TestClient(app) as c:
            _ = c.get("/stats")  # trigger inisialisasi lifespan
            yield c


def make_event(event_id: str, topic: str = "pytest-topic"):
    return {
        "topic": topic,
        "event_id": event_id,
        "timestamp": iso_now(),
        "source": "pytest",
        "payload": {"key": f"value-{event_id}"},
    }

def wait_for_events(client, expected_count, topic=None, timeout=3.0):
    """Tunggu hingga jumlah event sesuai harapan atau timeout."""
    start = time.time()
    while time.time() - start < timeout:
        params = {}
        if topic:
            params["topic"] = topic
        resp = client.get("/events", params=params)
        data = resp.json()
        if len(data) >= expected_count:
            return data
        time.sleep(0.1)
    return []


# 1️⃣ Deduplication behavior
def test_dedup_behavior(client):
    evt = make_event("dup-1", topic="dedup-test")

    r1 = client.post("/publish", json=evt)
    r2 = client.post("/publish", json=evt)

    assert r1.status_code == 200
    assert r2.status_code == 200
    assert r1.json()["accepted"] == 1  # pertama diterima
    assert r2.json()["accepted"] == 0  # kedua ditolak (duplikat)

    resp = client.get("/events?topic=dedup-test")
    data = resp.json()
    assert len(data) == 1
    assert data[0]["event_id"] == "dup-1"


# 2️⃣ Persistensi dedup store (simulasi restart)
def test_dedup_persistence():
    tmpdir = tempfile.mkdtemp()
    db_path = os.path.join(tmpdir, "dedup.db")

    # Jalankan app pertama (dengan lifespan aktif)
    app1 = make_app(db_path=db_path)
    with TestClient(app1) as client1:
        evt = make_event("persist-1")
        r1 = client1.post("/publish", json=evt)
        assert r1.json()["accepted"] == 1

    # Restart app kedua, masih pakai DB yang sama
    app2 = make_app(db_path=db_path)
    with TestClient(app2) as client2:
        r2 = client2.post("/publish", json=evt)
        assert r2.json()["accepted"] == 0  # harus dianggap duplikat

    shutil.rmtree(tmpdir)

# 3️⃣ Validasi skema event
def test_event_schema_validation(client):
    evt_missing_topic = {
        "event_id": "no-topic",
        "timestamp": iso_now(),
        "source": "pytest",
        "payload": {},
    }
    r1 = client.post("/publish", json=evt_missing_topic)
    assert r1.status_code == 422  # invalid karena topic hilang

    evt_invalid_ts = make_event("bad-ts")
    evt_invalid_ts["timestamp"] = "not-a-timestamp"
    r2 = client.post("/publish", json=evt_invalid_ts)
    assert r2.status_code == 422


# 4️⃣ Konsistensi /stats dan /events
def test_stats_and_events_consistency(client):
    evt1 = make_event("s1", topic="alpha")
    evt2 = make_event("s2", topic="beta")

    client.post("/publish", json=evt1)
    client.post("/publish", json=evt2)

    # paksa proses queue agar semua event tersimpan
    client.post("/_flush")

    events = client.get("/events").json()
    assert len(events) >= 2, "Event belum tersimpan ke storage"

    stats = client.get("/stats").json()
    assert stats["received"] >= 2
    assert isinstance(stats["topics"], list)

    topics = {e["topic"] for e in events}
    assert "alpha" in topics
    assert "beta" in topics


# 5️⃣ Stress kecil (batch)
def test_stress_small_batch(client):
    batch = [make_event(f"batch-{i}", topic="stress") for i in range(100)]
    start = time.time()
    r = client.post("/publish", json=batch)
    elapsed = time.time() - start

    assert r.status_code == 200
    assert r.json()["accepted"] == 100
    assert elapsed < 2.0, f"Terlalu lambat: {elapsed:.2f}s"

    # Paksa worker memproses seluruh event di queue
    client.post("/_flush")

    # Tunggu sampai semua event benar-benar tersimpan
    events = wait_for_events(client, expected_count=100, topic="stress")

    assert len(events) >= 100, f"Worker belum memproses semua event (baru {len(events)})"
