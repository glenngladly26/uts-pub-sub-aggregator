import os
import shutil
import tempfile
import time
import pytest
import logging
from fastapi.testclient import TestClient

# Enable logging untuk debugging
logging.basicConfig(level=logging.DEBUG)

from src.main import make_app
from src.util import iso_now

@pytest.fixture()
def tempdir():
    d = tempfile.mkdtemp()
    yield d
    shutil.rmtree(d)

def start_client_with_db(path):
    app = make_app(db_path=path)
    # gunakan context manager agar lifespan dijalankan
    client = TestClient(app)
    client.__enter__()
    return client

# 1️⃣ Validasi skema event — field wajib
def test_invalid_schema_rejected(tempdir):
    db = os.path.join(tempdir, 'db1.db')
    client = start_client_with_db(db)
    
    invalid_evt = {"topic": "x"}  # kurang field wajib
    r = client.post('/publish', json=invalid_evt)
    assert r.status_code == 422  # Pydantic validation error
    
    client.__exit__(None, None, None)

# 2️⃣ Validasi dedup — kirim duplikat, hanya 1 diterima
def test_dedup_behavior(tempdir):
    db = os.path.join(tempdir, 'db2.db')
    print(f"\n🔍 Test DB path: {db}")
    
    # Pastikan database tidak ada sebelumnya
    if os.path.exists(db):
        os.remove(db)
    for ext in ['-shm', '-wal']:
        if os.path.exists(db + ext):
            os.remove(db + ext)
    
    client = start_client_with_db(db)
    
    # Verifikasi database kosong
    events_before = client.get('/events').json()
    print(f"💾 Events before test: {len(events_before)}")
    
    evt = {
        'topic': 'dedup-test',
        'event_id': 'dup-1',
        'timestamp': iso_now(),
        'source': 'tester',
        'payload': {'x': 1}
    }
    
    # Kirim event pertama
    print(f"📤 Sending first event: {evt}")
    r1 = client.post('/publish', json=evt)
    print(f"📥 Response 1: status={r1.status_code}, body={r1.json()}")
    
    assert r1.status_code == 200
    assert r1.json()['accepted'] == 1, f"Expected accepted=1, got {r1.json()}"
    
    # Kirim event duplikat (event_id sama)
    print(f"📤 Sending duplicate event")
    r2 = client.post('/publish', json=evt)
    print(f"📥 Response 2: status={r2.status_code}, body={r2.json()}")
    
    assert r2.status_code == 200
    assert r2.json()['accepted'] == 0, f"Expected accepted=0 (duplicate), got {r2.json()}"
    
    # Tunggu sebentar agar consumer memproses
    time.sleep(0.5)
    
    # Cek stats
    stats = client.get('/stats').json()
    print(f"📊 Stats: {stats}")
    assert stats['received'] == 2, f"Expected received=2, got {stats}"
    assert stats['unique_processed'] == 1, f"Expected unique_processed=1, got {stats}"
    assert stats['duplicate_dropped'] == 1, f"Expected duplicate_dropped=1, got {stats}"
    
    # Cek events di database
    events = client.get('/events?topic=dedup-test').json()
    print(f"💾 Events in DB: {len(events)} items")
    assert len(events) == 1, f"Expected 1 event in DB, got {len(events)}"
    assert events[0]['event_id'] == 'dup-1'
    
    client.__exit__(None, None, None)