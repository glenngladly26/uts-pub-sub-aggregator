import os
import sys
import shutil
import tempfile
import time
import pytest
from fastapi.testclient import TestClient
from src.main import make_app
from src.util import iso_now

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


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


# 2️⃣ Validasi dedup — kirim duplikat, hanya 1 diterima
def test_dedup_behavior(tempdir):
    db = os.path.join(tempdir, 'db2.db')
    client = start_client_with_db(db)

    evt = {
        'topic': 'dedup-test',
        'event_id': 'dup-1',
        'timestamp': iso_now(),
        'source': 'tester',
        'payload': {'x': 1}
    }

    r1 = client.post('/publish', json=evt)
    r2 = client.post('/publish', json=evt)

    assert r1.status_code == 200
    assert r2.status_code == 200
    assert r1.json()['accepted'] == 1
    assert r2.json()['accepted'] == 0  # duplikat ditolak

    client.__exit__(None, None, None)


# # 3️⃣ Persistensi dedup store — restart app, kirim duplikat tetap ditolak
# def test_dedup_persistence(tempdir):
#     db = os.path.join(tempdir, 'db3.db')

#     # Jalankan "sesi pertama"
#     client1 = start_client_with_db(db)
#     evt = {
#         'topic': 'persist-test',
#         'event_id': 'persist-1',
#         'timestamp': iso_now(),
#         'source': 's1',
#         'payload': {}
#     }
#     r = client1.post('/publish', json=evt)
#     assert r.status_code == 200 and r.json()['accepted'] == 1

#     # Simulasi restart aplikasi
#     del client1
#     client2 = start_client_with_db(db)

#     # Kirim ulang event yang sama — harus ditolak karena sudah pernah
#     r2 = client2.post('/publish', json=evt)
#     assert r2.status_code == 200
#     assert r2.json()['accepted'] == 0


# # 4️⃣ GET /stats dan GET /events konsisten
# def test_stats_and_events_consistency(tempdir):
#     db = os.path.join(tempdir, 'db4.db')
#     client = start_client_with_db(db)

#     for i in range(3):
#         evt = {
#             'topic': 'stat-test',
#             'event_id': f'st{i}',
#             'timestamp': iso_now(),
#             'source': 'src',
#             'payload': {'i': i}
#         }
#         client.post('/publish', json=evt)

#     time.sleep(0.2)
#     stats = client.get('/stats').json()
#     events = client.get('/events').json()

#     assert stats['received'] >= 3
#     assert len(events) >= 3
#     assert all('event_id' in e for e in events)


# # 5️⃣ Batch event — semua diterima sekaligus
# def test_publish_batch(tempdir):
#     db = os.path.join(tempdir, 'db5.db')
#     client = start_client_with_db(db)

#     batch = [
#         {
#             'topic': 'batch-test',
#             'event_id': f'b{i}',
#             'timestamp': iso_now(),
#             'source': 'tester',
#             'payload': {'v': i}
#         }
#         for i in range(10)
#     ]

#     r = client.post('/publish_batch', json=batch)
#     assert r.status_code == 200
#     assert r.json()['accepted'] == len(batch)


# # 6️⃣ Stress kecil — masukan batch besar dan ukur waktu eksekusi
# def test_small_stress(tempdir):
#     db = os.path.join(tempdir, 'db6.db')
#     client = start_client_with_db(db)

#     batch = [
#         {
#             'topic': 'stress',
#             'event_id': f'stress-{i}',
#             'timestamp': iso_now(),
#             'source': 'load',
#             'payload': {'v': i}
#         }
#         for i in range(200)
#     ]

#     start = time.time()
#     r = client.post('/publish_batch', json=batch)
#     duration = time.time() - start

#     assert r.status_code == 200
#     assert r.json()['accepted'] == len(batch)
#     assert duration < 5.0, f"Batch insert terlalu lambat: {duration:.2f}s"


# # 7️⃣ Event dengan timestamp invalid ditolak
# def test_invalid_timestamp(tempdir):
#     db = os.path.join(tempdir, 'db7.db')
#     client = start_client_with_db(db)

#     evt = {
#         'topic': 'invalid-ts',
#         'event_id': 'badts',
#         'timestamp': 'invalid-date',
#         'source': 's',
#         'payload': {}
#     }

#     r = client.post('/publish', json=evt)
#     assert r.status_code == 422


# # 8️⃣ Event dengan payload besar tetap diterima
# def test_large_payload(tempdir):
#     db = os.path.join(tempdir, 'db8.db')
#     client = start_client_with_db(db)

#     large_payload = {'data': 'x' * 10_000}
#     evt = {
#         'topic': 'large',
#         'event_id': 'big1',
#         'timestamp': iso_now(),
#         'source': 's',
#         'payload': large_payload
#     }

#     r = client.post('/publish', json=evt)
#     assert r.status_code == 200
#     assert r.json()['accepted'] == 1


# # 9️⃣ Event dengan topic berbeda tetap disimpan terpisah
# def test_multiple_topics(tempdir):
#     db = os.path.join(tempdir, 'db9.db')
#     client = start_client_with_db(db)

#     evt1 = {
#         'topic': 't1',
#         'event_id': 'x1',
#         'timestamp': iso_now(),
#         'source': 's',
#         'payload': {}
#     }
#     evt2 = {
#         'topic': 't2',
#         'event_id': 'x1',  # event_id sama, tapi topic beda
#         'timestamp': iso_now(),
#         'source': 's',
#         'payload': {}
#     }

#     r1 = client.post('/publish', json=evt1)
#     r2 = client.post('/publish', json=evt2)

#     assert r1.status_code == 200
#     assert r2.status_code == 200
#     assert r1.json()['accepted'] == 1
#     assert r2.json()['accepted'] == 1  # tidak dianggap duplikat karena topic beda


# # 🔟 Endpoint tidak dikenal
# def test_unknown_endpoint(tempdir):
#     db = os.path.join(tempdir, 'db10.db')
#     client = start_client_with_db(db)
#     r = client.get('/unknown')
#     assert r.status_code == 404
