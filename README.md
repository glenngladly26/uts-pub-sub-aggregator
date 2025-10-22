# 🧩 UTS Pub-Sub Aggregator

Sebuah sistem Pub-Sub Log Aggregator sederhana yang dirancang untuk menangani deduplikasi event, pemrosesan asinkron, serta penyimpanan event menggunakan FastAPI dan SQLite.
Proyek ini mendemonstrasikan arsitektur idempotent consumer dalam sistem terdistribusi.


## ⚙️ Instalasi dan Menjalankan Aplikasi
### 1️⃣ Clone Repository
```
git clone https://github.com/glenngladly26/uts-pub-sub-aggregator.git
cd uts-pub-sub-aggregator
```

### 2️⃣ Buat Virtual Environment
```
python -m venv venv
```

### 3️⃣ Install Dependencies
```
pip install -r requirements.txt
```

### 4️⃣ Jalankan Server Lokal
```
uvicorn src.main:app --reload
```

### 5️⃣ Akses Dokumentasi API Lokal
```
http://127.0.0.1:8000/docs
```


## 🐳 Menjalankan Menggunakan Docker
### Build Image
```
docker build -t uts-aggregator .
```

### Jalankan Container
```
docker run -d -p 8080:8080 uts-aggregator
```

### Dokumentasi API via Docker
```
http://localhost:8080/docs
```


## 🧪 Testing
### Unit Testing
Menjalankan seluruh pengujian
```
pytest -v -s
```

### Load / Stress Testing
Mengirimkan lebih dari 5000 event secara paralel untuk menguji performa deduplikasi dan throughput
```
python load_test.py
```


## 🌐 Daftar Endpoint
| Method   | Endpoint   | Deskripsi                                                                                             |
| -------- | ---------- | ----------------------------------------------------------------------------------------------------- |
| **POST** | `/publish` | Menerima event tunggal atau batch. Melakukan deduplikasi sebelum dimasukkan ke queue.                 |
| **GET**  | `/events`  | Mengambil seluruh event yang sudah tersimpan. Dapat difilter berdasarkan `topic`.                     |
| **GET**  | `/stats`   | Menampilkan statistik runtime aplikasi (jumlah event diterima, diproses, duplikat, dan daftar topik). |
| **POST** | `/_flush`  | Endpoint internal (untuk testing) untuk memproses semua event yang tersisa di queue secara sinkron.   |


## 🧠 Asumsi Sistem
1. Deduplikasi berbasis (topic, event_id)
Jika kombinasi tersebut sudah pernah diproses, maka event berikutnya dianggap duplikat.

2. Penyimpanan Menggunakan SQLite
Database disimpan secara lokal (default: ./data/dedup.db) atau melalui variabel environment DEDUP_DB.

3. Queue Asinkron (in-memory)
Event yang diterima akan dimasukkan ke asyncio.Queue sebelum diproses oleh ConsumerWorker.

4. ConsumerWorker berjalan selama aplikasi aktif
Worker otomatis berhenti saat aplikasi dimatikan.

5. Testing menggunakan database sementara
Setiap test case membuat file database baru untuk menghindari interferensi antar test.