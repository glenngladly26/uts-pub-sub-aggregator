import os
import sqlite3
from typing import Tuple
from contextlib import closing


class DedupStore:
    """Simple SQLite backed dedup store. Thread-safe via sqlite's own locking."""

    def __init__(self, path: str):
        self.path = path
        os.makedirs(os.path.dirname(path) or '.', exist_ok=True)
        self.conn = sqlite3.connect(
            self.path,
            check_same_thread=False,
            isolation_level=None
        )
        # Use WAL for better concurrency
        self.conn.execute('PRAGMA journal_mode=WAL;')
        self.ensure_tables()

    def ensure_tables(self):
        with closing(self.conn.cursor()) as c:
            c.execute('''
            CREATE TABLE IF NOT EXISTS processed (
                topic TEXT NOT NULL,
                event_id TEXT NOT NULL,
                processed_at TEXT NOT NULL,
                PRIMARY KEY(topic, event_id)
            )
            ''')
            c.execute('''
            CREATE TABLE IF NOT EXISTS events (
                topic TEXT NOT NULL,
                event_id TEXT NOT NULL,
                timestamp TEXT,
                source TEXT,
                payload TEXT,
                PRIMARY KEY(topic, event_id)
            )
            ''')
            c.execute('CREATE INDEX IF NOT EXISTS idx_events_topic ON events(topic)')
            c.execute('CREATE INDEX IF NOT EXISTS idx_processed_topic ON processed(topic)')

    def add_if_new(self, topic: str, event_id: str, processed_at: str) -> bool:
        """
        Coba tambahkan event baru secara atomik.
        Return True jika event BARU (insert berhasil),
        False jika DUPLIKAT (sudah pernah ada).
        """
        with closing(self.conn.cursor()) as c:
            c.execute(
                "INSERT OR IGNORE INTO processed(topic, event_id, processed_at) VALUES (?, ?, ?)",
                (topic, event_id, processed_at),
            )
            return c.rowcount == 1  # True = baru, False = duplikat

    def insert_event_record(self, topic: str, event_id: str, timestamp: str,
                            source: str, payload: str):
        """Simpan detail event ke tabel events."""
        with closing(self.conn.cursor()) as c:
            c.execute(
                'INSERT OR IGNORE INTO events(topic, event_id, timestamp, source, payload) VALUES (?,?,?,?,?)',
                (topic, event_id, timestamp, source, payload)
            )

    def list_processed(self, topic: str = None):
        with closing(self.conn.cursor()) as c:
            if topic:
                c.execute(
                    'SELECT topic, event_id, processed_at FROM processed WHERE topic=? ORDER BY processed_at',
                    (topic,)
                )
            else:
                c.execute('SELECT topic, event_id, processed_at FROM processed ORDER BY processed_at')
            return c.fetchall()

    def list_events(self, topic: str = None):
        with closing(self.conn.cursor()) as c:
            if topic:
                c.execute(
                    'SELECT topic, event_id, timestamp, source, payload FROM events WHERE topic=? ORDER BY timestamp',
                    (topic,)
                )
            else:
                c.execute('SELECT topic, event_id, timestamp, source, payload FROM events ORDER BY timestamp')
            return c.fetchall()

    def get_stats(self):
        """Dapatkan statistik dari database."""
        with closing(self.conn.cursor()) as c:
            c.execute('SELECT COUNT(*) FROM processed')
            total_processed = c.fetchone()[0]

            c.execute('SELECT COUNT(DISTINCT topic) FROM processed')
            unique_topics = c.fetchone()[0]

            return {
                'total_processed': total_processed,
                'unique_topics': unique_topics
            }
        
    def insert_event(self, topic, event_id, timestamp, source, payload):
        """Masukkan event baru ke tabel events."""
        cur = self.conn.cursor()
        cur.execute(
            "INSERT INTO events (topic, event_id, timestamp, source, payload) VALUES (?, ?, ?, ?, ?)",
            (topic, event_id, timestamp, source, payload),
        )
        self.conn.commit()
        cur.close()

    def close(self):
        self.conn.close()
