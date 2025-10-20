import os
import sqlite3
import json
from typing import Tuple
from contextlib import closing


class DedupStore:
    """Simple SQLite backed dedup store. Thread-safe via sqlite's own locking.

    Methods:
    ensure_tables()
    add_if_new(topic, event_id) -> bool # True if newly added (not duplicate)
    list_processed(topic=None)
    stats()
    """

    def __init__(self, path: str):
        self.path = path
        os.makedirs(os.path.dirname(path), exist_ok=True)
        self.conn = sqlite3.connect(self.path, check_same_thread=False, isolation_level=None)
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
                payload TEXT
            )
            ''')

    def add_if_new(self, topic: str, event_id: str, processed_at: str) -> bool:
        """Atomically insert into processed; return True if inserted, False if duplicate."""
        before = self.conn.total_changes
        try:
            with closing(self.conn.cursor()) as c:
                c.execute(
                    'INSERT OR IGNORE INTO processed(topic, event_id, processed_at) VALUES (?, ?, ?)',
                    (topic, event_id, processed_at)
                )
        except Exception:
            raise
        after = self.conn.total_changes
        return (after - before) > 0


    def insert_event_record(self, topic: str, event_id: str, timestamp: str, source: str, payload: str):
        with closing(self.conn.cursor()) as c:
            c.execute('INSERT INTO events(topic,event_id,timestamp,source,payload) VALUES (?,?,?,?,?)', (topic,event_id,timestamp,source,payload))

    def list_processed(self, topic: str=None):
        with closing(self.conn.cursor()) as c:
            if topic:
                c.execute('SELECT topic,event_id,processed_at FROM processed WHERE topic=? ORDER BY processed_at', (topic,))
            else:
                c.execute('SELECT topic,event_id,processed_at FROM processed ORDER BY processed_at')
            return c.fetchall()

    def list_events(self, topic: str=None):
        with closing(self.conn.cursor()) as c:
            if topic:
                c.execute('SELECT topic,event_id,timestamp,source,payload FROM events WHERE topic=? ORDER BY timestamp', (topic,))
            else:
                c.execute('SELECT topic,event_id,timestamp,source,payload FROM events ORDER BY timestamp')
            return c.fetchall()
        
    def is_new_event(self, topic: str, event_id: str) -> bool:
        cur = self.conn.cursor()
        cur.execute("SELECT COUNT(*) FROM events WHERE topic=? AND event_id=?", (topic, event_id))
        count = cur.fetchone()[0]
        return count == 0
    
    def mark_event(self, topic: str, event_id: str, timestamp, source: str, payload):
        """Menandai event sebagai telah diproses dan menyimpannya ke tabel processed + events"""
        try:
            # konversi tipe yang tidak didukung SQLite
            if not isinstance(timestamp, str):
                timestamp = str(timestamp)
            if not isinstance(payload, str):
                payload = json.dumps(payload)

            with closing(self.conn.cursor()) as c:
                c.execute('BEGIN')
                c.execute('INSERT OR IGNORE INTO processed(topic, event_id, processed_at) VALUES (?,?,?)',
                        (topic, event_id, timestamp))
                c.execute('INSERT OR IGNORE INTO events(topic, event_id, timestamp, source, payload) VALUES (?,?,?,?,?)',
                        (topic, event_id, timestamp, source, payload))
                c.execute('COMMIT')
        except Exception:
            try:
                c.execute('ROLLBACK')
            except Exception:
                pass
            raise

    def close(self):
        self.conn.close()