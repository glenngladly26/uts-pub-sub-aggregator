import os
import sqlite3
import json
from typing import Tuple
from contextlib import closing

class DedupStore:
    """Simple SQLite backed dedup store. Thread-safe via sqlite's own locking."""
    
    def __init__(self, path: str):
        self.path = path
        os.makedirs(os.path.dirname(path) or '.', exist_ok=True)
        # Gunakan DEFERRED mode untuk transaction control
        self.conn = sqlite3.connect(
            self.path, 
            check_same_thread=False, 
            isolation_level='DEFERRED'  # Changed from None
        )
        # Use WAL for better concurrency
        self.conn.execute('PRAGMA journal_mode=WAL;')
        self.conn.commit()
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
            # Index untuk performa
            c.execute('CREATE INDEX IF NOT EXISTS idx_events_topic ON events(topic)')
            c.execute('CREATE INDEX IF NOT EXISTS idx_processed_topic ON processed(topic)')
        self.conn.commit()
    
    def mark_processed_if_new(self, topic: str, event_id: str, processed_at: str) -> bool:
        """
        Atomically check and mark event sebagai processed.
        Return True jika berhasil ditandai (event BARU).
        Return False jika sudah ada (event DUPLIKAT).
        
        Thread-safe dengan lock eksplisit.
        """
        with self.lock:
            with closing(self.conn.cursor()) as c:
                # Cek apakah sudah ada
                c.execute(
                    'SELECT COUNT(*) FROM processed WHERE topic=? AND event_id=?',
                    (topic, event_id)
                )
                exists = c.fetchone()[0] > 0
                
                if not exists:
                    # Insert jika belum ada
                    c.execute(
                        'INSERT INTO processed(topic, event_id, processed_at) VALUES (?, ?, ?)',
                        (topic, event_id, processed_at)
                    )
                    self.conn.commit()
                    return True
                else:
                    # Sudah ada
                    return False
    
    def insert_event_record(self, topic: str, event_id: str, timestamp: str, 
                           source: str, payload: str):
        """Simpan detail event ke tabel events."""
        with closing(self.conn.cursor()) as c:
            c.execute(
                'INSERT OR IGNORE INTO events(topic, event_id, timestamp, source, payload) VALUES (?,?,?,?,?)',
                (topic, event_id, timestamp, source, payload)
            )
        self.conn.commit()
    
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
    
    def close(self):
        self.conn.close()