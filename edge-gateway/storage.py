"""
Capa de persistencia local del gateway (SQLite con WAL).
Thread-safe: usa check_same_thread=False y un Lock para escrituras.
"""
import json
import logging
import sqlite3
import threading

log = logging.getLogger(__name__)


SCHEMA = """
CREATE TABLE IF NOT EXISTS pending_events (
    event_id    TEXT PRIMARY KEY,
    payload     TEXT NOT NULL,
    created_at  REAL NOT NULL,
    attempts    INTEGER DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_pending_created
    ON pending_events (created_at);

CREATE TABLE IF NOT EXISTS processed_events (
    event_id     TEXT PRIMARY KEY,
    processed_at REAL NOT NULL
);
"""


class LocalStorage:
    def __init__(self, db_path: str = "gateway.db", enable_dedup: bool = True):
        self.db_path = db_path
        self.enable_dedup = enable_dedup
        self._lock = threading.Lock()
        self._init_db()

    def _init_db(self):
        # check_same_thread=False permite usar la conexion desde varios hilos.
        # El Lock serializa las escrituras para evitar race conditions.
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.executescript(SCHEMA)
        self.conn.commit()

    # ---------- Store-and-forward ----------
    def enqueue(self, event: dict):
        """Guarda un evento para reenvio posterior."""
        with self._lock:
            self.conn.execute(
                "INSERT OR IGNORE INTO pending_events (event_id, payload, created_at) VALUES (?, ?, ?)",
                (event["event_id"], json.dumps(event), 0.0),
            )
            self.conn.commit()

    def dequeue_batch(self, limit: int = 100) -> list[dict]:
        """Devuelve los eventos pendientes mas antiguos."""
        with self._lock:
            rows = self.conn.execute(
                "SELECT event_id, payload FROM pending_events ORDER BY created_at LIMIT ?",
                (limit,),
            ).fetchall()
        return [{"event_id": r[0], **json.loads(r[1])} for r in rows]

    def ack(self, event_id: str):
        """Marca un evento como enviado (lo elimina de la cola)."""
        with self._lock:
            self.conn.execute("DELETE FROM pending_events WHERE event_id = ?", (event_id,))
            self.conn.commit()

    def pending_count(self) -> int:
        with self._lock:
            row = self.conn.execute("SELECT COUNT(*) FROM pending_events").fetchone()
        return row[0]

    # ---------- Deduplicacion ----------
    def is_duplicate(self, event_id: str) -> bool:
        if not self.enable_dedup:
            return False
        with self._lock:
            row = self.conn.execute(
                "SELECT 1 FROM processed_events WHERE event_id = ?", (event_id,)
            ).fetchone()
        return row is not None

    def mark_processed(self, event_id: str):
        if not self.enable_dedup:
            return
        with self._lock:
            self.conn.execute(
                "INSERT OR IGNORE INTO processed_events (event_id, processed_at) VALUES (?, ?)",
                (event_id, 0.0),
            )
            self.conn.commit()

    def close(self):
        with self._lock:
            self.conn.close()