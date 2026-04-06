import json
import logging
import sqlite3
import threading
from datetime import datetime, timedelta, timezone
from typing import Optional

from config import (
    SQLITE_DB,
    INITIAL_RETRY_DELAY_SEC,
    MAX_RETRY_DELAY_SEC,
    MAX_ERROR_TEXT_LEN,
    PROCESSING_STALE_SEC,
)

logger = logging.getLogger(__name__)
_db_lock = threading.Lock()


SCHEMA_SQL = [
    """
    CREATE TABLE IF NOT EXISTS telemetry_queue (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        message_id TEXT UNIQUE,
        machine_code TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        event_ts TEXT,
        received_at TEXT NOT NULL,
        send_status TEXT NOT NULL DEFAULT 'PENDING',
        retry_count INTEGER NOT NULL DEFAULT 0,
        last_error TEXT,
        last_attempt_at TEXT,
        next_retry_at TEXT,
        sent_at TEXT,
        response_code INTEGER,
        response_body TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS device_status (
        machine_code TEXT PRIMARY KEY,
        last_seen_at TEXT NOT NULL,
        last_counter REAL DEFAULT 0,
        last_run_state TEXT DEFAULT 'UNKNOWN',
        last_ip TEXT,
        last_payload_json TEXT,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS command_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        machine_code TEXT NOT NULL,
        request_id TEXT NOT NULL,
        command TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        sent_at TEXT NOT NULL,
        ack_status TEXT DEFAULT 'WAITING',
        ack_payload_json TEXT,
        ack_at TEXT
    )
    """,
]

INDEX_SQL = [
    "CREATE INDEX IF NOT EXISTS idx_queue_status_id ON telemetry_queue(send_status, id)",
    "CREATE INDEX IF NOT EXISTS idx_queue_machine ON telemetry_queue(machine_code)",
    "CREATE INDEX IF NOT EXISTS idx_queue_next_retry ON telemetry_queue(send_status, next_retry_at, id)",
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_queue_message_id ON telemetry_queue(message_id)",
    "CREATE INDEX IF NOT EXISTS idx_command_machine_request ON command_log(machine_code, request_id)",
]

EXPECTED_COLUMNS = {
    'telemetry_queue': {
        'message_id': 'TEXT',
        'next_retry_at': 'TEXT',
        'sent_at': 'TEXT',
        'response_code': 'INTEGER',
        'response_body': 'TEXT',
    },
    'device_status': {},
    'command_log': {},
}


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    return utc_now().isoformat()


def sanitize_error_text(text: str) -> str:
    return (text or '')[:MAX_ERROR_TEXT_LEN]


def calculate_next_retry_at(retry_count: int) -> str:
    delay = min(INITIAL_RETRY_DELAY_SEC * (2 ** max(retry_count - 1, 0)), MAX_RETRY_DELAY_SEC)
    return (utc_now() + timedelta(seconds=delay)).isoformat()


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(SQLITE_DB, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def _get_existing_columns(cur: sqlite3.Cursor, table_name: str) -> set[str]:
    cur.execute(f"PRAGMA table_info({table_name})")
    return {row[1] for row in cur.fetchall()}


def init_db() -> None:
    with _db_lock:
        conn = get_conn()
        cur = conn.cursor()

        for sql in SCHEMA_SQL:
            cur.execute(sql)

        for table_name, columns in EXPECTED_COLUMNS.items():
            existing = _get_existing_columns(cur, table_name)
            for column_name, column_type in columns.items():
                if column_name not in existing:
                    cur.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}")

        for sql in INDEX_SQL:
            cur.execute(sql)

        conn.commit()
        conn.close()

        logger.info('SQLite initialized: %s', SQLITE_DB)


def reset_stuck_processing() -> int:
    stale_before = (utc_now() - timedelta(seconds=PROCESSING_STALE_SEC)).isoformat()
    with _db_lock:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE telemetry_queue
            SET send_status='FAILED',
                retry_count=retry_count+1,
                last_error=COALESCE(last_error, 'Recovered stale PROCESSING row on startup'),
                next_retry_at=?,
                last_attempt_at=?
            WHERE send_status='PROCESSING'
              AND (last_attempt_at IS NULL OR last_attempt_at <= ?)
            """,
            (calculate_next_retry_at(1), utc_now_iso(), stale_before),
        )
        affected = cur.rowcount
        conn.commit()
        conn.close()
        return affected


def enqueue_telemetry(machine_code: str, payload_json: str, event_ts: Optional[str], message_id: str) -> bool:
    now_iso = utc_now_iso()
    with _db_lock:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT OR IGNORE INTO telemetry_queue (
                message_id, machine_code, payload_json, event_ts, received_at,
                send_status, retry_count, next_retry_at
            )
            VALUES (?, ?, ?, ?, ?, 'PENDING', 0, ?)
            """,
            (message_id, machine_code, payload_json, event_ts, now_iso, now_iso),
        )
        inserted = cur.rowcount > 0
        conn.commit()
        conn.close()
        return inserted


def update_device_status(
    machine_code: str,
    last_seen_at: str,
    last_counter: float,
    last_run_state: str,
    last_ip: Optional[str],
    last_payload_json: str,
) -> None:
    with _db_lock:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO device_status (
                machine_code, last_seen_at, last_counter, last_run_state, last_ip, last_payload_json, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(machine_code) DO UPDATE SET
                last_seen_at=excluded.last_seen_at,
                last_counter=excluded.last_counter,
                last_run_state=excluded.last_run_state,
                last_ip=excluded.last_ip,
                last_payload_json=excluded.last_payload_json,
                updated_at=excluded.updated_at
            """,
            (
                machine_code,
                last_seen_at,
                last_counter,
                last_run_state,
                last_ip,
                last_payload_json,
                utc_now_iso(),
            ),
        )
        conn.commit()
        conn.close()


def claim_pending_queue(limit: int = 50) -> list[sqlite3.Row]:
    now_iso = utc_now_iso()
    claimed_ids: list[int] = []

    with _db_lock:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id
            FROM telemetry_queue
            WHERE send_status IN ('PENDING', 'FAILED')
              AND (next_retry_at IS NULL OR next_retry_at <= ?)
            ORDER BY id ASC
            LIMIT ?
            """,
            (now_iso, limit),
        )
        rows = cur.fetchall()
        claimed_ids = [row['id'] for row in rows]

        if not claimed_ids:
            conn.close()
            return []

        for row_id in claimed_ids:
            cur.execute(
                """
                UPDATE telemetry_queue
                SET send_status='PROCESSING', last_attempt_at=?
                WHERE id=? AND send_status IN ('PENDING', 'FAILED')
                """,
                (now_iso, row_id),
            )

        conn.commit()

        placeholders = ','.join('?' for _ in claimed_ids)
        cur.execute(
            f"SELECT * FROM telemetry_queue WHERE id IN ({placeholders}) AND send_status='PROCESSING' ORDER BY id ASC",
            claimed_ids,
        )
        claimed_rows = cur.fetchall()
        conn.close()
        return claimed_rows


def mark_queue_sent(row_id: int, response_code: Optional[int] = None, response_body: Optional[str] = None) -> None:
    with _db_lock:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE telemetry_queue
            SET send_status='SENT',
                sent_at=?,
                last_attempt_at=?,
                last_error=NULL,
                response_code=?,
                response_body=?
            WHERE id=?
            """,
            (utc_now_iso(), utc_now_iso(), response_code, sanitize_error_text(response_body or ''), row_id),
        )
        conn.commit()
        conn.close()


def mark_queue_failed(row_id: int, retry_count: int, error_message: str, response_code: Optional[int] = None) -> None:
    next_retry_at = calculate_next_retry_at(retry_count)
    with _db_lock:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE telemetry_queue
            SET send_status='FAILED',
                retry_count=?,
                last_attempt_at=?,
                last_error=?,
                next_retry_at=?,
                response_code=?
            WHERE id=?
            """,
            (
                retry_count,
                utc_now_iso(),
                sanitize_error_text(error_message),
                next_retry_at,
                response_code,
                row_id,
            ),
        )
        conn.commit()
        conn.close()


def release_queue_processing(row_id: int, reason: str) -> None:
    with _db_lock:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE telemetry_queue
            SET send_status='FAILED',
                retry_count=retry_count+1,
                last_attempt_at=?,
                last_error=?,
                next_retry_at=?
            WHERE id=?
            """,
            (utc_now_iso(), sanitize_error_text(reason), calculate_next_retry_at(1), row_id),
        )
        conn.commit()
        conn.close()


def save_command_log(machine_code: str, request_id: str, command: str, payload_json: str) -> None:
    with _db_lock:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO command_log (machine_code, request_id, command, payload_json, sent_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (machine_code, request_id, command, payload_json, utc_now_iso()),
        )
        conn.commit()
        conn.close()


def mark_command_ack(machine_code: str, request_id: str, ack_payload_json: str) -> None:
    with _db_lock:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE command_log
            SET ack_status='ACKED', ack_payload_json=?, ack_at=?
            WHERE machine_code=? AND request_id=?
            """,
            (ack_payload_json, utc_now_iso(), machine_code, request_id),
        )
        conn.commit()
        conn.close()


def get_queue_summary() -> dict:
    with _db_lock:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT send_status, COUNT(*) AS cnt
            FROM telemetry_queue
            GROUP BY send_status
            """
        )
        rows = cur.fetchall()
        conn.close()

    summary = {'PENDING': 0, 'FAILED': 0, 'PROCESSING': 0, 'SENT': 0}
    for row in rows:
        summary[row['send_status']] = row['cnt']
    return summary


def get_device_overview() -> list[dict]:
    with _db_lock:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT machine_code, last_seen_at, last_counter, last_run_state, last_ip, updated_at
            FROM device_status
            ORDER BY machine_code ASC
            """
        )
        rows = [dict(row) for row in cur.fetchall()]
        conn.close()
        return rows
