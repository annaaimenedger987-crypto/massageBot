import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS services (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    price REAL NOT NULL,
    duration_minutes INTEGER NOT NULL CHECK(duration_minutes > 0),
    active INTEGER NOT NULL DEFAULT 1,
    sort_order INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS schedule_overrides (
    date TEXT PRIMARY KEY,
    working INTEGER NOT NULL,
    start_time TEXT,
    end_time TEXT,
    breaks_json TEXT NOT NULL DEFAULT '[]'
);

CREATE TABLE IF NOT EXISTS appointments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    public_code TEXT UNIQUE,
    client_telegram_id INTEGER,
    client_username TEXT,
    client_name TEXT NOT NULL,
    client_phone TEXT NOT NULL,
    start_at TEXT NOT NULL,
    end_at TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    source TEXT NOT NULL DEFAULT 'telegram',
    created_by_master INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT
);

CREATE TABLE IF NOT EXISTS appointment_services (
    appointment_id INTEGER NOT NULL,
    service_id INTEGER NOT NULL,
    position INTEGER NOT NULL DEFAULT 0,
    price_snapshot REAL NOT NULL,
    duration_snapshot INTEGER NOT NULL,
    name_snapshot TEXT NOT NULL,
    PRIMARY KEY (appointment_id, service_id, position),
    FOREIGN KEY (appointment_id) REFERENCES appointments(id) ON DELETE CASCADE,
    FOREIGN KEY (service_id) REFERENCES services(id)
);

CREATE INDEX IF NOT EXISTS idx_appointments_interval
ON appointments(status, start_at, end_at);

CREATE INDEX IF NOT EXISTS idx_appointments_client
ON appointments(client_telegram_id, status, start_at);

CREATE TABLE IF NOT EXISTS notification_log (
    appointment_id INTEGER NOT NULL,
    notification_type TEXT NOT NULL,
    sent_at TEXT NOT NULL,
    PRIMARY KEY (appointment_id, notification_type),
    FOREIGN KEY (appointment_id) REFERENCES appointments(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS waitlist (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    client_telegram_id INTEGER NOT NULL,
    service_id INTEGER,
    desired_date_from TEXT,
    desired_date_to TEXT,
    status TEXT NOT NULL DEFAULT 'waiting',
    created_at TEXT NOT NULL,
    FOREIGN KEY (service_id) REFERENCES services(id)
);
"""


class SlotBusyError(Exception):
    pass


class Database:
    def __init__(self, path: Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.initialize()

    def connect(self):
        conn = sqlite3.connect(self.path, timeout=15)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=15000")
        return conn

    def initialize(self):
        with self.connect() as conn:
            conn.executescript(SCHEMA)

    @contextmanager
    def transaction(self):
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def seed_services(self, items):
        with self.connect() as conn:
            count = conn.execute("SELECT COUNT(*) FROM services").fetchone()[0]
            if count:
                return
            conn.executemany(
                "INSERT INTO services(name, price, duration_minutes, sort_order) VALUES(?,?,?,?)",
                [
                    (x["name"], x["price"], x["duration_minutes"], i)
                    for i, x in enumerate(items)
                ],
            )

    def services(self):
        with self.connect() as conn:
            return conn.execute(
                "SELECT * FROM services WHERE active=1 ORDER BY sort_order, id"
            ).fetchall()

    def service(self, service_id):
        with self.connect() as conn:
            return conn.execute(
                "SELECT * FROM services WHERE id=? AND active=1", (service_id,)
            ).fetchone()

    def set_override(self, date_value, working, start=None, end=None, breaks=None):
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO schedule_overrides(date, working, start_time, end_time, breaks_json)
                VALUES(?,?,?,?,?)
                ON CONFLICT(date) DO UPDATE SET
                  working=excluded.working,
                  start_time=excluded.start_time,
                  end_time=excluded.end_time,
                  breaks_json=excluded.breaks_json
                """,
                (date_value, int(working), start, end, json.dumps(breaks or [])),
            )

    def clear_override(self, date_value):
        with self.connect() as conn:
            conn.execute("DELETE FROM schedule_overrides WHERE date=?", (date_value,))

    def override(self, date_value):
        with self.connect() as conn:
            return conn.execute(
                "SELECT * FROM schedule_overrides WHERE date=?", (date_value,)
            ).fetchone()

    def overlapping(self, start_at, end_at, ignore_id=None, conn=None):
        own_conn = conn is None
        conn = conn or self.connect()
        try:
            sql = """
                SELECT id FROM appointments
                WHERE status='active' AND start_at < ? AND end_at > ?
            """
            args = [end_at, start_at]
            if ignore_id is not None:
                sql += " AND id != ?"
                args.append(ignore_id)
            return conn.execute(sql, args).fetchall()
        finally:
            if own_conn:
                conn.close()

    def create_appointment(
        self,
        service,
        start_at,
        end_at,
        client_name,
        client_phone,
        client_id=None,
        username=None,
        source="telegram",
        manual=False,
    ):
        created = datetime.now(timezone.utc).isoformat(timespec="seconds")
        with self.transaction() as conn:
            if self.overlapping(start_at, end_at, conn=conn):
                raise SlotBusyError
            cursor = conn.execute(
                """
                INSERT INTO appointments(
                  client_telegram_id, client_username, client_name, client_phone,
                  start_at, end_at, source, created_by_master, created_at
                ) VALUES(?,?,?,?,?,?,?,?,?)
                """,
                (
                    client_id,
                    username,
                    client_name,
                    client_phone,
                    start_at,
                    end_at,
                    source,
                    int(manual),
                    created,
                ),
            )
            appointment_id = cursor.lastrowid
            code = f"SF-{appointment_id:06d}"
            conn.execute(
                "UPDATE appointments SET public_code=? WHERE id=?",
                (code, appointment_id),
            )
            conn.execute(
                """
                INSERT INTO appointment_services(
                  appointment_id, service_id, price_snapshot, duration_snapshot, name_snapshot
                ) VALUES(?,?,?,?,?)
                """,
                (
                    appointment_id,
                    service["id"],
                    service["price"],
                    service["duration_minutes"],
                    service["name"],
                ),
            )
        return self.appointment(appointment_id)

    def appointment(self, appointment_id):
        with self.connect() as conn:
            return conn.execute(
                """
                SELECT a.*, s.name_snapshot AS service_name,
                       s.price_snapshot AS price, s.duration_snapshot AS duration_minutes
                FROM appointments a
                JOIN appointment_services s ON s.appointment_id=a.id AND s.position=0
                WHERE a.id=?
                """,
                (appointment_id,),
            ).fetchone()

    def appointment_service(self, appointment_id):
        with self.connect() as conn:
            return conn.execute(
                """
                SELECT s.* FROM services s
                JOIN appointment_services a ON a.service_id=s.id
                WHERE a.appointment_id=?
                ORDER BY a.position LIMIT 1
                """,
                (appointment_id,),
            ).fetchone()

    def client_appointments(self, client_id, after):
        with self.connect() as conn:
            return conn.execute(
                """
                SELECT a.*, s.name_snapshot AS service_name,
                       s.price_snapshot AS price, s.duration_snapshot AS duration_minutes
                FROM appointments a
                JOIN appointment_services s ON s.appointment_id=a.id AND s.position=0
                WHERE a.client_telegram_id=? AND a.status='active' AND a.start_at>=?
                ORDER BY a.start_at
                """,
                (client_id, after),
            ).fetchall()

    def upcoming(self, after, limit=100):
        with self.connect() as conn:
            return conn.execute(
                """
                SELECT a.*, s.name_snapshot AS service_name,
                       s.price_snapshot AS price, s.duration_snapshot AS duration_minutes
                FROM appointments a
                JOIN appointment_services s ON s.appointment_id=a.id AND s.position=0
                WHERE a.status='active' AND a.start_at>=?
                ORDER BY a.start_at LIMIT ?
                """,
                (after, limit),
            ).fetchall()

    def active_on_date(self, date_value):
        start = f"{date_value}T00:00:00"
        end = f"{date_value}T23:59:59.999999"
        with self.connect() as conn:
            return conn.execute(
                """
                SELECT id FROM appointments
                WHERE status='active' AND start_at>=? AND start_at<=?
                """,
                (start, end),
            ).fetchall()

    def cancel(self, appointment_id, actor):
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE appointments SET status=?, updated_at=?
                WHERE id=? AND status='active'
                """,
                (
                    f"cancelled_by_{actor}",
                    datetime.now(timezone.utc).isoformat(timespec="seconds"),
                    appointment_id,
                ),
            )
            return conn.total_changes == 1

    def move(self, appointment_id, start_at, end_at):
        with self.transaction() as conn:
            if self.overlapping(start_at, end_at, ignore_id=appointment_id, conn=conn):
                raise SlotBusyError
            conn.execute(
                "UPDATE appointments SET start_at=?, end_at=?, updated_at=? WHERE id=? AND status='active'",
                (
                    start_at,
                    end_at,
                    datetime.now(timezone.utc).isoformat(timespec="seconds"),
                    appointment_id,
                ),
            )
            if conn.total_changes != 1:
                raise ValueError("appointment_not_active")
            conn.execute(
                "DELETE FROM notification_log WHERE appointment_id=?", (appointment_id,)
            )
        return self.appointment(appointment_id)

    def due_reminders(self, now_iso, until_iso, notification_type):
        with self.connect() as conn:
            return conn.execute(
                """
                SELECT a.*, s.name_snapshot AS service_name,
                       s.price_snapshot AS price, s.duration_snapshot AS duration_minutes
                FROM appointments a
                JOIN appointment_services s ON s.appointment_id=a.id AND s.position=0
                LEFT JOIN notification_log n
                  ON n.appointment_id=a.id AND n.notification_type=?
                WHERE a.status='active' AND a.client_telegram_id IS NOT NULL
                  AND a.start_at>? AND a.start_at<=? AND n.appointment_id IS NULL
                ORDER BY a.start_at
                """,
                (notification_type, now_iso, until_iso),
            ).fetchall()

    def mark_notification(self, appointment_id, notification_type, sent_at):
        with self.connect() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO notification_log VALUES(?,?,?)",
                (appointment_id, notification_type, sent_at),
            )
