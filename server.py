#!/usr/bin/env python3
import csv
import hashlib
import io
import json
import math
import os
import random
import re
import secrets
import sqlite3
import threading
import time
import uuid
from datetime import datetime, timedelta
from difflib import SequenceMatcher
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = Path(os.getenv("DATA_DIR", str(BASE_DIR / "data")))
STATIC_DIR = BASE_DIR / "static"
DB_PATH = DATA_DIR / "app.db"
LEADS_IMPORT_FILE = DATA_DIR / "leads_import.csv"
LEAD_SYNC_INTERVAL_SECONDS = 30 * 60
MAX_OTP_ATTEMPTS = 3
OTP_TTL_SECONDS = 120
GEO_RADIUS_METERS = 200
SESSION_HOURS = 24

ROLE_BROKER = "BROKER"
ROLE_RM = "RM"
ROLE_SRM = "SRM"

PROPERTY_STATUS_ACTIVE = "active"
PROPERTY_STATUS_SOLD = "sold"
PROPERTY_STATUS_WITHDRAWN = "withdrawn"
PROPERTY_STATUS_HIDDEN_DUPLICATE = "hidden_duplicate_review"
PROPERTY_STATUS_DUPLICATE_REJECTED = "duplicate_rejected"
PROPERTY_STATUS_BACKUP = "backup"

SLOT_STATUS_OPEN = "open"
SLOT_STATUS_BOOKED = "booked"
SLOT_STATUS_CANCELLED = "cancelled"
SLOT_STATUS_COMPLETED = "completed"

VISIT_STATUS_SCHEDULED = "scheduled"
VISIT_STATUS_CANCELLED_BROKER = "cancelled_by_broker"
VISIT_STATUS_CANCELLED_CUSTOMER = "cancelled_by_customer"
VISIT_STATUS_RESCHEDULED = "rescheduled_by_customer"
VISIT_STATUS_COMPLETED = "completed"

INCIDENT_PENDING_RM = "pending_rm_review"
INCIDENT_ESCALATED = "escalated_to_srm"
INCIDENT_APPROVED = "approved_emergency"
INCIDENT_REJECTED = "rejected_emergency"
INCIDENT_REJECTED_NO_EMERGENCY = "rejected_no_emergency"
INCIDENT_APPROVED_SRM = "approved_by_srm"
INCIDENT_REJECTED_SRM = "rejected_by_srm"

FLAG_ACTIVE = "active"
FLAG_DECAYED = "decayed"

WHATSAPP_LANG = "en"
WHATSAPP_PROVIDER = "mock_whatsapp_provider"


def now_local() -> datetime:
    return datetime.now()


def to_iso(ts: datetime | None) -> str | None:
    if ts is None:
        return None
    return ts.replace(microsecond=0).isoformat()


def parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value)


def hash_password(password: str) -> str:
    return hashlib.sha256(password.encode("utf-8")).hexdigest()


def normalize_phone(phone: str | None) -> str:
    raw = (phone or "").strip()
    digits = re.sub(r"\D", "", raw)
    if not digits:
        return ""
    if len(digits) == 10:
        return f"+91{digits}"
    if digits.startswith("91") and len(digits) == 12:
        return f"+{digits}"
    if raw.startswith("+"):
        return f"+{digits}"
    return f"+{digits}"


def normalize_text(value: str | None) -> str:
    text = (value or "").strip().lower()
    text = re.sub(r"\s+", " ", text)
    return text


def text_similarity(a: str | None, b: str | None) -> float:
    aa = normalize_text(a)
    bb = normalize_text(b)
    if not aa or not bb:
        return 0.0
    if aa == bb:
        return 1.0
    return SequenceMatcher(None, aa, bb).ratio()


def haversine_meters(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371000
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    d1 = math.radians(lat2 - lat1)
    d2 = math.radians(lon2 - lon1)
    a = math.sin(d1 / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(d2 / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return r * c


def connect_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with connect_db() as conn:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                email TEXT NOT NULL UNIQUE,
                password_hash TEXT NOT NULL,
                role TEXT NOT NULL,
                city TEXT,
                active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                token TEXT NOT NULL UNIQUE,
                expires_at TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY(user_id) REFERENCES users(id)
            );

            CREATE TABLE IF NOT EXISTS rm_assignments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                rm_id INTEGER NOT NULL,
                broker_id INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE(rm_id, broker_id)
            );

            CREATE TABLE IF NOT EXISTS properties (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                broker_id INTEGER NOT NULL,
                title TEXT NOT NULL,
                asset_type TEXT NOT NULL,
                configuration TEXT,
                spec_value REAL,
                spec_unit TEXT,
                bhk TEXT,
                area_value REAL,
                location_text TEXT NOT NULL,
                city TEXT NOT NULL,
                price REAL NOT NULL,
                maps_url TEXT,
                latitude REAL,
                longitude REAL,
                amenities TEXT,
                image_url TEXT,
                status TEXT NOT NULL,
                hidden_from_customers INTEGER NOT NULL DEFAULT 0,
                duplicate_score REAL,
                primary_property_id INTEGER,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY(broker_id) REFERENCES users(id)
            );

            CREATE TABLE IF NOT EXISTS property_removal_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                property_id INTEGER NOT NULL,
                broker_id INTEGER NOT NULL,
                reason TEXT NOT NULL,
                details TEXT,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS duplicate_review_queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                property_id INTEGER NOT NULL,
                matched_property_id INTEGER NOT NULL,
                similarity REAL NOT NULL,
                auto_hidden INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL,
                rm_id INTEGER,
                decision TEXT,
                notes TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS slots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                broker_id INTEGER NOT NULL,
                city TEXT NOT NULL,
                start_at TEXT NOT NULL,
                end_at TEXT NOT NULL,
                status TEXT NOT NULL,
                cancel_reason TEXT,
                cancelled_at TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS customers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT,
                phone_norm TEXT NOT NULL UNIQUE,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS leads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                customer_id INTEGER NOT NULL,
                city TEXT,
                location_pref TEXT,
                config_pref TEXT,
                budget_min REAL,
                budget_max REAL,
                requirement_text TEXT,
                source TEXT NOT NULL,
                last_synced_at TEXT NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE(customer_id, city, location_pref, config_pref, budget_min, budget_max)
            );

            CREATE TABLE IF NOT EXISTS visits (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                slot_id INTEGER NOT NULL,
                property_id INTEGER NOT NULL,
                broker_id INTEGER NOT NULL,
                rm_id INTEGER,
                customer_id INTEGER NOT NULL,
                customer_requirements TEXT,
                start_at TEXT NOT NULL,
                end_at TEXT NOT NULL,
                status TEXT NOT NULL,
                cancelled_by TEXT,
                cancellation_reason TEXT,
                priority_rebook_until TEXT,
                otp_code TEXT,
                otp_expires_at TEXT,
                otp_attempts INTEGER NOT NULL DEFAULT 0,
                otp_sent_at TEXT,
                checkin_lat REAL,
                checkin_lng REAL,
                distance_meters REAL,
                photo_fallback_base64 TEXT,
                is_unique_visit INTEGER NOT NULL DEFAULT 0,
                completion_mode TEXT,
                completed_at TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS cancellation_incidents (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                slot_id INTEGER NOT NULL,
                visit_id INTEGER NOT NULL,
                broker_id INTEGER NOT NULL,
                raised_at TEXT NOT NULL,
                within_24h INTEGER NOT NULL,
                is_booked INTEGER NOT NULL,
                emergency_requested INTEGER NOT NULL,
                emergency_reason TEXT,
                emergency_details TEXT,
                status TEXT NOT NULL,
                sla_due_at TEXT,
                escalated_to_srm INTEGER NOT NULL DEFAULT 0,
                srm_due_at TEXT,
                resolved_at TEXT,
                rm_id INTEGER,
                srm_id INTEGER,
                rm_note TEXT,
                srm_note TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS broker_flags (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                broker_id INTEGER NOT NULL,
                incident_id INTEGER,
                level INTEGER NOT NULL,
                reason TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                decays_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS broker_penalties (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                broker_id INTEGER NOT NULL,
                year INTEGER NOT NULL,
                month INTEGER NOT NULL,
                reason TEXT NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE(broker_id, year, month, reason)
            );

            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_type TEXT NOT NULL,
                entity_type TEXT NOT NULL,
                entity_id INTEGER,
                payload_json TEXT,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS whatsapp_templates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                template_name TEXT NOT NULL,
                language TEXT NOT NULL,
                body TEXT NOT NULL,
                active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL,
                UNIQUE(template_name, language)
            );

            CREATE TABLE IF NOT EXISTS whatsapp_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                direction TEXT NOT NULL,
                source TEXT NOT NULL,
                provider TEXT NOT NULL,
                to_phone TEXT,
                from_phone TEXT,
                template_name TEXT,
                language TEXT,
                message_text TEXT,
                payload_json TEXT,
                status TEXT NOT NULL,
                provider_message_id TEXT,
                related_visit_id INTEGER,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS whatsapp_webhook_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_type TEXT NOT NULL,
                from_phone TEXT,
                payload_json TEXT,
                created_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_properties_broker ON properties(broker_id);
            CREATE INDEX IF NOT EXISTS idx_properties_status ON properties(status);
            CREATE INDEX IF NOT EXISTS idx_slots_broker ON slots(broker_id);
            CREATE INDEX IF NOT EXISTS idx_slots_status ON slots(status);
            CREATE INDEX IF NOT EXISTS idx_visits_broker_status ON visits(broker_id, status);
            CREATE INDEX IF NOT EXISTS idx_incidents_status ON cancellation_incidents(status);
            CREATE INDEX IF NOT EXISTS idx_flags_broker_status ON broker_flags(broker_id, status);
            CREATE INDEX IF NOT EXISTS idx_whatsapp_messages_to_phone ON whatsapp_messages(to_phone);
            CREATE INDEX IF NOT EXISTS idx_whatsapp_messages_visit ON whatsapp_messages(related_visit_id);
            CREATE INDEX IF NOT EXISTS idx_whatsapp_webhook_event_type ON whatsapp_webhook_events(event_type);
            """
        )

        now = to_iso(now_local())
        users_seed = [
            ("Broker Jaipur", "broker.jaipur@example.com", "broker123", ROLE_BROKER, "Jaipur"),
            ("Broker Nagpur", "broker.nagpur@example.com", "broker123", ROLE_BROKER, "Nagpur"),
            ("RM Jaipur", "rm.jaipur@example.com", "rm123", ROLE_RM, "Jaipur"),
            ("RM Nagpur", "rm.nagpur@example.com", "rm123", ROLE_RM, "Nagpur"),
            ("SRM Ops", "srm.ops@example.com", "srm123", ROLE_SRM, None),
        ]
        for name, email, password, role, city in users_seed:
            conn.execute(
                """
                INSERT INTO users(name, email, password_hash, role, city, active, created_at)
                VALUES(?, ?, ?, ?, ?, 1, ?)
                ON CONFLICT(email) DO NOTHING
                """,
                (name, email, hash_password(password), role, city, now),
            )

        brokers = conn.execute("SELECT id, city FROM users WHERE role = ?", (ROLE_BROKER,)).fetchall()
        rms = conn.execute("SELECT id, city FROM users WHERE role = ?", (ROLE_RM,)).fetchall()
        rm_by_city = {row["city"]: row["id"] for row in rms}
        for broker in brokers:
            rm_id = rm_by_city.get(broker["city"])
            if rm_id:
                conn.execute(
                    """
                    INSERT INTO rm_assignments(rm_id, broker_id, created_at)
                    VALUES(?, ?, ?)
                    ON CONFLICT(rm_id, broker_id) DO NOTHING
                    """,
                    (rm_id, broker["id"], now),
                )

        property_count = conn.execute("SELECT COUNT(*) AS c FROM properties").fetchone()["c"]
        if property_count == 0:
            sample_properties = [
                {
                    "broker_email": "broker.jaipur@example.com",
                    "title": "Park View Residency",
                    "asset_type": "Apartment",
                    "configuration": "3 BHK",
                    "location_text": "Mansarovar, Jaipur",
                    "city": "Jaipur",
                    "price": 5600000,
                    "latitude": 26.8504,
                    "longitude": 75.7672,
                    "amenities": "Lift, Parking, Gym",
                    "image_url": "https://example.com/jaipur-park-view.jpg",
                },
                {
                    "broker_email": "broker.nagpur@example.com",
                    "title": "Orange Heights",
                    "asset_type": "Apartment",
                    "configuration": "2 BHK",
                    "location_text": "Dharampeth, Nagpur",
                    "city": "Nagpur",
                    "price": 4200000,
                    "latitude": 21.1458,
                    "longitude": 79.0832,
                    "amenities": "Security, Parking",
                    "image_url": "https://example.com/nagpur-orange-heights.jpg",
                },
            ]
            for item in sample_properties:
                broker_id = conn.execute(
                    "SELECT id FROM users WHERE email = ?", (item["broker_email"],)
                ).fetchone()["id"]
                conn.execute(
                    """
                    INSERT INTO properties(
                        broker_id, title, asset_type, configuration, spec_value, spec_unit, bhk, area_value,
                        location_text, city, price, maps_url, latitude, longitude, amenities, image_url,
                        status, hidden_from_customers, duplicate_score, primary_property_id, created_at, updated_at
                    )
                    VALUES(?, ?, ?, ?, NULL, NULL, ?, NULL, ?, ?, ?, NULL, ?, ?, ?, ?, ?, 0, NULL, NULL, ?, ?)
                    """,
                    (
                        broker_id,
                        item["title"],
                        item["asset_type"],
                        item["configuration"],
                        item["configuration"],
                        item["location_text"],
                        item["city"],
                        item["price"],
                        item["latitude"],
                        item["longitude"],
                        item["amenities"],
                        item["image_url"],
                        PROPERTY_STATUS_ACTIVE,
                        now,
                        now,
                    ),
                )

        slot_count = conn.execute("SELECT COUNT(*) AS c FROM slots").fetchone()["c"]
        if slot_count == 0:
            for broker in brokers:
                start = now_local().replace(hour=11, minute=0, second=0, microsecond=0) + timedelta(days=1)
                end = start + timedelta(hours=2)
                conn.execute(
                    """
                    INSERT INTO slots(broker_id, city, start_at, end_at, status, created_at, updated_at)
                    VALUES(?, ?, ?, ?, ?, ?, ?)
                    """,
                    (broker["id"], broker["city"], to_iso(start), to_iso(end), SLOT_STATUS_OPEN, now, now),
                )

        seed_whatsapp_templates(conn)

        conn.commit()


def record_event(conn: sqlite3.Connection, event_type: str, entity_type: str, entity_id: int | None, payload: dict | None = None) -> None:
    conn.execute(
        """
        INSERT INTO events(event_type, entity_type, entity_id, payload_json, created_at)
        VALUES(?, ?, ?, ?, ?)
        """,
        (event_type, entity_type, entity_id, json.dumps(payload or {}), to_iso(now_local())),
    )


def calc_rm_sla(raised_at: datetime) -> datetime:
    if raised_at.hour < 12:
        return raised_at + timedelta(hours=12)
    return raised_at + timedelta(hours=24)


def calc_srm_sla(escalated_at: datetime) -> datetime:
    if escalated_at.hour < 12:
        return escalated_at + timedelta(hours=12)
    return escalated_at + timedelta(hours=24)


def calculate_tour_duration_minutes(property_count: int) -> int:
    safe_count = max(1, property_count)
    return 120 + ((safe_count - 1) * 45)


class SafeTemplateDict(dict):
    def __missing__(self, key: str) -> str:
        return "{" + key + "}"


def seed_whatsapp_templates(conn: sqlite3.Connection) -> None:
    now = to_iso(now_local())
    templates = [
        (
            "visit_confirmation",
            WHATSAPP_LANG,
            "Visit #{visit_id} confirmed for {property_title} on {start_at}. Reply CANCEL {visit_id} to cancel.",
        ),
        (
            "visit_rescheduled_confirmation",
            WHATSAPP_LANG,
            "Visit #{new_visit_id} rescheduled from #{old_visit_id}. New slot: {start_at} for {property_title}.",
        ),
        (
            "customer_cancel_confirmation",
            WHATSAPP_LANG,
            "Your visit #{visit_id} has been cancelled. You can rebook anytime from available slots.",
        ),
        (
            "broker_cancel_with_priority",
            WHATSAPP_LANG,
            "Sorry, broker cancelled visit #{visit_id}. You have priority rebooking for 48 hours till {priority_rebook_until}.",
        ),
        (
            "broker_cancel_without_priority",
            WHATSAPP_LANG,
            "Sorry, broker cancelled visit #{visit_id}. Please select a new slot from available options.",
        ),
        (
            "otp_verification",
            WHATSAPP_LANG,
            "Your site visit OTP is {otp}. It expires in 2 minutes.",
        ),
        (
            "customer_help",
            WHATSAPP_LANG,
            "Commands: CANCEL <visit_id>, RESCHEDULE <visit_id> <slot_id>.",
        ),
    ]

    for template_name, language, body in templates:
        conn.execute(
            """
            INSERT INTO whatsapp_templates(template_name, language, body, active, created_at)
            VALUES(?, ?, ?, 1, ?)
            ON CONFLICT(template_name, language) DO NOTHING
            """,
            (template_name, language, body, now),
        )


def render_template(body: str, context: dict | None = None) -> str:
    return body.format_map(SafeTemplateDict(context or {}))


def queue_whatsapp_message(
    conn: sqlite3.Connection,
    *,
    direction: str,
    source: str,
    to_phone: str | None,
    from_phone: str | None,
    template_name: str | None,
    language: str | None,
    message_text: str,
    payload: dict | None,
    status: str,
    related_visit_id: int | None,
) -> dict:
    provider_message_id = f"wa_{uuid.uuid4().hex[:14]}"
    now = to_iso(now_local())
    conn.execute(
        """
        INSERT INTO whatsapp_messages(
            direction, source, provider, to_phone, from_phone, template_name, language, message_text,
            payload_json, status, provider_message_id, related_visit_id, created_at
        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            direction,
            source,
            WHATSAPP_PROVIDER,
            normalize_phone(to_phone) if to_phone else None,
            normalize_phone(from_phone) if from_phone else None,
            template_name,
            language,
            message_text,
            json.dumps(payload or {}),
            status,
            provider_message_id,
            related_visit_id,
            now,
        ),
    )
    message_id = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
    record_event(
        conn,
        "whatsapp_message_logged",
        "whatsapp_message",
        message_id,
        {
            "direction": direction,
            "template_name": template_name,
            "to_phone": normalize_phone(to_phone) if to_phone else None,
            "status": status,
            "related_visit_id": related_visit_id,
        },
    )
    return {"id": message_id, "provider_message_id": provider_message_id}


def send_whatsapp_template(
    conn: sqlite3.Connection,
    *,
    to_phone: str,
    template_name: str,
    context: dict | None = None,
    related_visit_id: int | None = None,
    source: str = "system",
) -> dict:
    row = conn.execute(
        """
        SELECT body, language
        FROM whatsapp_templates
        WHERE template_name = ? AND language = ? AND active = 1
        LIMIT 1
        """,
        (template_name, WHATSAPP_LANG),
    ).fetchone()

    if row:
        body = row["body"]
        language = row["language"]
    else:
        body = "Notification: {message}"
        language = WHATSAPP_LANG

    message_text = render_template(body, context or {})
    return queue_whatsapp_message(
        conn,
        direction="outbound",
        source=source,
        to_phone=to_phone,
        from_phone=None,
        template_name=template_name,
        language=language,
        message_text=message_text,
        payload={"context": context or {}},
        status="queued",
        related_visit_id=related_visit_id,
    )


def log_whatsapp_webhook_event(conn: sqlite3.Connection, event_type: str, from_phone: str | None, payload: dict | None) -> int:
    conn.execute(
        """
        INSERT INTO whatsapp_webhook_events(event_type, from_phone, payload_json, created_at)
        VALUES(?, ?, ?, ?)
        """,
        (event_type, normalize_phone(from_phone) if from_phone else None, json.dumps(payload or {}), to_iso(now_local())),
    )
    return conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]


def send_visit_whatsapp(
    conn: sqlite3.Connection,
    *,
    visit_id: int,
    template_name: str,
    source: str = "system",
    extra_context: dict | None = None,
) -> dict | None:
    row = conn.execute(
        """
        SELECT
            v.id AS visit_id,
            v.start_at,
            v.priority_rebook_until,
            c.phone_norm,
            p.title AS property_title
        FROM visits v
        JOIN customers c ON c.id = v.customer_id
        JOIN properties p ON p.id = v.property_id
        WHERE v.id = ?
        """,
        (visit_id,),
    ).fetchone()
    if not row:
        return None

    context = {
        "visit_id": row["visit_id"],
        "property_title": row["property_title"],
        "start_at": row["start_at"],
        "priority_rebook_until": row["priority_rebook_until"] or "-",
    }
    context.update(extra_context or {})
    return send_whatsapp_template(
        conn,
        to_phone=row["phone_norm"],
        template_name=template_name,
        context=context,
        related_visit_id=visit_id,
        source=source,
    )


def create_scheduled_visit(
    conn: sqlite3.Connection,
    *,
    slot_row: sqlite3.Row,
    property_row: sqlite3.Row,
    customer_id: int,
    customer_requirements: str,
    source: str,
    previous_visit_id: int | None = None,
) -> int:
    rm = conn.execute(
        "SELECT rm_id FROM rm_assignments WHERE broker_id = ? LIMIT 1",
        (slot_row["broker_id"],),
    ).fetchone()
    rm_id = rm["rm_id"] if rm else None

    now_dt = now_local()
    now = to_iso(now_dt)
    conn.execute(
        """
        INSERT INTO visits(
            slot_id, property_id, broker_id, rm_id, customer_id, customer_requirements,
            start_at, end_at, status, created_at, updated_at
        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            slot_row["id"],
            property_row["id"],
            slot_row["broker_id"],
            rm_id,
            customer_id,
            customer_requirements,
            slot_row["start_at"],
            slot_row["end_at"],
            VISIT_STATUS_SCHEDULED,
            now,
            now,
        ),
    )
    visit_id = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]

    conn.execute(
        "UPDATE slots SET status = ?, updated_at = ? WHERE id = ?",
        (SLOT_STATUS_BOOKED, now, slot_row["id"]),
    )

    start_at = parse_iso(slot_row["start_at"])
    reminder_due = now_dt
    if start_at:
        reminder_due = now_dt if (start_at - now_dt) < timedelta(hours=24) else (start_at - timedelta(days=1))
    record_event(
        conn,
        "rm_reminder_scheduled",
        "visit",
        visit_id,
        {
            "rm_id": rm_id,
            "due_at": to_iso(reminder_due),
            "immediate": bool(start_at and (start_at - now_dt) < timedelta(hours=24)),
            "source": source,
        },
    )
    record_event(
        conn,
        "visit_scheduled",
        "visit",
        visit_id,
        {"source": source, "previous_visit_id": previous_visit_id},
    )
    send_visit_whatsapp(conn, visit_id=visit_id, template_name="visit_confirmation", source=source)
    return visit_id


def get_rebooking_slots_for_visit(conn: sqlite3.Connection, visit_row: sqlite3.Row) -> list[dict]:
    now = to_iso(now_local())
    primary_broker_id = visit_row["broker_id"]
    broker_to_property: dict[int, int] = {primary_broker_id: visit_row["property_id"]}

    backup_rows = conn.execute(
        """
        SELECT id, broker_id
        FROM properties
        WHERE primary_property_id = ?
          AND status IN (?, ?)
        """,
        (visit_row["property_id"], PROPERTY_STATUS_BACKUP, PROPERTY_STATUS_ACTIVE),
    ).fetchall()
    for row in backup_rows:
        if row["broker_id"] not in broker_to_property:
            broker_to_property[row["broker_id"]] = row["id"]

    broker_ids = list(broker_to_property.keys())
    if not broker_ids:
        return []

    slots = conn.execute(
        f"""
        SELECT *
        FROM slots
        WHERE broker_id IN ({','.join('?' for _ in broker_ids)})
          AND status = ?
          AND start_at >= ?
        ORDER BY start_at ASC
        LIMIT 20
        """,
        (*broker_ids, SLOT_STATUS_OPEN, now),
    ).fetchall()

    available: list[dict] = []
    for slot in slots:
        mapped_property = broker_to_property.get(slot["broker_id"])
        if not mapped_property:
            continue
        available.append(
            {
                "slot_id": slot["id"],
                "broker_id": slot["broker_id"],
                "property_id": mapped_property,
                "start_at": slot["start_at"],
                "end_at": slot["end_at"],
                "city": slot["city"],
                "mode": "primary" if slot["broker_id"] == primary_broker_id else "backup",
            }
        )
    return available


def cancel_visit_by_customer(
    conn: sqlite3.Connection,
    *,
    visit_id: int,
    customer_phone: str,
    reason: str,
    source: str,
) -> dict:
    phone = normalize_phone(customer_phone)
    visit = conn.execute(
        """
        SELECT
            v.*,
            c.phone_norm,
            c.name AS customer_name
        FROM visits v
        JOIN customers c ON c.id = v.customer_id
        WHERE v.id = ?
        """,
        (visit_id,),
    ).fetchone()
    if not visit:
        raise ValueError("Visit not found")
    if visit["phone_norm"] != phone:
        raise ValueError("Phone does not match visit")
    if visit["status"] != VISIT_STATUS_SCHEDULED:
        raise ValueError("Only scheduled visits can be cancelled")

    now = to_iso(now_local())
    conn.execute(
        """
        UPDATE visits
        SET status = ?, cancelled_by = ?, cancellation_reason = ?, updated_at = ?
        WHERE id = ?
        """,
        (VISIT_STATUS_CANCELLED_CUSTOMER, "customer", reason or "customer_requested", now, visit_id),
    )
    conn.execute(
        "UPDATE slots SET status = ?, updated_at = ? WHERE id = ?",
        (SLOT_STATUS_OPEN, now, visit["slot_id"]),
    )
    record_event(
        conn,
        "customer_visit_cancelled",
        "visit",
        visit_id,
        {"source": source, "reason": reason or "customer_requested"},
    )
    send_visit_whatsapp(conn, visit_id=visit_id, template_name="customer_cancel_confirmation", source=source)
    return {"visit_id": visit_id, "status": VISIT_STATUS_CANCELLED_CUSTOMER}


def reschedule_visit_by_customer(
    conn: sqlite3.Connection,
    *,
    visit_id: int,
    customer_phone: str,
    target_slot_id: int,
    reason: str,
    source: str,
) -> dict:
    phone = normalize_phone(customer_phone)
    visit = conn.execute(
        """
        SELECT
            v.*,
            c.phone_norm
        FROM visits v
        JOIN customers c ON c.id = v.customer_id
        WHERE v.id = ?
        """,
        (visit_id,),
    ).fetchone()
    if not visit:
        raise ValueError("Visit not found")
    if visit["phone_norm"] != phone:
        raise ValueError("Phone does not match visit")
    if visit["status"] != VISIT_STATUS_SCHEDULED:
        raise ValueError("Only scheduled visits can be rescheduled")

    allowed_slots = get_rebooking_slots_for_visit(conn, visit)
    selected = next((slot for slot in allowed_slots if slot["slot_id"] == target_slot_id), None)
    if not selected:
        raise ValueError("Selected slot is not allowed for this visit")

    slot = conn.execute(
        "SELECT * FROM slots WHERE id = ?",
        (target_slot_id,),
    ).fetchone()
    if not slot or slot["status"] != SLOT_STATUS_OPEN:
        raise ValueError("Target slot is no longer available")

    property_row = conn.execute(
        "SELECT * FROM properties WHERE id = ?",
        (selected["property_id"],),
    ).fetchone()
    if not property_row:
        raise ValueError("Property mapping not found for selected slot")

    now = to_iso(now_local())
    conn.execute(
        """
        UPDATE visits
        SET status = ?, cancelled_by = ?, cancellation_reason = ?, updated_at = ?
        WHERE id = ?
        """,
        (
            VISIT_STATUS_RESCHEDULED,
            "customer",
            f"rescheduled_by_customer:{reason or 'customer_requested'}",
            now,
            visit_id,
        ),
    )
    conn.execute(
        "UPDATE slots SET status = ?, updated_at = ? WHERE id = ?",
        (SLOT_STATUS_OPEN, now, visit["slot_id"]),
    )

    new_visit_id = create_scheduled_visit(
        conn,
        slot_row=slot,
        property_row=property_row,
        customer_id=visit["customer_id"],
        customer_requirements=visit["customer_requirements"] or "",
        source=source,
        previous_visit_id=visit_id,
    )
    send_visit_whatsapp(
        conn,
        visit_id=new_visit_id,
        template_name="visit_rescheduled_confirmation",
        source=source,
        extra_context={"old_visit_id": visit_id, "new_visit_id": new_visit_id},
    )
    record_event(
        conn,
        "customer_visit_rescheduled",
        "visit",
        visit_id,
        {"new_visit_id": new_visit_id, "source": source},
    )
    return {"old_visit_id": visit_id, "new_visit_id": new_visit_id}


def csv_text(headers: list[str], rows: list[list]) -> str:
    buff = io.StringIO()
    writer = csv.writer(buff)
    writer.writerow(headers)
    writer.writerows(rows)
    return buff.getvalue()


def build_funnel_report(conn: sqlite3.Connection) -> dict:
    lead_count = conn.execute("SELECT COUNT(*) AS c FROM leads").fetchone()["c"]
    scheduled = conn.execute(
        "SELECT COUNT(*) AS c FROM visits WHERE status = ?",
        (VISIT_STATUS_SCHEDULED,),
    ).fetchone()["c"]
    completed_unique = conn.execute(
        "SELECT COUNT(*) AS c FROM visits WHERE status = ? AND is_unique_visit = 1",
        (VISIT_STATUS_COMPLETED,),
    ).fetchone()["c"]
    completed_non_unique = conn.execute(
        "SELECT COUNT(*) AS c FROM visits WHERE status = ? AND is_unique_visit = 0",
        (VISIT_STATUS_COMPLETED,),
    ).fetchone()["c"]
    broker_cancellations_lt24h = conn.execute(
        """
        SELECT COUNT(*) AS c
        FROM cancellation_incidents
        WHERE within_24h = 1 AND is_booked = 1
        """,
    ).fetchone()["c"]
    customer_cancellations = conn.execute(
        "SELECT COUNT(*) AS c FROM visits WHERE status = ?",
        (VISIT_STATUS_CANCELLED_CUSTOMER,),
    ).fetchone()["c"]
    customer_reschedules = conn.execute(
        "SELECT COUNT(*) AS c FROM visits WHERE status = ?",
        (VISIT_STATUS_RESCHEDULED,),
    ).fetchone()["c"]

    return {
        "lead_count": lead_count,
        "scheduled_visits": scheduled,
        "completed_unique": completed_unique,
        "completed_non_unique": completed_non_unique,
        "broker_cancellations_lt24h": broker_cancellations_lt24h,
        "customer_cancellations": customer_cancellations,
        "customer_reschedules": customer_reschedules,
    }


def build_broker_reliability_report(conn: sqlite3.Connection) -> list[dict]:
    brokers = conn.execute(
        "SELECT id, name, city, active FROM users WHERE role = ? ORDER BY name ASC",
        (ROLE_BROKER,),
    ).fetchall()
    report: list[dict] = []
    for broker in brokers:
        total = conn.execute(
            "SELECT COUNT(*) AS c FROM visits WHERE broker_id = ?",
            (broker["id"],),
        ).fetchone()["c"]
        completed = conn.execute(
            "SELECT COUNT(*) AS c FROM visits WHERE broker_id = ? AND status = ?",
            (broker["id"], VISIT_STATUS_COMPLETED),
        ).fetchone()["c"]
        broker_cancelled = conn.execute(
            "SELECT COUNT(*) AS c FROM visits WHERE broker_id = ? AND status = ?",
            (broker["id"], VISIT_STATUS_CANCELLED_BROKER),
        ).fetchone()["c"]
        late_cancel_incidents = conn.execute(
            """
            SELECT COUNT(*) AS c
            FROM cancellation_incidents
            WHERE broker_id = ? AND within_24h = 1 AND is_booked = 1
            """,
            (broker["id"],),
        ).fetchone()["c"]
        active_flags = conn.execute(
            "SELECT COUNT(*) AS c FROM broker_flags WHERE broker_id = ? AND status = ?",
            (broker["id"], FLAG_ACTIVE),
        ).fetchone()["c"]
        completion_rate = round((completed / total) * 100, 2) if total else 0.0

        report.append(
            {
                "broker_id": broker["id"],
                "broker_name": broker["name"],
                "city": broker["city"],
                "active": broker["active"],
                "total_visits": total,
                "completed_visits": completed,
                "completion_rate_pct": completion_rate,
                "broker_cancelled_visits": broker_cancelled,
                "late_cancel_incidents": late_cancel_incidents,
                "active_flags": active_flags,
            }
        )
    return report


def decay_flags(conn: sqlite3.Connection) -> None:
    now = now_local()
    conn.execute(
        """
        UPDATE broker_flags
        SET status = ?
        WHERE status = ? AND decays_at <= ?
        """,
        (FLAG_DECAYED, FLAG_ACTIVE, to_iso(now)),
    )


def active_flag_count(conn: sqlite3.Connection, broker_id: int) -> int:
    row = conn.execute(
        """
        SELECT COUNT(*) AS c
        FROM broker_flags
        WHERE broker_id = ? AND status = ?
        """,
        (broker_id, FLAG_ACTIVE),
    ).fetchone()
    return row["c"] if row else 0


def apply_flag(conn: sqlite3.Connection, broker_id: int, incident_id: int | None, reason: str) -> dict:
    decay_flags(conn)
    count = active_flag_count(conn, broker_id)
    level = count + 1
    now = now_local()
    decays_at = now + timedelta(days=90)

    conn.execute(
        """
        INSERT INTO broker_flags(broker_id, incident_id, level, reason, status, created_at, decays_at)
        VALUES(?, ?, ?, ?, ?, ?, ?)
        """,
        (broker_id, incident_id, level, reason, FLAG_ACTIVE, to_iso(now), to_iso(decays_at)),
    )

    if level == 2:
        conn.execute(
            """
            INSERT INTO broker_penalties(broker_id, year, month, reason, created_at)
            VALUES(?, ?, ?, ?, ?)
            ON CONFLICT(broker_id, year, month, reason) DO NOTHING
            """,
            (broker_id, now.year, now.month, "month_incentive_block_due_to_second_flag", to_iso(now)),
        )

    if level >= 3:
        conn.execute("UPDATE users SET active = 0 WHERE id = ?", (broker_id,))
        record_event(
            conn,
            "broker_removed_after_third_flag",
            "broker",
            broker_id,
            {"level": level, "reason": reason},
        )

    record_event(
        conn,
        "broker_flag_added",
        "broker",
        broker_id,
        {"level": level, "reason": reason, "incident_id": incident_id},
    )
    return {"level": level, "decays_at": to_iso(decays_at)}


def process_incident_escalations(conn: sqlite3.Connection) -> None:
    now = now_local()
    pending = conn.execute(
        """
        SELECT id
        FROM cancellation_incidents
        WHERE status = ? AND escalated_to_srm = 0 AND sla_due_at IS NOT NULL AND sla_due_at <= ?
        """,
        (INCIDENT_PENDING_RM, to_iso(now)),
    ).fetchall()

    for row in pending:
        srm_due_at = calc_srm_sla(now)
        conn.execute(
            """
            UPDATE cancellation_incidents
            SET status = ?, escalated_to_srm = 1, srm_due_at = ?, updated_at = ?
            WHERE id = ?
            """,
            (INCIDENT_ESCALATED, to_iso(srm_due_at), to_iso(now), row["id"]),
        )
        record_event(
            conn,
            "incident_escalated_to_srm",
            "cancellation_incident",
            row["id"],
            {"srm_due_at": to_iso(srm_due_at)},
        )


def parse_request_body(handler: BaseHTTPRequestHandler) -> dict:
    length = int(handler.headers.get("Content-Length", "0"))
    if length <= 0:
        return {}
    raw = handler.rfile.read(length)
    if not raw:
        return {}
    try:
        return json.loads(raw.decode("utf-8"))
    except json.JSONDecodeError:
        return {}


def compute_similarity(new_prop: sqlite3.Row, old_prop: sqlite3.Row) -> float:
    image_score = 0.0
    new_img = normalize_text(new_prop["image_url"])
    old_img = normalize_text(old_prop["image_url"])
    if new_img and old_img:
        if new_img == old_img:
            image_score = 1.0
        else:
            image_score = max(
                text_similarity(os.path.basename(new_img), os.path.basename(old_img)) * 0.8,
                text_similarity(new_img, old_img) * 0.5,
            )

    loc_score = text_similarity(new_prop["location_text"], old_prop["location_text"])
    if new_prop["latitude"] is not None and new_prop["longitude"] is not None and old_prop["latitude"] is not None and old_prop["longitude"] is not None:
        distance = haversine_meters(new_prop["latitude"], new_prop["longitude"], old_prop["latitude"], old_prop["longitude"])
        geo_score = 1.0 if distance <= 60 else max(0.0, 1.0 - (distance / 4000.0))
        loc_score = max(loc_score, geo_score)

    type_score = 1.0 if normalize_text(new_prop["asset_type"]) == normalize_text(old_prop["asset_type"]) else 0.0
    config_score = text_similarity(new_prop["configuration"], old_prop["configuration"])

    area_score = 0.0
    if new_prop["area_value"] and old_prop["area_value"]:
        diff = abs(float(new_prop["area_value"]) - float(old_prop["area_value"]))
        area_score = max(0.0, 1.0 - diff / max(float(new_prop["area_value"]), float(old_prop["area_value"]), 1.0))
    specifics_score = (type_score * 0.45) + (config_score * 0.4) + (area_score * 0.15)

    price_score = 0.0
    if new_prop["price"] and old_prop["price"]:
        diff = abs(float(new_prop["price"]) - float(old_prop["price"]))
        price_score = max(0.0, 1.0 - diff / max(float(new_prop["price"]), float(old_prop["price"]), 1.0))

    total = (
        image_score * 0.35
        + loc_score * 0.25
        + specifics_score * 0.25
        + price_score * 0.15
    )
    return round(total * 100.0, 2)


def run_duplicate_checks(conn: sqlite3.Connection, property_id: int) -> dict:
    prop = conn.execute("SELECT * FROM properties WHERE id = ?", (property_id,)).fetchone()
    if not prop:
        return {"matched": False}

    candidates = conn.execute(
        """
        SELECT *
        FROM properties
        WHERE id != ?
          AND city = ?
          AND status IN (?, ?, ?, ?)
        """,
        (
            property_id,
            prop["city"],
            PROPERTY_STATUS_ACTIVE,
            PROPERTY_STATUS_BACKUP,
            PROPERTY_STATUS_HIDDEN_DUPLICATE,
            PROPERTY_STATUS_SOLD,
        ),
    ).fetchall()

    best = None
    best_score = 0.0
    for candidate in candidates:
        score = compute_similarity(prop, candidate)
        if score > best_score:
            best_score = score
            best = candidate

    if not best or best_score <= 75.0:
        conn.execute(
            """
            UPDATE properties
            SET status = ?, hidden_from_customers = 0, duplicate_score = NULL, updated_at = ?
            WHERE id = ?
            """,
            (PROPERTY_STATUS_ACTIVE, to_iso(now_local()), property_id),
        )
        return {"matched": False, "score": best_score}

    auto_hidden = 1 if best_score > 95.0 else 0
    now = to_iso(now_local())

    older = best if parse_iso(best["created_at"]) <= parse_iso(prop["created_at"]) else prop
    primary_property_id = older["id"]

    conn.execute(
        """
        UPDATE properties
        SET status = ?, hidden_from_customers = 1, duplicate_score = ?, primary_property_id = ?, updated_at = ?
        WHERE id = ?
        """,
        (PROPERTY_STATUS_HIDDEN_DUPLICATE, best_score, primary_property_id, now, property_id),
    )

    conn.execute(
        """
        INSERT INTO duplicate_review_queue(
            property_id, matched_property_id, similarity, auto_hidden, status, created_at, updated_at
        ) VALUES(?, ?, ?, ?, 'pending', ?, ?)
        """,
        (property_id, best["id"], best_score, auto_hidden, now, now),
    )

    return {
        "matched": True,
        "score": best_score,
        "matched_property_id": best["id"],
        "auto_hidden": bool(auto_hidden),
    }


def require_role(user: sqlite3.Row, allowed: list[str]) -> bool:
    return user and user["role"] in allowed


def parse_query(path: str) -> dict:
    return {k: v[0] for k, v in parse_qs(urlparse(path).query).items()}


def import_leads_from_csv() -> dict:
    if not LEADS_IMPORT_FILE.exists():
        return {"imported": 0, "updated": 0, "status": "file_not_found"}

    imported = 0
    updated = 0
    now = to_iso(now_local())

    with connect_db() as conn:
        with LEADS_IMPORT_FILE.open("r", encoding="utf-8") as fp:
            reader = csv.DictReader(fp)
            for row in reader:
                phone = normalize_phone(row.get("phone"))
                if not phone:
                    continue

                customer = conn.execute(
                    "SELECT id FROM customers WHERE phone_norm = ?", (phone,)
                ).fetchone()
                if not customer:
                    conn.execute(
                        "INSERT INTO customers(name, phone_norm, created_at) VALUES(?, ?, ?)",
                        (row.get("name") or "", phone, now),
                    )
                    customer_id = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
                else:
                    customer_id = customer["id"]
                    if row.get("name"):
                        conn.execute(
                            "UPDATE customers SET name = ? WHERE id = ?",
                            (row.get("name"), customer_id),
                        )

                payload = (
                    customer_id,
                    row.get("city") or "",
                    row.get("location_pref") or "",
                    row.get("config_pref") or "",
                    float(row.get("budget_min") or 0),
                    float(row.get("budget_max") or 0),
                    row.get("requirement_text") or "",
                    "excel_sync",
                    now,
                    now,
                )

                exists = conn.execute(
                    """
                    SELECT id
                    FROM leads
                    WHERE customer_id = ?
                      AND city = ?
                      AND location_pref = ?
                      AND config_pref = ?
                      AND budget_min = ?
                      AND budget_max = ?
                    """,
                    payload[:6],
                ).fetchone()

                if exists:
                    conn.execute(
                        """
                        UPDATE leads
                        SET requirement_text = ?, source = ?, last_synced_at = ?
                        WHERE id = ?
                        """,
                        (payload[6], payload[7], payload[8], exists["id"]),
                    )
                    updated += 1
                else:
                    conn.execute(
                        """
                        INSERT INTO leads(
                            customer_id, city, location_pref, config_pref, budget_min, budget_max,
                            requirement_text, source, last_synced_at, created_at
                        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        payload,
                    )
                    imported += 1

        conn.commit()

    return {"imported": imported, "updated": updated, "status": "ok"}


class LeadSyncThread(threading.Thread):
    daemon = True

    def __init__(self, stop_event: threading.Event):
        super().__init__()
        self.stop_event = stop_event

    def run(self) -> None:
        while not self.stop_event.is_set():
            try:
                import_leads_from_csv()
            except Exception as exc:
                print(f"[lead-sync] error: {exc}")
            self.stop_event.wait(LEAD_SYNC_INTERVAL_SECONDS)


class AppHandler(BaseHTTPRequestHandler):
    server_version = "ProptechMVP/1.0"

    def _send_json(self, status: int, payload: dict) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _send_csv(self, filename: str, csv_payload: str) -> None:
        body = csv_payload.encode("utf-8")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/csv; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _send_file(self, file_path: Path) -> None:
        if not file_path.exists() or not file_path.is_file():
            self.send_error(HTTPStatus.NOT_FOUND, "Not Found")
            return

        mime = "text/plain"
        if file_path.suffix == ".html":
            mime = "text/html; charset=utf-8"
        elif file_path.suffix == ".css":
            mime = "text/css; charset=utf-8"
        elif file_path.suffix == ".js":
            mime = "application/javascript; charset=utf-8"
        elif file_path.suffix == ".json":
            mime = "application/json; charset=utf-8"

        data = file_path.read_bytes()
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", mime)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _auth_user(self) -> sqlite3.Row | None:
        auth_header = self.headers.get("Authorization", "")
        if not auth_header.startswith("Bearer "):
            return None
        token = auth_header.split(" ", 1)[1].strip()
        if not token:
            return None

        with connect_db() as conn:
            row = conn.execute(
                """
                SELECT u.*
                FROM sessions s
                JOIN users u ON u.id = s.user_id
                WHERE s.token = ? AND s.expires_at > ?
                """,
                (token, to_iso(now_local())),
            ).fetchone()
            return row

    def _maintenance(self) -> None:
        with connect_db() as conn:
            decay_flags(conn)
            process_incident_escalations(conn)
            conn.commit()

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Authorization, Content-Type")
        self.end_headers()

    def do_GET(self):
        path = urlparse(self.path).path
        if path.startswith("/api/"):
            self.handle_api("GET")
            return

        if path == "/" or path == "":
            self._send_file(STATIC_DIR / "index.html")
            return

        clean = unquote(path.lstrip("/"))
        target = (STATIC_DIR / clean).resolve()
        if STATIC_DIR.resolve() not in target.parents and target != STATIC_DIR.resolve():
            self.send_error(HTTPStatus.FORBIDDEN, "Forbidden")
            return
        self._send_file(target)

    def do_POST(self):
        path = urlparse(self.path).path
        if path.startswith("/api/"):
            self.handle_api("POST")
            return
        self.send_error(HTTPStatus.NOT_FOUND, "Not Found")

    def _unauthorized(self):
        self._send_json(HTTPStatus.UNAUTHORIZED, {"ok": False, "error": "Unauthorized"})

    def _forbidden(self):
        self._send_json(HTTPStatus.FORBIDDEN, {"ok": False, "error": "Forbidden"})

    def handle_api(self, method: str) -> None:
        self._maintenance()
        path = urlparse(self.path).path
        query = parse_query(self.path)

        if method == "GET" and path == "/api/health":
            self._send_json(HTTPStatus.OK, {"ok": True, "service": "proptech-mvp", "time": to_iso(now_local())})
            return

        if method == "GET" and path == "/api/scheduling/duration":
            property_count = int(query.get("property_count") or 1)
            duration = calculate_tour_duration_minutes(property_count)
            self._send_json(
                HTTPStatus.OK,
                {
                    "ok": True,
                    "property_count": max(1, property_count),
                    "total_duration_minutes": duration,
                    "rule": "First property = 120 mins, each additional property = 45 mins",
                },
            )
            return

        if method == "GET" and path == "/api/customer/visits":
            customer_phone = normalize_phone(query.get("phone"))
            if not customer_phone:
                self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "phone query is required"})
                return
            with connect_db() as conn:
                rows = conn.execute(
                    """
                    SELECT
                        v.*,
                        p.title AS property_title,
                        p.location_text,
                        b.name AS broker_name,
                        c.phone_norm
                    FROM visits v
                    JOIN customers c ON c.id = v.customer_id
                    JOIN properties p ON p.id = v.property_id
                    JOIN users b ON b.id = v.broker_id
                    WHERE c.phone_norm = ?
                      AND v.status = ?
                    ORDER BY v.start_at ASC
                    """,
                    (customer_phone, VISIT_STATUS_SCHEDULED),
                ).fetchall()

                items = []
                for row in rows:
                    item = dict(row)
                    item["available_slots"] = get_rebooking_slots_for_visit(conn, row)
                    items.append(item)
            self._send_json(HTTPStatus.OK, {"ok": True, "items": items})
            return

        if method == "POST" and path == "/api/customer/visits/cancel":
            data = parse_request_body(self)
            visit_id = int(data.get("visit_id") or 0)
            customer_phone = normalize_phone(data.get("customer_phone"))
            reason = (data.get("reason") or "customer_requested").strip()
            if not visit_id or not customer_phone:
                self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "visit_id and customer_phone are required"})
                return
            with connect_db() as conn:
                try:
                    result = cancel_visit_by_customer(
                        conn,
                        visit_id=visit_id,
                        customer_phone=customer_phone,
                        reason=reason,
                        source="customer_self_service",
                    )
                except ValueError as exc:
                    self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": str(exc)})
                    return
                conn.commit()
            self._send_json(HTTPStatus.OK, {"ok": True, "result": result})
            return

        if method == "POST" and path == "/api/customer/visits/reschedule":
            data = parse_request_body(self)
            visit_id = int(data.get("visit_id") or 0)
            target_slot_id = int(data.get("target_slot_id") or 0)
            customer_phone = normalize_phone(data.get("customer_phone"))
            reason = (data.get("reason") or "customer_requested").strip()
            if not visit_id or not target_slot_id or not customer_phone:
                self._send_json(
                    HTTPStatus.BAD_REQUEST,
                    {"ok": False, "error": "visit_id, target_slot_id, customer_phone are required"},
                )
                return
            with connect_db() as conn:
                try:
                    result = reschedule_visit_by_customer(
                        conn,
                        visit_id=visit_id,
                        customer_phone=customer_phone,
                        target_slot_id=target_slot_id,
                        reason=reason,
                        source="customer_self_service",
                    )
                except ValueError as exc:
                    self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": str(exc)})
                    return
                conn.commit()
            self._send_json(HTTPStatus.OK, {"ok": True, "result": result})
            return

        if method == "POST" and path == "/api/integrations/whatsapp/webhook":
            data = parse_request_body(self)
            event_type = (data.get("event_type") or "unknown").strip().lower()
            from_phone = normalize_phone(data.get("from_phone"))
            message_text = (data.get("message_text") or "").strip()

            with connect_db() as conn:
                webhook_id = log_whatsapp_webhook_event(conn, event_type, from_phone, data)
                queue_whatsapp_message(
                    conn,
                    direction="inbound",
                    source="whatsapp_webhook",
                    to_phone=None,
                    from_phone=from_phone,
                    template_name=None,
                    language=WHATSAPP_LANG,
                    message_text=message_text,
                    payload=data,
                    status="received",
                    related_visit_id=None,
                )

                command_result = {"action": "logged"}
                if event_type == "message_received" and from_phone and message_text:
                    upper = message_text.strip().upper()
                    cancel_match = re.match(r"^CANCEL\\s+(\\d+)$", upper)
                    reschedule_match = re.match(r"^RESCHEDULE\\s+(\\d+)\\s+(\\d+)$", upper)
                    if upper == "HELP":
                        send_whatsapp_template(
                            conn,
                            to_phone=from_phone,
                            template_name="customer_help",
                            context={},
                            source="whatsapp_webhook",
                        )
                        command_result = {"action": "help_sent"}
                    elif cancel_match:
                        try:
                            result = cancel_visit_by_customer(
                                conn,
                                visit_id=int(cancel_match.group(1)),
                                customer_phone=from_phone,
                                reason="customer_whatsapp_cancel",
                                source="whatsapp_webhook",
                            )
                            command_result = {"action": "visit_cancelled", "result": result}
                        except ValueError as exc:
                            send_whatsapp_template(
                                conn,
                                to_phone=from_phone,
                                template_name="customer_help",
                                context={"message": str(exc)},
                                source="whatsapp_webhook",
                            )
                            command_result = {"action": "cancel_failed", "error": str(exc)}
                    elif reschedule_match:
                        try:
                            result = reschedule_visit_by_customer(
                                conn,
                                visit_id=int(reschedule_match.group(1)),
                                customer_phone=from_phone,
                                target_slot_id=int(reschedule_match.group(2)),
                                reason="customer_whatsapp_reschedule",
                                source="whatsapp_webhook",
                            )
                            command_result = {"action": "visit_rescheduled", "result": result}
                        except ValueError as exc:
                            send_whatsapp_template(
                                conn,
                                to_phone=from_phone,
                                template_name="customer_help",
                                context={"message": str(exc)},
                                source="whatsapp_webhook",
                            )
                            command_result = {"action": "reschedule_failed", "error": str(exc)}
                    else:
                        send_whatsapp_template(
                            conn,
                            to_phone=from_phone,
                            template_name="customer_help",
                            context={},
                            source="whatsapp_webhook",
                        )
                        command_result = {"action": "unknown_command_help_sent"}

                conn.commit()
            self._send_json(
                HTTPStatus.OK,
                {"ok": True, "webhook_event_id": webhook_id, "result": command_result},
            )
            return

        if method == "POST" and path == "/api/auth/login":
            data = parse_request_body(self)
            email = (data.get("email") or "").strip().lower()
            password = data.get("password") or ""

            with connect_db() as conn:
                user = conn.execute(
                    "SELECT * FROM users WHERE email = ?",
                    (email,),
                ).fetchone()
                if not user or user["password_hash"] != hash_password(password):
                    self._send_json(HTTPStatus.UNAUTHORIZED, {"ok": False, "error": "Invalid credentials"})
                    return
                if user["active"] == 0:
                    self._send_json(HTTPStatus.FORBIDDEN, {"ok": False, "error": "Broker removed after 3 flags"})
                    return

                token = secrets.token_hex(24)
                expires_at = now_local() + timedelta(hours=SESSION_HOURS)
                conn.execute(
                    "INSERT INTO sessions(user_id, token, expires_at, created_at) VALUES(?, ?, ?, ?)",
                    (user["id"], token, to_iso(expires_at), to_iso(now_local())),
                )
                conn.commit()

            self._send_json(
                HTTPStatus.OK,
                {
                    "ok": True,
                    "token": token,
                    "user": {
                        "id": user["id"],
                        "name": user["name"],
                        "email": user["email"],
                        "role": user["role"],
                        "city": user["city"],
                    },
                },
            )
            return

        user = self._auth_user()
        if not user:
            self._unauthorized()
            return

        if method == "GET" and path == "/api/auth/me":
            self._send_json(
                HTTPStatus.OK,
                {
                    "ok": True,
                    "user": {
                        "id": user["id"],
                        "name": user["name"],
                        "email": user["email"],
                        "role": user["role"],
                        "city": user["city"],
                        "active": user["active"],
                    },
                },
            )
            return

        if method == "GET" and path == "/api/dashboard":
            with connect_db() as conn:
                if user["role"] == ROLE_BROKER:
                    metrics = conn.execute(
                        """
                        SELECT
                            SUM(CASE WHEN status IN (?, ?, ?) THEN 1 ELSE 0 END) AS inventory_count,
                            SUM(CASE WHEN status = ? AND start_at >= ? THEN 1 ELSE 0 END) AS upcoming_visits
                        FROM visits
                        WHERE broker_id = ?
                        """,
                        (
                            VISIT_STATUS_SCHEDULED,
                            VISIT_STATUS_COMPLETED,
                            VISIT_STATUS_CANCELLED_BROKER,
                            VISIT_STATUS_SCHEDULED,
                            to_iso(now_local()),
                            user["id"],
                        ),
                    ).fetchone()

                    property_row = conn.execute(
                        "SELECT COUNT(*) AS c FROM properties WHERE broker_id = ? AND status IN (?, ?)",
                        (user["id"], PROPERTY_STATUS_ACTIVE, PROPERTY_STATUS_BACKUP),
                    ).fetchone()

                    active_flags = active_flag_count(conn, user["id"])
                    self._send_json(
                        HTTPStatus.OK,
                        {
                            "ok": True,
                            "metrics": {
                                "inventory": property_row["c"],
                                "upcoming_visits": metrics["upcoming_visits"] or 0,
                                "active_flags": active_flags,
                            },
                        },
                    )
                    return

                if user["role"] == ROLE_RM:
                    assigned_brokers = conn.execute(
                        "SELECT broker_id FROM rm_assignments WHERE rm_id = ?",
                        (user["id"],),
                    ).fetchall()
                    broker_ids = [row["broker_id"] for row in assigned_brokers]
                    broker_placeholders = ",".join("?" for _ in broker_ids) or "NULL"

                    duplicate_pending = 0
                    emergency_pending = 0
                    if broker_ids:
                        duplicate_pending = conn.execute(
                            f"""
                            SELECT COUNT(*) AS c
                            FROM duplicate_review_queue dq
                            JOIN properties p ON p.id = dq.property_id
                            WHERE dq.status = 'pending' AND p.broker_id IN ({broker_placeholders})
                            """,
                            tuple(broker_ids),
                        ).fetchone()["c"]

                        emergency_pending = conn.execute(
                            f"""
                            SELECT COUNT(*) AS c
                            FROM cancellation_incidents
                            WHERE status = ? AND broker_id IN ({broker_placeholders})
                            """,
                            (INCIDENT_PENDING_RM, *broker_ids),
                        ).fetchone()["c"]

                    self._send_json(
                        HTTPStatus.OK,
                        {
                            "ok": True,
                            "metrics": {
                                "assigned_brokers": len(broker_ids),
                                "duplicate_queue": duplicate_pending,
                                "emergency_queue": emergency_pending,
                            },
                        },
                    )
                    return

                escalations = conn.execute(
                    "SELECT COUNT(*) AS c FROM cancellation_incidents WHERE status = ?",
                    (INCIDENT_ESCALATED,),
                ).fetchone()["c"]
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "ok": True,
                        "metrics": {
                            "escalations": escalations,
                        },
                    },
                )
            return

        if method == "GET" and path == "/api/inventory":
            city = query.get("city", "")
            include_hidden = query.get("include_hidden", "false") == "true"
            params: list = []
            where = []

            if user["role"] == ROLE_BROKER:
                where.append("broker_id = ?")
                params.append(user["id"])
            elif user["role"] == ROLE_RM:
                assigned = self._assigned_broker_ids(user["id"])
                if not assigned:
                    self._send_json(HTTPStatus.OK, {"ok": True, "items": []})
                    return
                where.append("broker_id IN ({})".format(",".join("?" for _ in assigned)))
                params.extend(assigned)

            if city:
                where.append("city = ?")
                params.append(city)

            if not include_hidden:
                where.append("hidden_from_customers = 0")

            sql = "SELECT * FROM properties"
            if where:
                sql += " WHERE " + " AND ".join(where)
            sql += " ORDER BY created_at DESC"

            with connect_db() as conn:
                rows = conn.execute(sql, tuple(params)).fetchall()

            items = [dict(row) for row in rows]
            self._send_json(HTTPStatus.OK, {"ok": True, "items": items})
            return

        if method == "POST" and path == "/api/inventory":
            if not require_role(user, [ROLE_BROKER]):
                self._forbidden()
                return

            data = parse_request_body(self)
            required_fields = ["title", "asset_type", "location_text", "city", "price"]
            missing = [field for field in required_fields if not data.get(field)]
            if missing:
                self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": f"Missing fields: {', '.join(missing)}"})
                return

            now = to_iso(now_local())
            with connect_db() as conn:
                conn.execute(
                    """
                    INSERT INTO properties(
                        broker_id, title, asset_type, configuration, spec_value, spec_unit,
                        bhk, area_value, location_text, city, price, maps_url, latitude, longitude,
                        amenities, image_url, status, hidden_from_customers, created_at, updated_at
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?)
                    """,
                    (
                        user["id"],
                        data.get("title"),
                        data.get("asset_type"),
                        data.get("configuration"),
                        float(data.get("spec_value")) if data.get("spec_value") else None,
                        data.get("spec_unit"),
                        data.get("bhk"),
                        float(data.get("area_value")) if data.get("area_value") else None,
                        data.get("location_text"),
                        data.get("city"),
                        float(data.get("price")),
                        data.get("maps_url"),
                        float(data.get("latitude")) if data.get("latitude") else None,
                        float(data.get("longitude")) if data.get("longitude") else None,
                        data.get("amenities"),
                        data.get("image_url"),
                        PROPERTY_STATUS_ACTIVE,
                        now,
                        now,
                    ),
                )
                property_id = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
                duplicate_info = run_duplicate_checks(conn, property_id)
                conn.commit()

                item = conn.execute("SELECT * FROM properties WHERE id = ?", (property_id,)).fetchone()

            self._send_json(
                HTTPStatus.OK,
                {"ok": True, "property": dict(item), "duplicate_check": duplicate_info},
            )
            return

        if method == "POST" and path == "/api/inventory/remove":
            if not require_role(user, [ROLE_BROKER]):
                self._forbidden()
                return

            data = parse_request_body(self)
            property_id = int(data.get("property_id") or 0)
            reason = (data.get("reason") or "").strip()
            details = (data.get("details") or "").strip()
            if not property_id or not reason:
                self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "property_id and reason are required"})
                return

            status = PROPERTY_STATUS_SOLD if normalize_text(reason) == "property already sold" else PROPERTY_STATUS_WITHDRAWN

            with connect_db() as conn:
                row = conn.execute(
                    "SELECT * FROM properties WHERE id = ? AND broker_id = ?",
                    (property_id, user["id"]),
                ).fetchone()
                if not row:
                    self._send_json(HTTPStatus.NOT_FOUND, {"ok": False, "error": "Property not found"})
                    return

                conn.execute(
                    "UPDATE properties SET status = ?, hidden_from_customers = 1, updated_at = ? WHERE id = ?",
                    (status, to_iso(now_local()), property_id),
                )
                conn.execute(
                    "INSERT INTO property_removal_log(property_id, broker_id, reason, details, created_at) VALUES(?, ?, ?, ?, ?)",
                    (property_id, user["id"], reason, details, to_iso(now_local())),
                )
                conn.commit()

            self._send_json(HTTPStatus.OK, {"ok": True, "property_id": property_id, "status": status})
            return

        if method == "GET" and path == "/api/rm/duplicate-queue":
            if not require_role(user, [ROLE_RM]):
                self._forbidden()
                return

            assigned = self._assigned_broker_ids(user["id"])
            if not assigned:
                self._send_json(HTTPStatus.OK, {"ok": True, "items": []})
                return

            with connect_db() as conn:
                rows = conn.execute(
                    f"""
                    SELECT
                        dq.*, p.title AS property_title, p.city, p.broker_id,
                        m.title AS matched_property_title
                    FROM duplicate_review_queue dq
                    JOIN properties p ON p.id = dq.property_id
                    JOIN properties m ON m.id = dq.matched_property_id
                    WHERE dq.status = 'pending' AND p.broker_id IN ({','.join('?' for _ in assigned)})
                    ORDER BY dq.created_at ASC
                    """,
                    tuple(assigned),
                ).fetchall()
            self._send_json(HTTPStatus.OK, {"ok": True, "items": [dict(r) for r in rows]})
            return

        if method == "POST" and path == "/api/rm/duplicate-review":
            if not require_role(user, [ROLE_RM]):
                self._forbidden()
                return

            data = parse_request_body(self)
            queue_id = int(data.get("queue_id") or 0)
            decision = (data.get("decision") or "").strip()
            notes = (data.get("notes") or "").strip()
            if decision not in ["approve_visible", "mark_duplicate", "keep_backup"]:
                self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "Invalid decision"})
                return

            with connect_db() as conn:
                row = conn.execute(
                    """
                    SELECT dq.*, p.broker_id
                    FROM duplicate_review_queue dq
                    JOIN properties p ON p.id = dq.property_id
                    WHERE dq.id = ? AND dq.status = 'pending'
                    """,
                    (queue_id,),
                ).fetchone()
                if not row:
                    self._send_json(HTTPStatus.NOT_FOUND, {"ok": False, "error": "Queue item not found"})
                    return

                assigned = self._assigned_broker_ids(user["id"])
                if row["broker_id"] not in assigned:
                    self._forbidden()
                    return

                if decision == "approve_visible":
                    conn.execute(
                        "UPDATE properties SET status = ?, hidden_from_customers = 0, updated_at = ? WHERE id = ?",
                        (PROPERTY_STATUS_ACTIVE, to_iso(now_local()), row["property_id"]),
                    )
                elif decision == "mark_duplicate":
                    conn.execute(
                        "UPDATE properties SET status = ?, hidden_from_customers = 1, updated_at = ? WHERE id = ?",
                        (PROPERTY_STATUS_DUPLICATE_REJECTED, to_iso(now_local()), row["property_id"]),
                    )
                else:
                    conn.execute(
                        """
                        UPDATE properties
                        SET status = ?, hidden_from_customers = 1, primary_property_id = ?, updated_at = ?
                        WHERE id = ?
                        """,
                        (PROPERTY_STATUS_BACKUP, row["matched_property_id"], to_iso(now_local()), row["property_id"]),
                    )

                conn.execute(
                    """
                    UPDATE duplicate_review_queue
                    SET status = 'resolved', rm_id = ?, decision = ?, notes = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (user["id"], decision, notes, to_iso(now_local()), queue_id),
                )
                conn.commit()

            self._send_json(HTTPStatus.OK, {"ok": True})
            return

        if method == "GET" and path == "/api/slots":
            params = []
            where = []
            if user["role"] == ROLE_BROKER:
                where.append("broker_id = ?")
                params.append(user["id"])
            elif user["role"] == ROLE_RM:
                assigned = self._assigned_broker_ids(user["id"])
                if not assigned:
                    self._send_json(HTTPStatus.OK, {"ok": True, "items": []})
                    return
                where.append("broker_id IN ({})".format(",".join("?" for _ in assigned)))
                params.extend(assigned)

            city = query.get("city")
            if city:
                where.append("city = ?")
                params.append(city)

            sql = "SELECT * FROM slots"
            if where:
                sql += " WHERE " + " AND ".join(where)
            sql += " ORDER BY start_at ASC"

            with connect_db() as conn:
                rows = conn.execute(sql, tuple(params)).fetchall()

            self._send_json(HTTPStatus.OK, {"ok": True, "items": [dict(r) for r in rows]})
            return

        if method == "POST" and path == "/api/slots/add":
            if not require_role(user, [ROLE_BROKER]):
                self._forbidden()
                return

            data = parse_request_body(self)
            start_at = parse_iso(data.get("start_at"))
            end_at = parse_iso(data.get("end_at"))
            city = data.get("city") or user["city"]

            if not start_at or not end_at or end_at <= start_at:
                self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "Invalid start/end time"})
                return

            with connect_db() as conn:
                overlap = conn.execute(
                    """
                    SELECT id
                    FROM slots
                    WHERE broker_id = ?
                      AND status IN (?, ?)
                      AND NOT (? <= start_at OR ? >= end_at)
                    """,
                    (
                        user["id"],
                        SLOT_STATUS_OPEN,
                        SLOT_STATUS_BOOKED,
                        to_iso(start_at),
                        to_iso(end_at),
                    ),
                ).fetchone()
                if overlap:
                    self._send_json(HTTPStatus.CONFLICT, {"ok": False, "error": "Slot overlaps with existing slot"})
                    return

                now = to_iso(now_local())
                conn.execute(
                    """
                    INSERT INTO slots(broker_id, city, start_at, end_at, status, created_at, updated_at)
                    VALUES(?, ?, ?, ?, ?, ?, ?)
                    """,
                    (user["id"], city, to_iso(start_at), to_iso(end_at), SLOT_STATUS_OPEN, now, now),
                )
                slot_id = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
                conn.commit()

            self._send_json(HTTPStatus.OK, {"ok": True, "slot_id": slot_id})
            return

        if method == "POST" and path == "/api/slots/cancel":
            if not require_role(user, [ROLE_BROKER]):
                self._forbidden()
                return

            data = parse_request_body(self)
            slot_id = int(data.get("slot_id") or 0)
            reason = (data.get("reason") or "").strip()
            emergency_requested = bool(data.get("emergency_requested"))
            emergency_reason = (data.get("emergency_reason") or "").strip()
            emergency_details = (data.get("emergency_details") or "").strip()

            if not slot_id or not reason:
                self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "slot_id and reason are required"})
                return

            now_dt = now_local()
            now_iso = to_iso(now_dt)

            with connect_db() as conn:
                slot = conn.execute(
                    "SELECT * FROM slots WHERE id = ? AND broker_id = ?",
                    (slot_id, user["id"]),
                ).fetchone()
                if not slot:
                    self._send_json(HTTPStatus.NOT_FOUND, {"ok": False, "error": "Slot not found"})
                    return

                if slot["status"] in [SLOT_STATUS_CANCELLED, SLOT_STATUS_COMPLETED]:
                    self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "Slot already closed"})
                    return

                visit = conn.execute(
                    """
                    SELECT *
                    FROM visits
                    WHERE slot_id = ? AND status = ?
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (slot_id, VISIT_STATUS_SCHEDULED),
                ).fetchone()

                conn.execute(
                    "UPDATE slots SET status = ?, cancel_reason = ?, cancelled_at = ?, updated_at = ? WHERE id = ?",
                    (SLOT_STATUS_CANCELLED, reason, now_iso, now_iso, slot_id),
                )

                incident_id = None
                priority_rebook_until = None
                flagged = None

                if visit:
                    start_at = parse_iso(visit["start_at"])
                    within_24h = False
                    if start_at:
                        within_24h = (start_at - now_dt) <= timedelta(hours=24)

                    if within_24h:
                        priority_rebook_until = now_dt + timedelta(hours=48)

                    conn.execute(
                        """
                        UPDATE visits
                        SET status = ?, cancelled_by = ?, cancellation_reason = ?,
                            priority_rebook_until = ?, updated_at = ?
                        WHERE id = ?
                        """,
                        (
                            VISIT_STATUS_CANCELLED_BROKER,
                            "broker",
                            reason,
                            to_iso(priority_rebook_until) if priority_rebook_until else None,
                            now_iso,
                            visit["id"],
                        ),
                    )

                    record_event(
                        conn,
                        "customer_apology_sent",
                        "visit",
                        visit["id"],
                        {
                            "broker_id": user["id"],
                            "priority_rebook_until": to_iso(priority_rebook_until),
                            "within_24h": within_24h,
                        },
                    )
                    send_visit_whatsapp(
                        conn,
                        visit_id=visit["id"],
                        template_name="broker_cancel_with_priority" if within_24h else "broker_cancel_without_priority",
                        source="broker_slot_cancel",
                    )

                    if within_24h:
                        record_event(
                            conn,
                            "rm_call_triggered",
                            "visit",
                            visit["id"],
                            {"reason": "broker_cancelled_within_24h"},
                        )

                        status = INCIDENT_PENDING_RM if emergency_requested else INCIDENT_REJECTED_NO_EMERGENCY
                        sla_due_at = calc_rm_sla(now_dt) if emergency_requested else None

                        conn.execute(
                            """
                            INSERT INTO cancellation_incidents(
                                slot_id, visit_id, broker_id, raised_at, within_24h, is_booked,
                                emergency_requested, emergency_reason, emergency_details, status,
                                sla_due_at, created_at, updated_at
                            ) VALUES(?, ?, ?, ?, 1, 1, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                slot_id,
                                visit["id"],
                                user["id"],
                                now_iso,
                                1 if emergency_requested else 0,
                                emergency_reason,
                                emergency_details,
                                status,
                                to_iso(sla_due_at),
                                now_iso,
                                now_iso,
                            ),
                        )
                        incident_id = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]

                        if not emergency_requested:
                            flagged = apply_flag(
                                conn,
                                user["id"],
                                incident_id,
                                "Booked visit cancelled within 24h without emergency approval",
                            )
                            conn.execute(
                                """
                                UPDATE cancellation_incidents
                                SET resolved_at = ?, updated_at = ?
                                WHERE id = ?
                                """,
                                (now_iso, now_iso, incident_id),
                            )

                conn.commit()

            self._send_json(
                HTTPStatus.OK,
                {
                    "ok": True,
                    "slot_id": slot_id,
                    "incident_id": incident_id,
                    "flag": flagged,
                },
            )
            return

        if method == "GET" and path == "/api/visits":
            params = []
            where = []

            if user["role"] == ROLE_BROKER:
                where.append("v.broker_id = ?")
                params.append(user["id"])
            elif user["role"] == ROLE_RM:
                assigned = self._assigned_broker_ids(user["id"])
                if not assigned:
                    self._send_json(HTTPStatus.OK, {"ok": True, "items": []})
                    return
                where.append("v.broker_id IN ({})".format(",".join("?" for _ in assigned)))
                params.extend(assigned)

            status_filter = query.get("status")
            if status_filter:
                where.append("v.status = ?")
                params.append(status_filter)

            sql = """
                SELECT
                    v.*, c.name AS customer_name, c.phone_norm,
                    p.title AS property_title, s.city
                FROM visits v
                JOIN customers c ON c.id = v.customer_id
                JOIN properties p ON p.id = v.property_id
                JOIN slots s ON s.id = v.slot_id
            """
            if where:
                sql += " WHERE " + " AND ".join(where)
            sql += " ORDER BY v.start_at ASC"

            with connect_db() as conn:
                rows = conn.execute(sql, tuple(params)).fetchall()

            self._send_json(HTTPStatus.OK, {"ok": True, "items": [dict(r) for r in rows]})
            return

        if method == "POST" and path == "/api/visits/book":
            data = parse_request_body(self)
            slot_id = int(data.get("slot_id") or 0)
            property_id = int(data.get("property_id") or 0)
            customer_phone = normalize_phone(data.get("customer_phone"))
            customer_name = (data.get("customer_name") or "").strip()
            customer_requirements = (data.get("customer_requirements") or "").strip()

            if not slot_id or not property_id or not customer_phone:
                self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "slot_id, property_id, customer_phone are required"})
                return

            with connect_db() as conn:
                slot = conn.execute(
                    "SELECT * FROM slots WHERE id = ?",
                    (slot_id,),
                ).fetchone()
                prop = conn.execute(
                    "SELECT * FROM properties WHERE id = ?",
                    (property_id,),
                ).fetchone()

                if not slot or not prop:
                    self._send_json(HTTPStatus.NOT_FOUND, {"ok": False, "error": "Slot or property not found"})
                    return
                if slot["status"] != SLOT_STATUS_OPEN:
                    self._send_json(HTTPStatus.CONFLICT, {"ok": False, "error": "Slot not available"})
                    return
                if prop["broker_id"] != slot["broker_id"]:
                    self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "Slot must belong to the same broker as property"})
                    return

                customer = conn.execute(
                    "SELECT * FROM customers WHERE phone_norm = ?",
                    (customer_phone,),
                ).fetchone()
                if not customer:
                    conn.execute(
                        "INSERT INTO customers(name, phone_norm, created_at) VALUES(?, ?, ?)",
                        (customer_name or "Customer", customer_phone, to_iso(now_local())),
                    )
                    customer_id = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
                else:
                    customer_id = customer["id"]
                    if customer_name:
                        conn.execute("UPDATE customers SET name = ? WHERE id = ?", (customer_name, customer_id))

                visit_id = create_scheduled_visit(
                    conn,
                    slot_row=slot,
                    property_row=prop,
                    customer_id=customer_id,
                    customer_requirements=customer_requirements,
                    source="rm_booking",
                )
                conn.commit()

            self._send_json(HTTPStatus.OK, {"ok": True, "visit_id": visit_id})
            return

        if method == "POST" and path == "/api/visits/send-otp":
            if not require_role(user, [ROLE_BROKER]):
                self._forbidden()
                return

            data = parse_request_body(self)
            visit_id = int(data.get("visit_id") or 0)
            if not visit_id:
                self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "visit_id is required"})
                return

            code = f"{random.randint(0, 999999):06d}"
            expires = now_local() + timedelta(seconds=OTP_TTL_SECONDS)

            with connect_db() as conn:
                visit = conn.execute(
                    """
                    SELECT v.*, c.phone_norm
                    FROM visits v
                    JOIN customers c ON c.id = v.customer_id
                    WHERE v.id = ? AND v.broker_id = ?
                    """,
                    (visit_id, user["id"]),
                ).fetchone()
                if not visit or visit["status"] != VISIT_STATUS_SCHEDULED:
                    self._send_json(HTTPStatus.NOT_FOUND, {"ok": False, "error": "Visit not eligible"})
                    return

                conn.execute(
                    """
                    UPDATE visits
                    SET otp_code = ?, otp_expires_at = ?, otp_attempts = 0, otp_sent_at = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (code, to_iso(expires), to_iso(now_local()), to_iso(now_local()), visit_id),
                )
                record_event(conn, "otp_sent", "visit", visit_id, {"expires_at": to_iso(expires)})
                send_whatsapp_template(
                    conn,
                    to_phone=visit["phone_norm"],
                    template_name="otp_verification",
                    context={"otp": code},
                    related_visit_id=visit_id,
                    source="broker_otp",
                )
                conn.commit()

            self._send_json(
                HTTPStatus.OK,
                {
                    "ok": True,
                    "visit_id": visit_id,
                    "otp_expires_at": to_iso(expires),
                    "demo_otp": code,
                },
            )
            return

        if method == "POST" and path == "/api/visits/complete":
            if not require_role(user, [ROLE_BROKER]):
                self._forbidden()
                return

            data = parse_request_body(self)
            visit_id = int(data.get("visit_id") or 0)
            otp = (data.get("otp") or "").strip()
            photo = data.get("photo_base64") or ""
            lat = data.get("lat")
            lng = data.get("lng")

            if not visit_id or not otp:
                self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "visit_id and otp are required"})
                return

            with connect_db() as conn:
                visit = conn.execute(
                    """
                    SELECT v.*, p.latitude AS p_lat, p.longitude AS p_lng
                    FROM visits v
                    JOIN properties p ON p.id = v.property_id
                    WHERE v.id = ? AND v.broker_id = ?
                    """,
                    (visit_id, user["id"]),
                ).fetchone()

                if not visit or visit["status"] != VISIT_STATUS_SCHEDULED:
                    self._send_json(HTTPStatus.NOT_FOUND, {"ok": False, "error": "Visit not eligible"})
                    return

                if not visit["otp_code"]:
                    self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "Send OTP first"})
                    return

                if visit["otp_attempts"] >= MAX_OTP_ATTEMPTS:
                    self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "OTP attempts exhausted"})
                    return

                expires_at = parse_iso(visit["otp_expires_at"])
                if not expires_at or expires_at < now_local():
                    self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "OTP expired"})
                    return

                if otp != visit["otp_code"]:
                    attempts = visit["otp_attempts"] + 1
                    conn.execute("UPDATE visits SET otp_attempts = ?, updated_at = ? WHERE id = ?", (attempts, to_iso(now_local()), visit_id))
                    conn.commit()
                    self._send_json(
                        HTTPStatus.BAD_REQUEST,
                        {
                            "ok": False,
                            "error": "Invalid OTP",
                            "remaining_attempts": max(0, MAX_OTP_ATTEMPTS - attempts),
                        },
                    )
                    return

                distance = None
                completion_mode = None
                lat_val = float(lat) if lat is not None else None
                lng_val = float(lng) if lng is not None else None

                if lat_val is not None and lng_val is not None and visit["p_lat"] is not None and visit["p_lng"] is not None:
                    distance = haversine_meters(lat_val, lng_val, float(visit["p_lat"]), float(visit["p_lng"]))
                    if distance <= GEO_RADIUS_METERS:
                        completion_mode = "geo_checkin"

                if completion_mode is None:
                    if not photo:
                        self._send_json(
                            HTTPStatus.BAD_REQUEST,
                            {
                                "ok": False,
                                "error": "Geo check failed or unavailable. Upload customer photo fallback.",
                            },
                        )
                        return
                    completion_mode = "photo_fallback"

                prior_completed = conn.execute(
                    """
                    SELECT COUNT(*) AS c
                    FROM visits
                    WHERE customer_id = ?
                      AND status = ?
                      AND id != ?
                    """,
                    (visit["customer_id"], VISIT_STATUS_COMPLETED, visit_id),
                ).fetchone()["c"]
                is_unique = 1 if prior_completed == 0 else 0

                now = to_iso(now_local())
                conn.execute(
                    """
                    UPDATE visits
                    SET
                        status = ?,
                        checkin_lat = ?,
                        checkin_lng = ?,
                        distance_meters = ?,
                        photo_fallback_base64 = ?,
                        is_unique_visit = ?,
                        completion_mode = ?,
                        completed_at = ?,
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (
                        VISIT_STATUS_COMPLETED,
                        lat_val,
                        lng_val,
                        distance,
                        photo if completion_mode == "photo_fallback" else None,
                        is_unique,
                        completion_mode,
                        now,
                        now,
                        visit_id,
                    ),
                )

                conn.execute(
                    "UPDATE slots SET status = ?, updated_at = ? WHERE id = ?",
                    (SLOT_STATUS_COMPLETED, now, visit["slot_id"]),
                )

                record_event(
                    conn,
                    "visit_completed",
                    "visit",
                    visit_id,
                    {
                        "unique_visit": bool(is_unique),
                        "completion_mode": completion_mode,
                        "distance_meters": distance,
                    },
                )
                conn.commit()

            self._send_json(
                HTTPStatus.OK,
                {
                    "ok": True,
                    "visit_id": visit_id,
                    "unique_visit": bool(is_unique),
                    "completion_mode": completion_mode,
                },
            )
            return

        if method == "GET" and path == "/api/rm/emergency-queue":
            if not require_role(user, [ROLE_RM]):
                self._forbidden()
                return
            assigned = self._assigned_broker_ids(user["id"])
            if not assigned:
                self._send_json(HTTPStatus.OK, {"ok": True, "items": []})
                return
            with connect_db() as conn:
                rows = conn.execute(
                    f"""
                    SELECT ci.*, v.start_at, c.phone_norm, c.name AS customer_name
                    FROM cancellation_incidents ci
                    JOIN visits v ON v.id = ci.visit_id
                    JOIN customers c ON c.id = v.customer_id
                    WHERE ci.status = ? AND ci.broker_id IN ({','.join('?' for _ in assigned)})
                    ORDER BY ci.raised_at ASC
                    """,
                    (INCIDENT_PENDING_RM, *assigned),
                ).fetchall()
            self._send_json(HTTPStatus.OK, {"ok": True, "items": [dict(r) for r in rows]})
            return

        if method == "POST" and path == "/api/rm/emergency-review":
            if not require_role(user, [ROLE_RM]):
                self._forbidden()
                return

            data = parse_request_body(self)
            incident_id = int(data.get("incident_id") or 0)
            approve = bool(data.get("approve"))
            note = (data.get("note") or "").strip()

            with connect_db() as conn:
                incident = conn.execute(
                    "SELECT * FROM cancellation_incidents WHERE id = ? AND status = ?",
                    (incident_id, INCIDENT_PENDING_RM),
                ).fetchone()
                if not incident:
                    self._send_json(HTTPStatus.NOT_FOUND, {"ok": False, "error": "Incident not found"})
                    return

                assigned = self._assigned_broker_ids(user["id"])
                if incident["broker_id"] not in assigned:
                    self._forbidden()
                    return

                now = to_iso(now_local())
                status = INCIDENT_APPROVED if approve else INCIDENT_REJECTED
                conn.execute(
                    """
                    UPDATE cancellation_incidents
                    SET status = ?, rm_id = ?, rm_note = ?, resolved_at = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (status, user["id"], note, now, now, incident_id),
                )

                flag = None
                if not approve:
                    flag = apply_flag(
                        conn,
                        incident["broker_id"],
                        incident_id,
                        "Emergency cancellation rejected by RM",
                    )

                record_event(
                    conn,
                    "rm_emergency_reviewed",
                    "cancellation_incident",
                    incident_id,
                    {"approved": approve, "rm_id": user["id"]},
                )
                conn.commit()

            self._send_json(HTTPStatus.OK, {"ok": True, "flag": flag})
            return

        if method == "GET" and path == "/api/srm/escalations":
            if not require_role(user, [ROLE_SRM]):
                self._forbidden()
                return

            with connect_db() as conn:
                rows = conn.execute(
                    """
                    SELECT ci.*, v.start_at, c.name AS customer_name, c.phone_norm
                    FROM cancellation_incidents ci
                    JOIN visits v ON v.id = ci.visit_id
                    JOIN customers c ON c.id = v.customer_id
                    WHERE ci.status = ?
                    ORDER BY ci.updated_at ASC
                    """,
                    (INCIDENT_ESCALATED,),
                ).fetchall()
            self._send_json(HTTPStatus.OK, {"ok": True, "items": [dict(r) for r in rows]})
            return

        if method == "POST" and path == "/api/srm/escalation-review":
            if not require_role(user, [ROLE_SRM]):
                self._forbidden()
                return

            data = parse_request_body(self)
            incident_id = int(data.get("incident_id") or 0)
            approve = bool(data.get("approve"))
            note = (data.get("note") or "").strip()

            with connect_db() as conn:
                incident = conn.execute(
                    "SELECT * FROM cancellation_incidents WHERE id = ? AND status = ?",
                    (incident_id, INCIDENT_ESCALATED),
                ).fetchone()
                if not incident:
                    self._send_json(HTTPStatus.NOT_FOUND, {"ok": False, "error": "Escalation not found"})
                    return

                status = INCIDENT_APPROVED_SRM if approve else INCIDENT_REJECTED_SRM
                now = to_iso(now_local())
                conn.execute(
                    """
                    UPDATE cancellation_incidents
                    SET status = ?, srm_id = ?, srm_note = ?, resolved_at = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (status, user["id"], note, now, now, incident_id),
                )

                flag = None
                if not approve:
                    flag = apply_flag(
                        conn,
                        incident["broker_id"],
                        incident_id,
                        "Emergency cancellation rejected by SRM",
                    )

                conn.commit()

            self._send_json(HTTPStatus.OK, {"ok": True, "flag": flag})
            return

        if method == "GET" and path == "/api/flags":
            broker_id = int(query.get("broker_id") or 0)
            with connect_db() as conn:
                if user["role"] == ROLE_BROKER:
                    broker_id = user["id"]
                if user["role"] == ROLE_RM and broker_id and broker_id not in self._assigned_broker_ids(user["id"]):
                    self._forbidden()
                    return
                if not broker_id:
                    broker_id = user["id"]

                rows = conn.execute(
                    "SELECT * FROM broker_flags WHERE broker_id = ? ORDER BY created_at DESC",
                    (broker_id,),
                ).fetchall()
            self._send_json(HTTPStatus.OK, {"ok": True, "items": [dict(r) for r in rows]})
            return

        if method == "GET" and path == "/api/leads":
            with connect_db() as conn:
                rows = conn.execute(
                    """
                    SELECT l.*, c.name AS customer_name, c.phone_norm
                    FROM leads l
                    JOIN customers c ON c.id = l.customer_id
                    ORDER BY l.last_synced_at DESC
                    LIMIT 200
                    """
                ).fetchall()
            self._send_json(HTTPStatus.OK, {"ok": True, "items": [dict(r) for r in rows]})
            return

        if method == "POST" and path == "/api/leads/import-now":
            result = import_leads_from_csv()
            self._send_json(HTTPStatus.OK, {"ok": True, "result": result})
            return

        if method == "GET" and path == "/api/integrations/whatsapp/messages":
            if not require_role(user, [ROLE_RM, ROLE_SRM]):
                self._forbidden()
                return
            with connect_db() as conn:
                rows = conn.execute(
                    """
                    SELECT *
                    FROM whatsapp_messages
                    ORDER BY created_at DESC
                    LIMIT 200
                    """
                ).fetchall()
            self._send_json(HTTPStatus.OK, {"ok": True, "items": [dict(r) for r in rows]})
            return

        if method == "POST" and path == "/api/integrations/whatsapp/send-test":
            if not require_role(user, [ROLE_RM, ROLE_SRM]):
                self._forbidden()
                return
            data = parse_request_body(self)
            to_phone = normalize_phone(data.get("to_phone"))
            template_name = (data.get("template_name") or "customer_help").strip()
            context = data.get("context") or {}
            related_visit_id = int(data.get("related_visit_id") or 0) or None
            if not to_phone:
                self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "to_phone is required"})
                return
            with connect_db() as conn:
                result = send_whatsapp_template(
                    conn,
                    to_phone=to_phone,
                    template_name=template_name,
                    context=context if isinstance(context, dict) else {},
                    related_visit_id=related_visit_id,
                    source="manual_rm_send",
                )
                conn.commit()
            self._send_json(HTTPStatus.OK, {"ok": True, "result": result})
            return

        if method == "GET" and path == "/api/reports/funnel":
            if not require_role(user, [ROLE_RM, ROLE_SRM]):
                self._forbidden()
                return
            with connect_db() as conn:
                report = build_funnel_report(conn)
            self._send_json(HTTPStatus.OK, {"ok": True, "report": report})
            return

        if method == "GET" and path == "/api/reports/broker-reliability":
            if not require_role(user, [ROLE_RM, ROLE_SRM]):
                self._forbidden()
                return
            with connect_db() as conn:
                items = build_broker_reliability_report(conn)
            self._send_json(HTTPStatus.OK, {"ok": True, "items": items})
            return

        if method == "GET" and path == "/api/reports/visit-counts":
            with connect_db() as conn:
                rows = conn.execute(
                    """
                    SELECT
                        u.id AS broker_id,
                        u.name AS broker_name,
                        SUM(CASE WHEN v.status = ? AND v.is_unique_visit = 1 THEN 1 ELSE 0 END) AS unique_visits,
                        SUM(CASE WHEN v.status = ? AND v.is_unique_visit = 0 THEN 1 ELSE 0 END) AS non_unique_visits,
                        SUM(CASE WHEN v.status = ? THEN 1 ELSE 0 END) AS total_completed
                    FROM users u
                    LEFT JOIN visits v ON v.broker_id = u.id
                    WHERE u.role = ?
                    GROUP BY u.id, u.name
                    ORDER BY u.name ASC
                    """,
                    (VISIT_STATUS_COMPLETED, VISIT_STATUS_COMPLETED, VISIT_STATUS_COMPLETED, ROLE_BROKER),
                ).fetchall()
            self._send_json(HTTPStatus.OK, {"ok": True, "items": [dict(r) for r in rows]})
            return

        if method == "GET" and path == "/api/reports/export.csv":
            if not require_role(user, [ROLE_RM, ROLE_SRM]):
                self._forbidden()
                return

            export_type = (query.get("type") or "visit_counts").strip().lower()
            with connect_db() as conn:
                if export_type == "visit_counts":
                    rows = conn.execute(
                        """
                        SELECT
                            u.name AS broker_name,
                            SUM(CASE WHEN v.status = ? AND v.is_unique_visit = 1 THEN 1 ELSE 0 END) AS unique_visits,
                            SUM(CASE WHEN v.status = ? AND v.is_unique_visit = 0 THEN 1 ELSE 0 END) AS non_unique_visits,
                            SUM(CASE WHEN v.status = ? THEN 1 ELSE 0 END) AS total_completed
                        FROM users u
                        LEFT JOIN visits v ON v.broker_id = u.id
                        WHERE u.role = ?
                        GROUP BY u.id, u.name
                        ORDER BY u.name ASC
                        """,
                        (VISIT_STATUS_COMPLETED, VISIT_STATUS_COMPLETED, VISIT_STATUS_COMPLETED, ROLE_BROKER),
                    ).fetchall()
                    csv_payload = csv_text(
                        ["broker_name", "unique_visits", "non_unique_visits", "total_completed"],
                        [[r["broker_name"], r["unique_visits"], r["non_unique_visits"], r["total_completed"]] for r in rows],
                    )
                elif export_type == "funnel":
                    report = build_funnel_report(conn)
                    csv_payload = csv_text(
                        ["metric", "value"],
                        [[k, v] for k, v in report.items()],
                    )
                elif export_type == "broker_reliability":
                    items = build_broker_reliability_report(conn)
                    csv_payload = csv_text(
                        [
                            "broker_id",
                            "broker_name",
                            "city",
                            "active",
                            "total_visits",
                            "completed_visits",
                            "completion_rate_pct",
                            "broker_cancelled_visits",
                            "late_cancel_incidents",
                            "active_flags",
                        ],
                        [
                            [
                                item["broker_id"],
                                item["broker_name"],
                                item["city"],
                                item["active"],
                                item["total_visits"],
                                item["completed_visits"],
                                item["completion_rate_pct"],
                                item["broker_cancelled_visits"],
                                item["late_cancel_incidents"],
                                item["active_flags"],
                            ]
                            for item in items
                        ],
                    )
                elif export_type == "whatsapp_messages":
                    rows = conn.execute(
                        """
                        SELECT
                            id, direction, source, to_phone, from_phone, template_name,
                            message_text, status, related_visit_id, created_at
                        FROM whatsapp_messages
                        ORDER BY created_at DESC
                        LIMIT 1000
                        """
                    ).fetchall()
                    csv_payload = csv_text(
                        [
                            "id",
                            "direction",
                            "source",
                            "to_phone",
                            "from_phone",
                            "template_name",
                            "message_text",
                            "status",
                            "related_visit_id",
                            "created_at",
                        ],
                        [
                            [
                                row["id"],
                                row["direction"],
                                row["source"],
                                row["to_phone"],
                                row["from_phone"],
                                row["template_name"],
                                row["message_text"],
                                row["status"],
                                row["related_visit_id"],
                                row["created_at"],
                            ]
                            for row in rows
                        ],
                    )
                elif export_type == "visits":
                    rows = conn.execute(
                        """
                        SELECT
                            v.id, v.status, v.cancelled_by, v.cancellation_reason,
                            v.start_at, v.end_at, v.is_unique_visit, v.completion_mode,
                            c.name AS customer_name, c.phone_norm,
                            p.title AS property_title,
                            b.name AS broker_name
                        FROM visits v
                        JOIN customers c ON c.id = v.customer_id
                        JOIN properties p ON p.id = v.property_id
                        JOIN users b ON b.id = v.broker_id
                        ORDER BY v.start_at DESC
                        LIMIT 2000
                        """
                    ).fetchall()
                    csv_payload = csv_text(
                        [
                            "visit_id",
                            "status",
                            "cancelled_by",
                            "cancellation_reason",
                            "start_at",
                            "end_at",
                            "is_unique_visit",
                            "completion_mode",
                            "customer_name",
                            "customer_phone",
                            "property_title",
                            "broker_name",
                        ],
                        [
                            [
                                row["id"],
                                row["status"],
                                row["cancelled_by"],
                                row["cancellation_reason"],
                                row["start_at"],
                                row["end_at"],
                                row["is_unique_visit"],
                                row["completion_mode"],
                                row["customer_name"],
                                row["phone_norm"],
                                row["property_title"],
                                row["broker_name"],
                            ]
                            for row in rows
                        ],
                    )
                else:
                    self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "Unknown export type"})
                    return

            filename = f"{export_type}_{now_local().strftime('%Y%m%d_%H%M%S')}.csv"
            self._send_csv(filename, csv_payload)
            return

        self._send_json(HTTPStatus.NOT_FOUND, {"ok": False, "error": "Route not found"})

    def _assigned_broker_ids(self, rm_id: int) -> list[int]:
        with connect_db() as conn:
            rows = conn.execute(
                "SELECT broker_id FROM rm_assignments WHERE rm_id = ?",
                (rm_id,),
            ).fetchall()
        return [row["broker_id"] for row in rows]


def main() -> None:
    init_db()
    import_leads_from_csv()

    stop_event = threading.Event()
    sync_thread = LeadSyncThread(stop_event)
    sync_thread.start()

    host = os.getenv("HOST", "127.0.0.1")
    port = int(os.getenv("PORT", "8080"))
    server = ThreadingHTTPServer((host, port), AppHandler)
    ui_host = "127.0.0.1" if host in ("0.0.0.0", "::") else host

    print("Proptech MVP running")
    print(f"URL: http://{ui_host}:{port}")
    print(f"Listening on {host}:{port}")
    print("Demo users:")
    print("- broker.jaipur@example.com / broker123")
    print("- broker.nagpur@example.com / broker123")
    print("- rm.jaipur@example.com / rm123")
    print("- rm.nagpur@example.com / rm123")
    print("- srm.ops@example.com / srm123")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        stop_event.set()
        server.server_close()


if __name__ == "__main__":
    main()
