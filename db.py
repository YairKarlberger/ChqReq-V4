import contextlib
import os
import sqlite3
import time
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional

DB_PATH = os.environ.get("CHEQUE_REQ_DB", "data.db")


def _dict_factory(cursor: sqlite3.Cursor, row: Iterable[Any]) -> Dict[str, Any]:
    return {col[0]: row[idx] for idx, col in enumerate(cursor.description)}


@contextlib.contextmanager
def conn(row_factory: Optional[Any] = sqlite3.Row):
    database = sqlite3.connect(DB_PATH)
    if row_factory:
        database.row_factory = row_factory
    try:
        yield database
        database.commit()
    finally:
        database.close()


def init_db() -> None:
    with conn() as database:
        cur = database.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT,
                email TEXT,
                role TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS projects (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                slug TEXT UNIQUE NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS vendors (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                slug TEXT UNIQUE NOT NULL,
                address1 TEXT,
                address2 TEXT,
                city_prov TEXT,
                region TEXT,
                postal_code TEXT,
                contact TEXT,
                tel TEXT,
                hst_number TEXT,
                folder_path TEXT,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS cheque_requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                project_id INTEGER,
                vendor_id INTEGER,
                date TEXT,
                invoice_date TEXT,
                currency TEXT,
                department TEXT,
                po_desc TEXT,
                vendor_address_text TEXT,
                amount_before_hst REAL,
                hst_amount REAL,
                pst_amount REAL,
                gst_amount REAL,
                amount_total REAL,
                invoice_number TEXT,
                po_number TEXT,
                status TEXT NOT NULL DEFAULT 'draft',
                requested_at TEXT,
                dept_approved_at TEXT,
                pm_approved_at TEXT,
                producer_approved_at TEXT,
                acctg_approved_at TEXT,
                studio_approved_at TEXT,
                completed_at TEXT,
                fingerprint_a TEXT,
                fingerprint_b TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(project_id) REFERENCES projects(id),
                FOREIGN KEY(vendor_id) REFERENCES vendors(id)
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS cheque_request_lines (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cheque_request_id INTEGER NOT NULL,
                line_no INTEGER NOT NULL,
                qty TEXT,
                description TEXT,
                coding TEXT,
                unit_price TEXT,
                tax TEXT,
                line_total TEXT,
                FOREIGN KEY(cheque_request_id) REFERENCES cheque_requests(id)
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS approval_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cheque_request_id INTEGER NOT NULL,
                from_status TEXT,
                to_status TEXT,
                at TEXT NOT NULL,
                signer_name TEXT,
                note TEXT,
                signature_path TEXT,
                FOREIGN KEY(cheque_request_id) REFERENCES cheque_requests(id)
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS exports (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cheque_request_id INTEGER NOT NULL,
                project_slug TEXT,
                vendor_slug TEXT,
                year TEXT,
                path TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(cheque_request_id) REFERENCES cheque_requests(id)
            )
            """
        )
        database.commit()


def slugify(value: str) -> str:
    value = value.strip().lower()
    allowed = [c if c.isalnum() else "-" for c in value]
    slug = "".join(allowed)
    while "--" in slug:
        slug = slug.replace("--", "-")
    slug = slug.strip("-")
    return slug or f"item-{int(time.time())}"


def list_projects() -> List[sqlite3.Row]:
    with conn() as database:
        cur = database.execute(
            "SELECT id, name, slug FROM projects ORDER BY name COLLATE NOCASE"
        )
        return cur.fetchall()


def list_vendors() -> List[sqlite3.Row]:
    with conn() as database:
        cur = database.execute(
            "SELECT * FROM vendors ORDER BY name COLLATE NOCASE"
        )
        return cur.fetchall()


def search_vendors(q: str) -> List[Dict[str, Any]]:
    like = f"%{q.strip()}%"
    with conn(row_factory=_dict_factory) as database:
        cur = database.execute(
            "SELECT * FROM vendors WHERE name LIKE ? ORDER BY name COLLATE NOCASE LIMIT 10",
            (like,),
        )
        return cur.fetchall()


def get_vendor_by_name(name: str) -> Optional[sqlite3.Row]:
    with conn() as database:
        cur = database.execute(
            "SELECT * FROM vendors WHERE lower(name) = lower(?)",
            (name.strip(),),
        )
        return cur.fetchone()


def upsert_vendor(data: Dict[str, Any]) -> int:
    name = data.get("name", "").strip()
    if not name:
        raise ValueError("Vendor name is required")
    existing = get_vendor_by_name(name)
    slug = data.get("slug") or slugify(name)
    now = datetime.utcnow().isoformat()
    address1 = data.get("address1", "").strip() or None
    address2 = data.get("address2", "").strip() or None
    city_prov = data.get("city_prov", "").strip() or None
    region = data.get("region", "").strip() or None
    postal_code = data.get("postal_code", "").strip() or None
    contact = data.get("contact", "").strip() or None
    tel = data.get("tel", "").strip() or None
    hst_number = data.get("hst_number", "").strip() or None
    folder_path = data.get("folder_path", "").strip() or None
    with conn() as database:
        if existing:
            database.execute(
                """
                UPDATE vendors
                SET slug=?, address1=?, address2=?, city_prov=?, region=?, postal_code=?,
                    contact=?, tel=?, hst_number=?, folder_path=?, updated_at=?
                WHERE id=?
                """,
                (
                    slug,
                    address1,
                    address2,
                    city_prov,
                    region,
                    postal_code,
                    contact,
                    tel,
                    hst_number,
                    folder_path,
                    now,
                    existing["id"],
                ),
            )
            return existing["id"]
        cur = database.execute(
            """
            INSERT INTO vendors (name, slug, address1, address2, city_prov, region, postal_code,
                                 contact, tel, hst_number, folder_path, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                name,
                slug,
                address1,
                address2,
                city_prov,
                region,
                postal_code,
                contact,
                tel,
                hst_number,
                folder_path,
                now,
            ),
        )
        return cur.lastrowid


def record_cheque_request(project_id: Optional[int], vendor_id: Optional[int], payload: Dict[str, Any]) -> int:
    fields = (
        "date",
        "invoice_date",
        "currency",
        "department",
        "po_desc",
        "vendor_address_text",
        "amount_before_hst",
        "hst_amount",
        "pst_amount",
        "gst_amount",
        "amount_total",
        "invoice_number",
        "po_number",
        "status",
        "requested_at",
        "dept_approved_at",
        "pm_approved_at",
        "producer_approved_at",
        "acctg_approved_at",
        "studio_approved_at",
        "completed_at",
        "fingerprint_a",
        "fingerprint_b",
    )
    values = [payload.get(field) for field in fields]
    with conn() as database:
        if payload.get("id"):
            set_clause = ", ".join(f"{field} = ?" for field in fields)
            database.execute(
                f"UPDATE cheque_requests SET project_id=?, vendor_id=?, {set_clause} WHERE id=?",
                [project_id, vendor_id, *values, payload["id"]],
            )
            cheque_id = payload["id"]
        else:
            cur = database.execute(
                """
                INSERT INTO cheque_requests (
                    project_id, vendor_id, date, invoice_date, currency, department, po_desc,
                    vendor_address_text, amount_before_hst, hst_amount, pst_amount, gst_amount,
                    amount_total, invoice_number, po_number, status, requested_at, dept_approved_at,
                    pm_approved_at, producer_approved_at, acctg_approved_at, studio_approved_at,
                    completed_at, fingerprint_a, fingerprint_b
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [project_id, vendor_id, *values],
            )
            cheque_id = cur.lastrowid
    return cheque_id


def save_cheque_lines(cheque_request_id: int, lines: List[Dict[str, Any]]) -> None:
    with conn() as database:
        database.execute(
            "DELETE FROM cheque_request_lines WHERE cheque_request_id=?",
            (cheque_request_id,),
        )
        for idx, line in enumerate(lines, start=1):
            if not any(line.values()):
                continue
            database.execute(
                """
                INSERT INTO cheque_request_lines
                (cheque_request_id, line_no, qty, description, coding, unit_price, tax, line_total)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    cheque_request_id,
                    idx,
                    line.get("qty"),
                    line.get("description"),
                    line.get("coding"),
                    line.get("unit_price"),
                    line.get("tax"),
                    line.get("line_total"),
                ),
            )


def set_status(
    cheque_request_id: int,
    to_status: str,
    signer_name: Optional[str] = None,
    note: str = "",
    signature_path: Optional[str] = None,
) -> None:
    timestamp = datetime.utcnow().isoformat()
    status_field = {
        "requested": "requested_at",
        "dept": "dept_approved_at",
        "pm": "pm_approved_at",
        "producer": "producer_approved_at",
        "acctg": "acctg_approved_at",
        "studio": "studio_approved_at",
        "completed": "completed_at",
    }.get(to_status)
    with conn() as database:
        cur = database.execute(
            "SELECT status FROM cheque_requests WHERE id=?",
            (cheque_request_id,),
        )
        row = cur.fetchone()
        from_status = row["status"] if row else None
        database.execute(
            "UPDATE cheque_requests SET status=?" + (f", {status_field}=?" if status_field else "") + " WHERE id=?",
            (to_status, timestamp, cheque_request_id) if status_field else (to_status, cheque_request_id),
        )
        database.execute(
            """
            INSERT INTO approval_events (cheque_request_id, from_status, to_status, at, signer_name, note, signature_path)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (cheque_request_id, from_status, to_status, timestamp, signer_name, note, signature_path),
        )


def list_in_progress() -> List[sqlite3.Row]:
    with conn() as database:
        cur = database.execute(
            """
            SELECT c.*, p.name AS project_name, v.name AS vendor_name
            FROM cheque_requests c
            LEFT JOIN projects p ON p.id = c.project_id
            LEFT JOIN vendors v ON v.id = c.vendor_id
            WHERE c.status != 'completed'
            ORDER BY c.created_at DESC
            """
        )
        return cur.fetchall()


def list_recent_completed(n: int = 10) -> List[sqlite3.Row]:
    with conn() as database:
        cur = database.execute(
            """
            SELECT c.*, p.name AS project_name, v.name AS vendor_name, e.path AS export_path
            FROM cheque_requests c
            LEFT JOIN projects p ON p.id = c.project_id
            LEFT JOIN vendors v ON v.id = c.vendor_id
            LEFT JOIN exports e ON e.cheque_request_id = c.id
            WHERE c.status = 'completed'
            ORDER BY c.completed_at DESC
            LIMIT ?
            """,
            (n,),
        )
        return cur.fetchall()


def get_cheque_request(cheque_request_id: int) -> Optional[sqlite3.Row]:
    with conn() as database:
        cur = database.execute(
            """
            SELECT c.*, p.name AS project_name, p.slug AS project_slug,
                   v.name AS vendor_name, v.slug AS vendor_slug
            FROM cheque_requests c
            LEFT JOIN projects p ON p.id = c.project_id
            LEFT JOIN vendors v ON v.id = c.vendor_id
            WHERE c.id=?
            """,
            (cheque_request_id,),
        )
        return cur.fetchone()


def get_cheque_lines(cheque_request_id: int) -> List[sqlite3.Row]:
    with conn() as database:
        cur = database.execute(
            "SELECT * FROM cheque_request_lines WHERE cheque_request_id=? ORDER BY line_no",
            (cheque_request_id,),
        )
        return cur.fetchall()


def record_export(cheque_request_id: int, project_slug: str, vendor_slug: str, year: str, path: str) -> None:
    with conn() as database:
        database.execute(
            """
            INSERT INTO exports (cheque_request_id, project_slug, vendor_slug, year, path)
            VALUES (?, ?, ?, ?, ?)
            """,
            (cheque_request_id, project_slug, vendor_slug, year, path),
        )
