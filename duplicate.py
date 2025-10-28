import hashlib
from typing import Dict, List, Optional

from db import conn


def normalize_text(text: str) -> str:
    return " ".join((text or "").lower().split())


def compute_fingerprints(text: str, meta: Dict[str, Optional[str]]):
    normalized = normalize_text(text)
    fp_a = hashlib.sha256(normalized.encode("utf-8")).hexdigest() if len(normalized) > 200 else None
    vendor = (meta.get("vendor") or "").strip().lower()
    invoice = (meta.get("invoice_number") or "").strip().lower()
    total = (meta.get("amount_total") or "").strip().lower()
    inv_date = (meta.get("invoice_date") or "").strip().lower()
    concat = "|".join(filter(None, [vendor, invoice, total, inv_date]))
    fp_b = hashlib.sha256(concat.encode("utf-8")).hexdigest() if concat else None
    return fp_a, fp_b


def find_duplicates(fp_a: Optional[str], fp_b: Optional[str]) -> List[int]:
    clauses = []
    params = []
    if fp_a:
        clauses.append("fingerprint_a = ?")
        params.append(fp_a)
    if fp_b:
        clauses.append("fingerprint_b = ?")
        params.append(fp_b)
    if not clauses:
        return []
    where = " OR ".join(clauses)
    with conn() as database:
        cur = database.execute(
            f"SELECT id FROM cheque_requests WHERE {where}",
            tuple(params) if len(params) > 1 else (params[0],) if params else (),
        )
        return [row["id"] for row in cur.fetchall()]
