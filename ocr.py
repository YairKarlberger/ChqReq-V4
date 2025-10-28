import json
import os
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import requests
from dateutil import parser as dateparser
from pdfminer.high_level import extract_text


@dataclass
class AgentResult:
    value: Optional[str]
    confidence: float
    meta: Dict[str, str]


DATE_PAT = re.compile(r"(\d{1,2}[/-]\d{1,2}[/-]\d{2,4}|\d{4}[/-]\d{1,2}[/-]\d{1,2})")
MONEY_PAT = re.compile(r"(?<!\d)(\d{1,3}(?:,\d{3})*(?:\.\d{2})|\d+\.\d{2})(?!\d)")
HST_PAT = re.compile(r"\b(\d{9})(?:\s*RT\s*0001)?\b", re.IGNORECASE)


def _clean_text(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def extract_pdf_text(path: str) -> str:
    try:
        return extract_text(path)
    except Exception:
        return ""


def extract_ocr_space(path: str) -> str:
    api_key = os.getenv("OCRSPACE_API_KEY")
    if not api_key:
        return ""
    with open(path, "rb") as f:
        resp = requests.post(
            "https://api.ocr.space/parse/image",
            data={"language": "eng", "isOverlayRequired": False},
            files={"filename": (os.path.basename(path), f)},
            headers={"apikey": api_key},
            timeout=120,
        )
    if resp.status_code != 200:
        return ""
    try:
        payload = resp.json()
    except json.JSONDecodeError:
        return ""
    texts: List[str] = []
    for result in payload.get("ParsedResults", []) or []:
        if result.get("ParsedText"):
            texts.append(result["ParsedText"])
    return "\n".join(texts)


def pick_text(native: str, ocr_text: str, native_conf: float, ocr_conf: float) -> Tuple[str, float, str]:
    if native_conf > ocr_conf:
        return native, native_conf, "native"
    if ocr_conf > native_conf:
        return ocr_text, ocr_conf, "ocr"
    # tie -> prefer native
    return native, native_conf, "native"


def agent_vendor(text: str) -> AgentResult:
    match = re.search(r"vendor[:\s]+(.+)", text, re.IGNORECASE)
    if match:
        value = _clean_text(match.group(1))
        return AgentResult(value=value, confidence=0.9, meta={"source": "keyword"})
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    if lines:
        return AgentResult(value=lines[0][:120], confidence=0.4, meta={"source": "first-line"})
    return AgentResult(value=None, confidence=0.0, meta={"source": "none"})


def agent_hst(text: str) -> AgentResult:
    match = HST_PAT.search(text)
    if match:
        number = match.group(1)
        formatted = f"{number} RT0001"
        return AgentResult(value=formatted, confidence=0.85, meta={"source": "regex"})
    return AgentResult(value=None, confidence=0.0, meta={"source": "none"})


def agent_address(text: str) -> AgentResult:
    blocks = [l.strip() for l in text.splitlines() if l.strip()]
    candidates = []
    for idx, line in enumerate(blocks):
        if "street" in line.lower() or re.search(r"\d{2,4}\s+", line):
            chunk = blocks[idx : idx + 4]
            candidates.append(
                AgentResult(
                    value="\n".join(chunk),
                    confidence=0.75,
                    meta={"source": "street", "lines": json.dumps(chunk)},
                )
            )
    if candidates:
        return max(candidates, key=lambda c: c.confidence)
    if blocks:
        return AgentResult(value="\n".join(blocks[:3]), confidence=0.4, meta={"source": "top-lines"})
    return AgentResult(value=None, confidence=0.0, meta={"source": "none"})


def agent_description(text: str) -> AgentResult:
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    for line in lines:
        if "description" in line.lower():
            return AgentResult(value=line, confidence=0.6, meta={"source": "keyword"})
    snippet = " ".join(lines[:2])[:200] if lines else None
    return AgentResult(value=snippet, confidence=0.3 if snippet else 0.0, meta={"source": "snippet"})


def agent_invoice_number(text: str) -> AgentResult:
    match = re.search(r"invoice\s*(no\.?|#)?[:\s]*([\w-]+)", text, re.IGNORECASE)
    if match:
        return AgentResult(value=match.group(2), confidence=0.9, meta={"source": "regex"})
    generic = re.findall(r"INV[\w-]+", text)
    if generic:
        return AgentResult(value=generic[0], confidence=0.5, meta={"source": "inv"})
    return AgentResult(value=None, confidence=0.0, meta={"source": "none"})


def agent_invoice_date(text: str) -> AgentResult:
    match = DATE_PAT.search(text)
    if not match:
        return AgentResult(value=None, confidence=0.0, meta={"source": "none"})
    raw = match.group(1)
    try:
        dt = dateparser.parse(raw, dayfirst=False)
        if dt:
            return AgentResult(value=dt.date().isoformat(), confidence=0.8, meta={"raw": raw})
    except (ValueError, OverflowError):
        pass
    return AgentResult(value=raw, confidence=0.5, meta={"source": "unparsed"})


def agent_line_items(text: str) -> AgentResult:
    lines = [l for l in text.splitlines() if l.strip()]
    rows: List[Dict[str, str]] = []
    for line in lines:
        numbers = MONEY_PAT.findall(line)
        qty_match = re.search(r"\b(\d+(?:\.\d+)?)\b", line)
        if len(numbers) >= 1 and qty_match:
            row = {
                "qty": qty_match.group(1),
                "description": line[:80].strip(),
                "coding": "",
                "unit_price": numbers[0],
                "tax": numbers[1] if len(numbers) > 1 else "",
                "line_total": numbers[-1],
            }
            rows.append(row)
    if rows:
        return AgentResult(value=json.dumps(rows), confidence=0.6, meta={"count": str(len(rows))})
    return AgentResult(value=json.dumps([]), confidence=0.1, meta={"source": "none"})


def agent_tax(text: str, tax_label: str) -> AgentResult:
    pattern = re.compile(rf"{tax_label}\s*[:$]?\s*(\d[\d,]*\.\d{{2}})", re.IGNORECASE)
    match = pattern.search(text)
    if match:
        return AgentResult(value=match.group(1), confidence=0.85, meta={"source": "regex"})
    return AgentResult(value=None, confidence=0.0, meta={"source": "none"})


def agent_amount(text: str, label: str) -> AgentResult:
    pattern = re.compile(rf"{label}\s*[:$]?\s*(\d[\d,]*\.\d{{2}})", re.IGNORECASE)
    match = pattern.search(text)
    if match:
        return AgentResult(value=match.group(1), confidence=0.9, meta={"source": "regex"})
    totals = MONEY_PAT.findall(text)
    if totals:
        return AgentResult(value=totals[-1], confidence=0.4, meta={"source": "fallback"})
    return AgentResult(value=None, confidence=0.0, meta={"source": "none"})


def run_agents(path: str) -> Dict[str, Dict[str, Optional[str]]]:
    native_text = extract_pdf_text(path)
    ocr_text = extract_ocr_space(path)

    agents = {
        "vendor": agent_vendor,
        "hst": agent_hst,
        "address": agent_address,
        "description": agent_description,
        "invoice_number": agent_invoice_number,
        "invoice_date": agent_invoice_date,
    }

    results: Dict[str, Dict[str, Optional[str]]] = {}
    for key, func in agents.items():
        native_result = func(native_text)
        ocr_result = func(ocr_text) if ocr_text else AgentResult(None, 0.0, {"source": "skip"})
        value, confidence, picked = pick_text(
            native_result.value or "",
            ocr_result.value or "",
            native_result.confidence,
            ocr_result.confidence,
        )
        meta = {"picked": picked, "native": native_result.meta, "ocr": ocr_result.meta}
        if value:
            meta["value"] = value
        results[key] = {"value": value or None, "confidence": max(native_result.confidence, ocr_result.confidence), "meta": meta}

    # line items
    native_lines = agent_line_items(native_text)
    ocr_lines = agent_line_items(ocr_text) if ocr_text else AgentResult("[]", 0.0, {"source": "skip"})
    line_value, _, picked = pick_text(
        native_lines.value or "[]",
        ocr_lines.value or "[]",
        native_lines.confidence,
        ocr_lines.confidence,
    )
    results["line_items"] = {
        "value": json.loads(line_value or "[]"),
        "confidence": max(native_lines.confidence, ocr_lines.confidence),
        "meta": {"picked": picked},
    }

    for tax_label, key in [("HST", "hst_amount"), ("PST", "pst_amount"), ("GST", "gst_amount")]:
        native_tax = agent_tax(native_text, tax_label)
        ocr_tax = agent_tax(ocr_text, tax_label) if ocr_text else AgentResult(None, 0.0, {"source": "skip"})
        value, confidence, picked = pick_text(
            native_tax.value or "",
            ocr_tax.value or "",
            native_tax.confidence,
            ocr_tax.confidence,
        )
        results[key] = {"value": value or None, "confidence": confidence, "meta": {"picked": picked}}

    for label, key in [("Subtotal", "amount_before_hst"), ("Total", "amount_total")]:
        native_amount = agent_amount(native_text, label)
        ocr_amount = agent_amount(ocr_text, label) if ocr_text else AgentResult(None, 0.0, {"source": "skip"})
        value, confidence, picked = pick_text(
            native_amount.value or "",
            ocr_amount.value or "",
            native_amount.confidence,
            ocr_amount.confidence,
        )
        results[key] = {"value": value or None, "confidence": confidence, "meta": {"picked": picked}}

    return results
