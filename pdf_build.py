import io
import os
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

from PIL import Image
from pypdf import PdfReader, PdfWriter
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas

TEMPLATE_PATH = Path("assets/cheque_request_template.pdf")


FIELD_COORDS = {
    "vendor": (80, 600),
    "address": (80, 580),
    "invoice_number": (400, 650),
    "invoice_date": (400, 630),
    "amount_before_hst": (430, 190),
    "hst_amount": (430, 170),
    "pst_amount": (430, 150),
    "gst_amount": (430, 130),
    "amount_total": (430, 110),
    "project": (120, 650),
    "company": (80, 670),
    "date": (300, 650),
    "currency": (320, 630),
}

LINE_START = 460
LINE_HEIGHT = 20


def _draw_text(can: canvas.Canvas, text: str, x: float, y: float, max_width: float = 440):
    if not text:
        return
    lines = text.split("\n")
    for idx, line in enumerate(lines):
        can.drawString(x, y - idx * 12, line[:150])


def _ensure_template() -> None:
    if not TEMPLATE_PATH.exists():
        raise FileNotFoundError(f"Template not found at {TEMPLATE_PATH}")


def fill_front_page(form_data: Dict[str, str]) -> str:
    _ensure_template()
    overlay_fd, overlay_path = tempfile.mkstemp(suffix="_overlay.pdf")
    os.close(overlay_fd)
    can = canvas.Canvas(overlay_path, pagesize=letter)
    can.setFont("Helvetica", 10)

    # Project banner
    can.setFillColor(colors.HexColor("#1e3a8a"))
    can.rect(72, 692, 200, 24, fill=1, stroke=0)
    can.setFillColor(colors.white)
    can.setFont("Helvetica-Bold", 12)
    can.drawString(80, 702, (form_data.get("project_name") or "").upper())
    can.setFillColor(colors.black)
    can.setFont("Helvetica", 10)

    for field, (x, y) in FIELD_COORDS.items():
        value = form_data.get(field)
        if value:
            _draw_text(can, str(value), x, y)

    # Line items
    line_items: List[Dict[str, str]] = form_data.get("line_items") or []
    for idx in range(min(len(line_items), 6)):
        row = line_items[idx]
        y = LINE_START - idx * LINE_HEIGHT
        _draw_text(can, (row.get("description") or "")[:80], 90, y)
        _draw_text(can, row.get("coding") or "", 320, y)
        _draw_text(can, row.get("line_total") or "", 460, y)

    notes = form_data.get("notes")
    if notes:
        _draw_text(can, notes, 90, 120)

    can.save()

    template_reader = PdfReader(str(TEMPLATE_PATH))
    overlay_reader = PdfReader(overlay_path)
    template_page = template_reader.pages[0]
    template_page.merge_page(overlay_reader.pages[0])

    out_fd, out_path = tempfile.mkstemp(suffix="_front.pdf")
    os.close(out_fd)
    writer = PdfWriter()
    writer.add_page(template_page)
    with open(out_path, "wb") as f:
        writer.write(f)
    os.remove(overlay_path)
    return out_path


def _image_to_pdf(path: str) -> str:
    image = Image.open(path)
    pdf_bytes = io.BytesIO()
    if image.mode == "RGBA":
        image = image.convert("RGB")
    image.save(pdf_bytes, format="PDF")
    pdf_bytes.seek(0)
    fd, temp_path = tempfile.mkstemp(suffix="_img.pdf")
    os.close(fd)
    with open(temp_path, "wb") as f:
        f.write(pdf_bytes.read())
    return temp_path


def merge_with_invoice(front_pdf: str, invoice_path: str, out_pdf: str) -> str:
    invoice_path = str(invoice_path)
    ext = Path(invoice_path).suffix.lower()
    working_invoice = invoice_path
    try:
        if ext in {".png", ".jpg", ".jpeg"}:
            working_invoice = _image_to_pdf(invoice_path)
        writer = PdfWriter()
        front_reader = PdfReader(front_pdf)
        invoice_reader = PdfReader(working_invoice)
        for page in front_reader.pages:
            writer.add_page(page)
        for page in invoice_reader.pages:
            writer.add_page(page)
        with open(out_pdf, "wb") as f:
            writer.write(f)
    finally:
        if working_invoice != invoice_path and os.path.exists(working_invoice):
            os.remove(working_invoice)
    return out_pdf
