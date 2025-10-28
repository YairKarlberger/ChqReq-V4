import base64
import json
import os
import shutil
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from flask import (
    Flask,
    Response,
    flash,
    jsonify,
    redirect,
    render_template,
    request,
    send_file,
    url_for,
)

import db
from db import get_cheque_lines, get_cheque_request, init_db, list_in_progress, list_projects, list_recent_completed, list_vendors, record_cheque_request, record_export, save_cheque_lines, set_status, upsert_vendor
from duplicate import compute_fingerprints, find_duplicates
from ocr import extract_pdf_text, run_agents
from pdf_build import fill_front_page, merge_with_invoice

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET", "dev-secret")

UPLOAD_DIR = Path("uploads")
ARCHIVE_DIR = Path("archive/projects")
SIGNATURE_DIR = Path("static/signatures")

STAGES = ["draft", "requested", "dept", "pm", "producer", "acctg", "studio", "completed"]


def ensure_dirs():
    for directory in [UPLOAD_DIR, ARCHIVE_DIR, SIGNATURE_DIR]:
        directory.mkdir(parents=True, exist_ok=True)


def seed_projects():
    with db.conn() as database:
        cur = database.execute("SELECT COUNT(*) AS c FROM projects")
        count = cur.fetchone()[0]
        if count == 0:
            database.execute(
                "INSERT INTO projects (name, slug) VALUES (?, ?)",
                ("General Project", "general-project"),
            )


def next_stage(current: str) -> str:
    try:
        idx = STAGES.index(current)
    except ValueError:
        return "requested"
    return STAGES[min(idx + 1, len(STAGES) - 1)]


@app.before_first_request
def setup():
    init_db()
    ensure_dirs()
    seed_projects()


@app.route("/")
def home():
    return redirect(url_for("request_form"))


def blank_form() -> Dict[str, Optional[str]]:
    return {
        "company": "",
        "project_id": "",
        "date": datetime.utcnow().date().isoformat(),
        "currency": "CAD",
        "department": "",
        "po_desc": "",
        "vendor_name": "",
        "vendor_id": "",
        "vendor_address_text": "",
        "hst_number": "",
        "invoice_number": "",
        "invoice_date": "",
        "line_items": [],
        "amount_before_hst": "",
        "hst_amount": "",
        "pst_amount": "",
        "gst_amount": "",
        "amount_total": "",
        "notes": "",
        "uploaded_invoice": "",
    }


def parse_line_items(form) -> List[Dict[str, str]]:
    line_items = []
    for key, value in form.items():
        if key.startswith("line_items"):
            # handled below via multi dict
            pass
    # Use getlist for each field indexes
    items: Dict[str, Dict[str, str]] = {}
    for full_key in form:
        if not full_key.startswith("line_items["):
            continue
        base, rest = full_key.split("[", 1)
        index = rest.split("]", 1)[0]
        field = rest.split("]", 1)[1]
        field = field.strip("[]")
        items.setdefault(index, {})[field] = form.get(full_key)
    for idx in sorted(items, key=lambda x: int(x)):
        entry = items[idx]
        if any((entry.get("description") or entry.get("line_total") or entry.get("qty"))):
            line_items.append(
                {
                    "qty": entry.get("qty", ""),
                    "description": entry.get("description", ""),
                    "coding": entry.get("coding", ""),
                    "unit_price": entry.get("unit_price", ""),
                    "tax": entry.get("tax", ""),
                    "line_total": entry.get("line_total", ""),
                }
            )
    return line_items


def store_form_payload(cheque_id: int, payload: Dict[str, Any]) -> None:
    folder = UPLOAD_DIR / str(cheque_id)
    folder.mkdir(parents=True, exist_ok=True)
    with open(folder / "form.json", "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def load_form_payload(cheque_id: int) -> Dict[str, Any]:
    path = UPLOAD_DIR / str(cheque_id) / "form.json"
    if path.exists():
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return blank_form()


def save_uploaded_file(file_storage, existing_path: Optional[str] = None) -> str:
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    filename = file_storage.filename or "invoice.pdf"
    ext = Path(filename).suffix or ".pdf"
    fd, temp_path = tempfile.mkstemp(dir=UPLOAD_DIR, suffix=ext)
    os.close(fd)
    file_storage.save(temp_path)
    if existing_path:
        try:
            os.remove(existing_path)
        except OSError:
            pass
    return os.path.basename(temp_path)


def move_invoice_to_record(temp_name: str, cheque_id: int) -> str:
    source = UPLOAD_DIR / temp_name
    if not source.exists():
        return ""
    target_dir = UPLOAD_DIR / str(cheque_id)
    target_dir.mkdir(parents=True, exist_ok=True)
    final_path = target_dir / f"invoice{source.suffix}"
    shutil.move(str(source), final_path)
    return str(final_path)


def get_invoice_path(cheque_id: int) -> Optional[str]:
    folder = UPLOAD_DIR / str(cheque_id)
    if not folder.exists():
        return None
    for file in folder.iterdir():
        if file.name.startswith("invoice"):
            return str(file)
    return None


def build_form_from_db(cheque_id: int) -> Dict[str, any]:
    cheque = get_cheque_request(cheque_id)
    if not cheque:
        return blank_form()
    payload = load_form_payload(cheque_id)
    payload.update(
        {
            "id": cheque_id,
            "project_id": cheque["project_id"],
            "vendor_id": cheque["vendor_id"],
            "vendor_name": cheque["vendor_name"],
            "invoice_number": cheque["invoice_number"],
            "invoice_date": cheque["invoice_date"],
            "amount_total": cheque["amount_total"],
            "amount_before_hst": cheque["amount_before_hst"],
            "hst_amount": cheque["hst_amount"],
            "pst_amount": cheque["pst_amount"],
            "gst_amount": cheque["gst_amount"],
            "vendor_address_text": cheque["vendor_address_text"],
            "po_desc": cheque["po_desc"],
            "department": cheque["department"],
            "currency": cheque["currency"],
            "date": cheque["date"],
        }
    )
    lines = get_cheque_lines(cheque_id)
    payload["line_items"] = [
        {
            "qty": row["qty"],
            "description": row["description"],
            "coding": row["coding"],
            "unit_price": row["unit_price"],
            "tax": row["tax"],
            "line_total": row["line_total"],
        }
        for row in lines
    ]
    return payload


@app.route("/request")
def request_form():
    form_data = blank_form()
    projects = list_projects()
    vendors = list_vendors()
    return render_template(
        "request.html",
        active="request",
        form=form_data,
        projects=projects,
        vendors=vendors,
        extracted=None,
        submit_action=url_for("save_draft"),
        status_action="draft",
    )


@app.route("/upload", methods=["POST"])
def upload_invoice():
    file = request.files.get("invoice")
    if not file:
        flash("Please choose an invoice file to upload.")
        return redirect(url_for("request_form"))
    temp_name = save_uploaded_file(file)
    path = UPLOAD_DIR / temp_name
    extracted = run_agents(str(path))
    form_data = blank_form()
    form_data.update({
        "vendor_name": extracted.get("vendor", {}).get("value"),
        "invoice_number": extracted.get("invoice_number", {}).get("value"),
        "invoice_date": extracted.get("invoice_date", {}).get("value"),
        "amount_total": extracted.get("amount_total", {}).get("value"),
        "amount_before_hst": extracted.get("amount_before_hst", {}).get("value"),
        "hst_number": extracted.get("hst", {}).get("value"),
        "vendor_address_text": extracted.get("address", {}).get("value"),
        "line_items": extracted.get("line_items", {}).get("value", []),
    })
    form_data["uploaded_invoice"] = temp_name
    flash("Invoice processed. Review and complete the form.")
    return render_template(
        "request.html",
        active="request",
        form=form_data,
        projects=list_projects(),
        vendors=list_vendors(),
        extracted=extracted,
        submit_action=url_for("save_draft"),
        status_action="draft",
    )


def handle_save(submit: bool = False, fingerprint_a: Optional[str] = None, fingerprint_b: Optional[str] = None):
    form = request.form.to_dict(flat=False)
    single_form = {k: v[-1] for k, v in form.items()}
    cheque_id = single_form.get("cheque_request_id")
    cheque_id = int(cheque_id) if cheque_id else None
    uploaded_invoice = single_form.get("uploaded_invoice")

    line_items = parse_line_items(request.form)

    vendor_name = single_form.get("vendor_name", "").strip()
    vendor_id = single_form.get("vendor_id")
    vendor_db_id = int(vendor_id) if vendor_id else None

    existing = get_cheque_request(cheque_id) if cheque_id else None
    current_status = existing["status"] if existing else "draft"

    address_text = single_form.get("vendor_address_text", "") or ""
    address_lines = [line.strip() for line in address_text.splitlines() if line.strip()]
    address1 = address_lines[0] if address_lines else ""
    address2 = address_lines[1] if len(address_lines) > 1 else ""
    city_prov = address_lines[2] if len(address_lines) > 2 else ""
    postal_code = address_lines[3] if len(address_lines) > 3 else ""

    vendor_payload = {
        "name": vendor_name,
        "address1": address1,
        "address2": address2,
        "city_prov": city_prov,
        "region": "",
        "postal_code": postal_code,
        "hst_number": single_form.get("hst_number"),
    }
    if vendor_name:
        vendor_db_id = upsert_vendor(vendor_payload)

    payload = {
        "id": cheque_id,
        "date": single_form.get("date"),
        "invoice_date": single_form.get("invoice_date"),
        "currency": single_form.get("currency"),
        "department": single_form.get("department"),
        "po_desc": single_form.get("po_desc"),
        "vendor_address_text": single_form.get("vendor_address_text"),
        "amount_before_hst": single_form.get("amount_before_hst"),
        "hst_amount": single_form.get("hst_amount"),
        "pst_amount": single_form.get("pst_amount"),
        "gst_amount": single_form.get("gst_amount"),
        "amount_total": single_form.get("amount_total"),
        "invoice_number": single_form.get("invoice_number"),
        "po_number": single_form.get("po_number"),
        "status": current_status,
        "fingerprint_a": fingerprint_a,
        "fingerprint_b": fingerprint_b,
    }

    if existing:
        payload["requested_at"] = existing["requested_at"]
    elif submit:
        payload["requested_at"] = datetime.utcnow().isoformat()
    else:
        payload["requested_at"] = None

    project_id = single_form.get("project_id")
    project_id = int(project_id) if project_id else None

    cheque_id = record_cheque_request(project_id, vendor_db_id, payload)
    save_cheque_lines(cheque_id, line_items)

    form_payload = {
        "company": single_form.get("company"),
        "project_id": project_id,
        "vendor_id": vendor_db_id,
        "vendor_name": vendor_name,
        "vendor_address_text": single_form.get("vendor_address_text"),
        "notes": single_form.get("notes"),
        "line_items": line_items,
        "currency": single_form.get("currency"),
    }
    store_form_payload(cheque_id, form_payload)

    if uploaded_invoice and Path(UPLOAD_DIR / uploaded_invoice).exists():
        move_invoice_to_record(uploaded_invoice, cheque_id)

    if submit:
        if current_status == "draft":
            set_status(cheque_id, "requested")
            flash("Cheque request submitted for approval.")
        else:
            flash(f"Updates saved. Current status: {current_status}.")
    else:
        flash("Draft saved.")

    return redirect(url_for("approvals" if submit else "request_form"))


@app.route("/save", methods=["POST"])
def save_draft():
    return handle_save(submit=False)


@app.route("/submit", methods=["POST"])
def submit_request():
    cheque_id = request.form.get("cheque_request_id")
    uploaded_invoice = request.form.get("uploaded_invoice")
    if not uploaded_invoice and cheque_id:
        uploaded_invoice = None
    if uploaded_invoice:
        invoice_path = UPLOAD_DIR / uploaded_invoice
    elif cheque_id:
        invoice_path = get_invoice_path(int(cheque_id))
    else:
        invoice_path = None

    if invoice_path and Path(invoice_path).exists():
        text = extract_pdf_text(str(invoice_path))
    else:
        text = ""
    meta = {
        "vendor": request.form.get("vendor_name"),
        "invoice_number": request.form.get("invoice_number"),
        "amount_total": request.form.get("amount_total"),
        "invoice_date": request.form.get("invoice_date"),
    }
    fp_a, fp_b = compute_fingerprints(text, meta)
    duplicates = find_duplicates(fp_a, fp_b)
    if cheque_id:
        try:
            current_id = int(cheque_id)
            duplicates = [d for d in duplicates if d != current_id]
        except ValueError:
            pass
    duplicate_warning = None
    if duplicates:
        duplicate_warning = f"Potential duplicate of IDs: {', '.join(map(str, duplicates))}"
        flash(duplicate_warning)
    return handle_save(submit=True, fingerprint_a=fp_a, fingerprint_b=fp_b)


@app.route("/approvals")
def approvals():
    return render_template(
        "approvals.html",
        active="approvals",
        requests=list_in_progress(),
    )


@app.route("/review/<int:cheque_id>")
def review_request(cheque_id: int):
    cheque = get_cheque_request(cheque_id)
    if not cheque:
        flash("Cheque request not found.")
        return redirect(url_for("approvals"))
    with db.conn() as database:
        cur = database.execute(
            "SELECT * FROM approval_events WHERE cheque_request_id=? ORDER BY at",
            (cheque_id,),
        )
        signatures = cur.fetchall()
    return render_template(
        "review.html",
        active="approvals",
        cheque=cheque,
        signatures=signatures,
    )


def save_signature_image(cheque_id: int, data_url: str) -> Optional[str]:
    if not data_url:
        return None
    header, _, encoded = data_url.partition(",")
    if not encoded:
        encoded = header
    image_bytes = base64.b64decode(encoded)
    folder = SIGNATURE_DIR / str(cheque_id)
    folder.mkdir(parents=True, exist_ok=True)
    filename = datetime.utcnow().strftime("%Y%m%d%H%M%S") + ".png"
    path = folder / filename
    with open(path, "wb") as f:
        f.write(image_bytes)
    return str(path)


@app.route("/review/<int:cheque_id>/sign", methods=["POST"])
def sign_request(cheque_id: int):
    cheque = get_cheque_request(cheque_id)
    if not cheque:
        flash("Cheque request not found.")
        return redirect(url_for("approvals"))
    signer_name = request.form.get("signer_name")
    note = request.form.get("note", "")
    signature_data = request.form.get("signature_data", "")
    signature_path = save_signature_image(cheque_id, signature_data)
    new_status = next_stage(cheque["status"])
    set_status(cheque_id, new_status, signer_name=signer_name, note=note, signature_path=signature_path)

    if new_status == "completed":
        invoice_path = get_invoice_path(cheque_id)
        if invoice_path:
            form_payload = load_form_payload(cheque_id)
            form_payload.update({
                "vendor": cheque["vendor_name"],
                "project": cheque["project_name"],
                "project_name": cheque["project_name"],
                "company": form_payload.get("company"),
                "invoice_number": cheque["invoice_number"],
                "invoice_date": cheque["invoice_date"],
                "amount_total": cheque["amount_total"],
                "amount_before_hst": cheque["amount_before_hst"],
                "hst_amount": cheque["hst_amount"],
                "pst_amount": cheque["pst_amount"],
                "gst_amount": cheque["gst_amount"],
                "address": cheque["vendor_address_text"],
                "line_items": form_payload.get("line_items", []),
            })
            front = fill_front_page(form_payload)
            final_path = UPLOAD_DIR / str(cheque_id) / "merged.pdf"
            merge_with_invoice(front, invoice_path, str(final_path))
            os.remove(front)
            export_path = archive_completed(cheque_id, cheque, str(final_path))
            if export_path:
                record_export(
                    cheque_id,
                    cheque["project_slug"],
                    cheque["vendor_slug"],
                    datetime.utcnow().strftime("%Y"),
                    export_path,
                )
    flash(f"Cheque moved to {new_status} stage.")
    return redirect(url_for("approvals"))


def archive_completed(cheque_id: int, cheque, merged_path: str) -> Optional[str]:
    project_slug = cheque["project_slug"] or db.slugify(cheque["project_name"] or f"project-{cheque_id}")
    vendor_slug = cheque["vendor_slug"] or db.slugify(cheque["vendor_name"] or f"vendor-{cheque_id}")
    year = datetime.utcnow().strftime("%Y")
    target_dir = ARCHIVE_DIR / project_slug / vendor_slug / year
    target_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    filename = f"{timestamp}_Proj-{cheque['project_name'] or project_slug}_Inv-{cheque['invoice_number'] or cheque_id}_ChequeReq.pdf"
    destination = target_dir / filename
    shutil.copyfile(merged_path, destination)
    return str(destination)


@app.route("/files")
def files_browser():
    return render_template(
        "files.html",
        active="files",
        recent=list_recent_completed(),
    )


def safe_join(base: Path, target: str) -> Path:
    joined = (base / target.lstrip("/")).resolve()
    if base.resolve() not in joined.parents and joined != base.resolve():
        raise ValueError("Invalid path")
    return joined


@app.route("/files/browse")
def browse_files():
    rel = request.args.get("q", "").lstrip("/")
    base = ARCHIVE_DIR
    target = safe_join(base, rel)
    entries = []
    if target.exists():
        for item in sorted(target.iterdir()):
            entries.append(
                {
                    "name": item.name,
                    "type": "dir" if item.is_dir() else "file",
                    "path": str(item.relative_to(base)),
                }
            )
    return jsonify({"entries": entries})


@app.route("/archive/download")
def download_archive():
    rel = request.args.get("path", "")
    try:
        path = safe_join(ARCHIVE_DIR, rel)
    except ValueError:
        flash("Invalid path")
        return redirect(url_for("files_browser"))
    if not path.exists():
        flash("File not found")
        return redirect(url_for("files_browser"))
    return send_file(path, as_attachment=True)


@app.route("/api/vendors")
def vendors_api():
    term = request.args.get("q", "")
    vendors = db.search_vendors(term) if term else []
    return jsonify(vendors)


@app.route("/api/vendor")
def vendor_detail():
    name = request.args.get("name")
    if not name:
        return jsonify({}), 400
    vendor = db.get_vendor_by_name(name)
    if not vendor:
        return jsonify({}), 404
    address_parts = [vendor["address1"], vendor["address2"], vendor["city_prov"], vendor["region"], vendor["postal_code"]]
    address_text = "\n".join([part for part in address_parts if part])
    return jsonify({
        "id": vendor["id"],
        "name": vendor["name"],
        "address_text": address_text,
        "hst_number": vendor["hst_number"],
    })


@app.route("/preview/<int:cheque_id>")
def preview(cheque_id: int):
    cheque = get_cheque_request(cheque_id)
    if not cheque:
        flash("Cheque not found")
        return redirect(url_for("approvals"))
    invoice_path = get_invoice_path(cheque_id)
    if not invoice_path:
        flash("Invoice missing")
        return redirect(url_for("approvals"))
    form_payload = load_form_payload(cheque_id)
    form_payload.update({
        "project_name": cheque["project_name"],
        "project": cheque["project_name"],
        "vendor": cheque["vendor_name"],
        "invoice_number": cheque["invoice_number"],
        "invoice_date": cheque["invoice_date"],
        "amount_total": cheque["amount_total"],
        "amount_before_hst": cheque["amount_before_hst"],
        "hst_amount": cheque["hst_amount"],
        "pst_amount": cheque["pst_amount"],
        "gst_amount": cheque["gst_amount"],
        "address": cheque["vendor_address_text"],
    })
    front = fill_front_page(form_payload)
    fd, path = tempfile.mkstemp(suffix="_preview.pdf")
    os.close(fd)
    merge_with_invoice(front, invoice_path, path)
    os.remove(front)
    return send_file(path, as_attachment=True)


if __name__ == "__main__":
    init_db()
    ensure_dirs()
    seed_projects()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), debug=True)
