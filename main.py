from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from typing import Any, List, Optional
from uuid import uuid4

from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

app = FastAPI(title="ElevenLabs Tool Backend", version="1.0.0")

DATA_DIR = Path("data")
LEADS_FILE = DATA_DIR / "leads.json"
file_lock = Lock()
in_memory_leads: List[dict] = []


class LeadUpsert(BaseModel):
    name: str = Field(..., min_length=1)
    phone: str = Field(..., min_length=1)
    interest: str = Field(..., min_length=1)
    delete_requested: bool = False


class VisitRequest(BaseModel):
    phone: str = Field(..., min_length=1)
    day: str = Field(..., min_length=1)
    time: str = Field(..., min_length=1)


class LeadDeleteRequest(BaseModel):
    phone: str = Field(..., min_length=1)


def generate_uuid() -> str:
    return str(uuid4())


def normalize_phone(phone: str) -> str:
    return "".join(character for character in phone.strip() if character.isdigit())


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _api_error(status_code: int, message: str) -> None:
    raise HTTPException(status_code=status_code, detail=message)


def validate_phone(phone: str) -> str:
    normalized_phone = normalize_phone(phone)
    if len(normalized_phone) != 10:
        _api_error(status_code=422, message="Invalid phone, must be exactly 10 digits")
    return normalized_phone


def validate_visit_time(time_value: str) -> str:
    cleaned_time = time_value.strip()
    if not cleaned_time:
        _api_error(status_code=422, message="Invalid time, must be provided in HH:MM format")

    try:
        parsed_time = datetime.strptime(cleaned_time, "%H:%M").time()
    except ValueError:
        _api_error(status_code=422, message="Invalid time format, expected HH:MM (24-hour)")

    opening_time = datetime.strptime("06:00", "%H:%M").time()
    closing_time = datetime.strptime("22:00", "%H:%M").time()
    if parsed_time < opening_time or parsed_time > closing_time:
        _api_error(status_code=422, message="Invalid time, must be within gym hours (06:00-22:00)")

    return cleaned_time


def _normalize_persisted_phone(phone: str) -> str:
    normalized_phone = normalize_phone(phone)
    if len(normalized_phone) == 10:
        return normalized_phone
    if len(normalized_phone) == 11 and normalized_phone.startswith("1"):
        return normalized_phone[1:]
    raise ValueError("persisted phone must resolve to 10 digits")


def _require_non_empty(value: str, field_name: str) -> str:
    cleaned_value = value.strip()
    if not cleaned_value:
        _api_error(status_code=422, message=f"Invalid {field_name}, must be non-empty")
    return cleaned_value


def _normalize_text(value: Optional[str]) -> str:
    if value is None:
        return ""
    return value.strip()


def _coerce_timestamp(value: Any) -> str:
    if isinstance(value, str) and value.strip():
        return value
    return _now_iso()


def _normalize_record(record: dict) -> dict:
    if not isinstance(record, dict):
        raise ValueError("record must be a dictionary")

    created_at = _coerce_timestamp(record.get("created_at") or record.get("captured_at"))
    updated_at = _coerce_timestamp(record.get("updated_at") or created_at)

    normalized_phone = _normalize_persisted_phone(str(record.get("phone", "")))

    visit = record.get("visit")
    if not isinstance(visit, dict):
        visit = {}

    normalized_record = {
        "lead_id": str(record.get("lead_id") or generate_uuid()),
        "name": _require_non_empty(str(record.get("name", "")), "name"),
        "phone": normalized_phone,
        "interest": _require_non_empty(str(record.get("interest", "")), "interest"),
        "delete_requested": bool(record.get("delete_requested", False)),
        "created_at": created_at,
        "updated_at": updated_at,
        "visit": {
            "requested": bool(visit.get("requested", False)),
            "day": _normalize_text(visit.get("day")),
            "time": _normalize_text(visit.get("time")),
        },
    }

    return normalized_record


def find_lead_by_phone(phone: str) -> Optional[dict]:
    normalized_phone = validate_phone(phone)
    for lead in in_memory_leads:
        if lead.get("phone") == normalized_phone:
            return lead
    return None


def _atomic_write_leads(leads: List[dict]) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", delete=False, dir=DATA_DIR, suffix=".tmp", encoding="utf-8") as temp_file:
        json.dump(leads, temp_file, indent=2)
        temp_file.write("\n")
        temp_path = Path(temp_file.name)
    os.replace(temp_path, LEADS_FILE)


def safe_read_json() -> List[dict]:
    if not LEADS_FILE.exists():
        return []

    try:
        raw_data = json.loads(LEADS_FILE.read_text(encoding="utf-8"))
    except json.JSONDecodeError as error:
        raise ValueError("data/leads.json contains invalid JSON") from error

    if not isinstance(raw_data, list):
        raise ValueError("data/leads.json must contain a JSON array")

    normalized_leads_by_phone = {}
    for record in raw_data:
        normalized_record = _normalize_record(record)
        normalized_leads_by_phone[normalized_record["phone"]] = normalized_record

    return list(normalized_leads_by_phone.values())


def safe_write_json() -> None:
    _atomic_write_leads(in_memory_leads)


@app.on_event("startup")
def startup() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    if not LEADS_FILE.exists():
        LEADS_FILE.write_text("[]\n", encoding="utf-8")

    try:
        loaded_leads = safe_read_json()
    except ValueError as error:
        print(f"WARNING: {error}. Starting with empty in-memory leads.")
        loaded_leads = []
        safe_write_json()

    with file_lock:
        in_memory_leads.clear()
        in_memory_leads.extend(loaded_leads)

    if loaded_leads:
        safe_write_json()


@app.exception_handler(HTTPException)
async def handle_http_exception(_: Request, exc: HTTPException) -> JSONResponse:
    if isinstance(exc.detail, str):
        message = exc.detail
    else:
        message = "Request failed"
    return JSONResponse(status_code=exc.status_code, content={"error": message})


@app.exception_handler(RequestValidationError)
async def handle_validation_exception(_: Request, exc: RequestValidationError) -> JSONResponse:
    errors = exc.errors()
    if errors:
        message = errors[0].get("msg", "Invalid request")
    else:
        message = "Invalid request"
    return JSONResponse(status_code=422, content={"error": message})


@app.get("/")
def health() -> dict:
    return {"status": "ok"}


@app.post("/lead")
def capture_or_update_lead(payload: LeadUpsert) -> dict:
    name = _require_non_empty(payload.name, "name")
    phone = validate_phone(payload.phone)
    interest = _require_non_empty(payload.interest, "interest")

    with file_lock:
        existing_lead = find_lead_by_phone(phone)

        if existing_lead is None:
            lead = {
                "lead_id": generate_uuid(),
                "name": name,
                "phone": phone,
                "interest": interest,
                "delete_requested": bool(payload.delete_requested),
                "created_at": _now_iso(),
                "updated_at": _now_iso(),
                "visit": {
                    "requested": False,
                    "day": "",
                    "time": "",
                },
            }
            in_memory_leads.append(lead)
            action = "created"
        else:
            existing_lead["name"] = name
            existing_lead["interest"] = interest
            existing_lead["delete_requested"] = bool(payload.delete_requested)
            if "visit" not in existing_lead or not isinstance(existing_lead["visit"], dict):
                existing_lead["visit"] = {"requested": False, "day": "", "time": ""}
            existing_lead["updated_at"] = _now_iso()
            lead = existing_lead
            action = "updated"

        safe_write_json()

    print("\n=== LEAD UPSERT ===")
    print(f"Action: {action}")
    print(f"Lead ID: {lead['lead_id']}")
    print(f"Name: {lead['name']}")
    print(f"Phone: {lead['phone']}")
    print(f"Interest: {lead['interest']}")
    print(f"Delete Requested: {lead['delete_requested']}")
    print(f"Visit Requested: {lead['visit']['requested']}")
    print(f"Updated At: {lead['updated_at']}")
    print("====================\n")

    return {"status": action, "lead": lead}


@app.get("/lead/{phone}")
def get_lead(phone: str) -> dict:
    with file_lock:
        lead = find_lead_by_phone(phone)
        if lead is None:
            _api_error(status_code=404, message="Lead not found")
        return lead


@app.post("/visit")
def book_visit(payload: VisitRequest) -> dict:
    phone = validate_phone(payload.phone)
    day = _require_non_empty(payload.day, "day")
    visit_time = validate_visit_time(payload.time)

    with file_lock:
        lead = find_lead_by_phone(phone)
        if lead is None:
            _api_error(status_code=404, message="Lead not found")

        if "visit" not in lead or not isinstance(lead["visit"], dict):
            lead["visit"] = {"requested": False, "day": "", "time": ""}

        lead["visit"]["requested"] = True
        lead["visit"]["day"] = day
        lead["visit"]["time"] = visit_time
        lead["updated_at"] = _now_iso()
        safe_write_json()

    return {"status": "visit_booked", "lead": lead}


@app.get("/visits")
def get_visits() -> list[dict]:
    with file_lock:
        return [lead for lead in in_memory_leads if lead.get("visit", {}).get("requested") is True]


@app.post("/lead/delete")
def flag_lead_for_deletion(payload: LeadDeleteRequest) -> dict:
    phone = validate_phone(payload.phone)

    with file_lock:
        lead = find_lead_by_phone(phone)
        if lead is None:
            _api_error(status_code=404, message="Lead not found")

        lead["delete_requested"] = True
        lead["updated_at"] = _now_iso()
        safe_write_json()

    return {"status": "delete_requested", "lead": lead}

@app.get("/leads")
def get_leads() -> list[dict]:
    with file_lock:
        return list(in_memory_leads)