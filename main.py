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
from pydantic import BaseModel, Field

from utils import extract_and_clean_fields, load_json_array, safe_append_json_record

app = FastAPI(title="ElevenLabs Voice Agent Backend", version="1.0.0")

# Webhook storage
OUTPUT_FILE = Path("data.json")

# Lead storage
DATA_DIR = Path("data")
LEADS_FILE = DATA_DIR / "leads.json"
file_lock = Lock()
in_memory_leads: List[dict] = []


# Pydantic models for lead operations
class LeadUpsert(BaseModel):
    name: str = Field(..., min_length=1)
    phone: str = Field(..., min_length=1)
    interest: str = Field(..., min_length=1)


class VisitRequest(BaseModel):
    phone: str = Field(..., min_length=1)
    day: str = Field(..., min_length=1)
    time: str = Field(..., min_length=1)


class LeadDeleteRequest(BaseModel):
    phone: str = Field(..., min_length=1)


# Helper functions
def generate_uuid() -> str:
    return str(uuid4())


def normalize_phone(phone: str) -> str:
    """Strip non-digits from phone number."""
    return "".join(c for c in phone.strip() if c.isdigit())


def validate_phone(phone: str) -> str:
    """Validate phone is exactly 10 digits after normalization."""
    normalized = normalize_phone(phone)
    if len(normalized) != 10:
        raise HTTPException(status_code=422, detail={"phone": "phone must be exactly 10 digits"})
    return normalized


def validate_visit_time(time_str: str) -> str:
    """Validate time is within gym hours (06:00-22:00)."""
    time_str = time_str.strip()
    if not time_str:
        raise HTTPException(status_code=422, detail={"time": "time must be non-empty"})
    
    # Try to parse time in HH:MM format
    try:
        parts = time_str.split(":")
        if len(parts) != 2:
            raise ValueError("Invalid format")
        hour = int(parts[0])
        minute = int(parts[1])
        
        if not (6 <= hour <= 22):
            raise HTTPException(status_code=422, detail={"time": "time must be within gym hours (06:00-22:00)"})
    except (ValueError, IndexError):
        raise HTTPException(status_code=422, detail={"time": "time must be in HH:MM format"})
    
    return time_str


def _now_iso() -> str:
    """Get current timestamp in ISO format."""
    return datetime.now(timezone.utc).isoformat()


def _require_non_empty(value: str, field_name: str) -> str:
    """Validate field is non-empty after trimming."""
    cleaned = value.strip()
    if not cleaned:
        raise HTTPException(status_code=422, detail={field_name: f"{field_name} must be non-empty"})
    return cleaned


def _coerce_timestamp(value: Any) -> str:
    """Coerce value to ISO timestamp or return current time."""
    if isinstance(value, str) and value.strip():
        return value
    return _now_iso()


def _normalize_record(record: dict) -> dict:
    """Migrate old record format to new structured schema."""
    if not isinstance(record, dict):
        raise ValueError("record must be a dictionary")
    
    created_at = _coerce_timestamp(record.get("created_at") or record.get("captured_at"))
    updated_at = _coerce_timestamp(record.get("updated_at") or created_at)
    
    # Handle phone normalization for persisted data
    phone_str = str(record.get("phone", ""))
    normalized_phone = normalize_phone(phone_str)
    if len(normalized_phone) == 11 and normalized_phone.startswith("1"):
        normalized_phone = normalized_phone[1:]
    if len(normalized_phone) != 10:
        raise ValueError(f"persisted phone must resolve to 10 digits, got: {normalized_phone}")
    
    # Handle visit data
    visit = record.get("visit")
    if not isinstance(visit, dict):
        visit = {}
    
    return {
        "lead_id": str(record.get("lead_id") or generate_uuid()),
        "name": _require_non_empty(str(record.get("name", "")), "name"),
        "phone": normalized_phone,
        "interest": _require_non_empty(str(record.get("interest", "")), "interest"),
        "delete_requested": bool(record.get("delete_requested", False)),
        "created_at": created_at,
        "updated_at": updated_at,
        "visit": {
            "requested": bool(visit.get("requested", False)),
            "day": str(visit.get("day", "")).strip(),
            "time": str(visit.get("time", "")).strip(),
        },
    }


def find_lead_by_phone(phone: str) -> Optional[dict]:
    """Find a lead by normalized phone number."""
    normalized_phone = validate_phone(phone)
    for lead in in_memory_leads:
        if lead.get("phone") == normalized_phone:
            return lead
    return None


def _atomic_write_leads(leads: List[dict]) -> None:
    """Write leads to disk atomically."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", delete=False, dir=DATA_DIR, suffix=".tmp", encoding="utf-8") as f:
        json.dump(leads, f, indent=2)
        f.write("\n")
        temp_path = Path(f.name)
    os.replace(temp_path, LEADS_FILE)


def safe_read_json() -> List[dict]:
    """Safely read and migrate leads from disk."""
    if not LEADS_FILE.exists():
        return []
    
    try:
        raw_data = json.loads(LEADS_FILE.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        raise ValueError("data/leads.json contains invalid JSON") from e
    
    if not isinstance(raw_data, list):
        raise ValueError("data/leads.json must contain a JSON array")
    
    # Deduplicate by phone and normalize all records
    by_phone = {}
    for record in raw_data:
        normalized = _normalize_record(record)
        by_phone[normalized["phone"]] = normalized
    
    return list(by_phone.values())


def safe_write_json() -> None:
    """Safely write leads to disk."""
    _atomic_write_leads(in_memory_leads)


@app.on_event("startup")
def startup() -> None:
    """Load leads from disk on startup."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    if not LEADS_FILE.exists():
        LEADS_FILE.write_text("[]\\n", encoding="utf-8")
    
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


@app.get("/")
def health() -> dict:
    """Health check endpoint."""
    return {"status": "ok"}


# ========== LEAD CAPTURE ENDPOINTS ==========

@app.post("/lead")
def capture_or_update_lead(payload: LeadUpsert) -> dict:
    """Create or update a lead by phone."""
    name = _require_non_empty(payload.name, "name")
    phone = validate_phone(payload.phone)
    interest = _require_non_empty(payload.interest, "interest")
    
    with file_lock:
        existing_lead = find_lead_by_phone(phone)
        
        if existing_lead is None:
            # Create new lead
            lead = {
                "lead_id": generate_uuid(),
                "name": name,
                "phone": phone,
                "interest": interest,
                "delete_requested": False,
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
            # Update existing lead
            existing_lead["name"] = name
            existing_lead["interest"] = interest
            existing_lead["updated_at"] = _now_iso()
            if "visit" not in existing_lead or not isinstance(existing_lead["visit"], dict):
                existing_lead["visit"] = {"requested": False, "day": "", "time": ""}
            lead = existing_lead
            action = "updated"
        
        safe_write_json()
    
    print(f"\\n=== LEAD {action.upper()} ===")
    print(f"Lead ID: {lead['lead_id']}")
    print(f"Name: {lead['name']}")
    print(f"Phone: {lead['phone']}")
    print(f"Interest: {lead['interest']}")
    print("====================\\n")
    
    return {"status": action, "lead": lead}


@app.get("/lead/{phone}")
def get_lead(phone: str) -> dict:
    """Retrieve a single lead by phone."""
    with file_lock:
        lead = find_lead_by_phone(phone)
        if lead is None:
            raise HTTPException(status_code=404, detail={"error": "lead not found"})
        return lead


@app.post("/visit")
def book_visit(payload: VisitRequest) -> dict:
    """Book a visit for an existing lead."""
    phone = validate_phone(payload.phone)
    day = _require_non_empty(payload.day, "day")
    time_str = validate_visit_time(payload.time)
    
    with file_lock:
        lead = find_lead_by_phone(phone)
        if lead is None:
            raise HTTPException(status_code=404, detail={"error": "lead not found"})
        
        if "visit" not in lead or not isinstance(lead["visit"], dict):
            lead["visit"] = {"requested": False, "day": "", "time": ""}
        
        lead["visit"]["requested"] = True
        lead["visit"]["day"] = day
        lead["visit"]["time"] = time_str
        lead["updated_at"] = _now_iso()
        safe_write_json()
    
    print(f"\\n=== VISIT BOOKED ===")
    print(f"Lead Phone: {lead['phone']}")
    print(f"Day: {day}")
    print(f"Time: {time_str}")
    print("====================\\n")
    
    return {"status": "visit_booked", "lead": lead}


@app.get("/visits")
def get_visits() -> list[dict]:
    """Get all leads with visit.requested = true."""
    with file_lock:
        return [lead for lead in in_memory_leads if lead.get("visit", {}).get("requested") is True]


@app.get("/leads")
def get_leads() -> list[dict]:
    """Get all leads."""
    with file_lock:
        return list(in_memory_leads)


@app.post("/lead/delete")
def flag_lead_for_deletion(payload: LeadDeleteRequest) -> dict:
    """Soft-delete: mark a lead as delete_requested."""
    phone = validate_phone(payload.phone)
    
    with file_lock:
        lead = find_lead_by_phone(phone)
        if lead is None:
            raise HTTPException(status_code=404, detail={"error": "lead not found"})
        
        lead["delete_requested"] = True
        lead["updated_at"] = _now_iso()
        safe_write_json()
    
    return {"status": "delete_requested", "lead": lead}


# ========== WEBHOOK ENDPOINTS ==========

@app.get("/records")
def get_records() -> list[dict]:
    """Get all webhook-processed records."""
    try:
        return load_json_array(OUTPUT_FILE)
    except Exception as error:
        print(f"WARNING: failed to read records: {error}")
        return []


@app.post("/webhook")
async def webhook(request: Request) -> dict:
    """Process ElevenLabs post-call webhook."""
    payload = {}

    try:
        payload = await request.json()
        if not isinstance(payload, dict):
            print("WARNING: webhook payload is not a JSON object")
            payload = {}
    except Exception as error:
        print(f"WARNING: invalid JSON payload: {error}")

    print("\\n=== INCOMING WEBHOOK PAYLOAD ===")
    try:
        print(json.dumps(payload, indent=2, ensure_ascii=True))
    except Exception:
        print(str(payload))
    print("================================\\n")

    parsed_data = extract_and_clean_fields(payload)

    print("\\n=== PARSED DATA ===")
    print(json.dumps(parsed_data, indent=2, ensure_ascii=True))
    print("===================\\n")

    should_skip = parsed_data.get("name") is None and parsed_data.get("phone") is None
    if should_skip:
        print("INFO: skipping storage because both name and phone are null")
    else:
        try:
            safe_append_json_record(OUTPUT_FILE, parsed_data)
        except Exception as error:
            print(f"WARNING: failed to store webhook record: {error}")

    print("\\n=== FINAL STORED OBJECT ===")
    print(json.dumps(parsed_data, indent=2, ensure_ascii=True))
    print("===========================\\n")

    return {"status": "ok"}

