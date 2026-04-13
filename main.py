from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from typing import List

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

app = FastAPI(title="ElevenLabs Tool Backend", version="1.0.0")

DATA_DIR = Path("data")
LEADS_FILE = DATA_DIR / "leads.json"
file_lock = Lock()
in_memory_leads: List[dict] = []


class LeadIn(BaseModel):
    name: str = Field(..., min_length=1)
    phone: str = Field(..., min_length=1)
    interest: str = Field(..., min_length=1)


@app.on_event("startup")
def startup() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    if not LEADS_FILE.exists():
        LEADS_FILE.write_text("[]\n", encoding="utf-8")

    try:
        persisted = json.loads(LEADS_FILE.read_text(encoding="utf-8"))
        if isinstance(persisted, list):
            in_memory_leads.extend(item for item in persisted if isinstance(item, dict))
    except json.JSONDecodeError:
        # If file is corrupted, keep service alive and preserve new captures only.
        print("WARNING: data/leads.json is invalid JSON. Starting with empty in-memory leads.")


@app.get("/")
def health() -> dict:
    return {"status": "ok"}


@app.post("/lead")
def capture_lead(payload: LeadIn) -> dict:
    lead = {
        "name": payload.name.strip(),
        "phone": payload.phone.strip(),
        "interest": payload.interest.strip(),
        "captured_at": datetime.now(timezone.utc).isoformat(),
    }

    if not lead["name"] or not lead["phone"] or not lead["interest"]:
        raise HTTPException(status_code=422, detail="name, phone, and interest must be non-empty")

    with file_lock:
        in_memory_leads.append(lead)
        LEADS_FILE.write_text(json.dumps(in_memory_leads, indent=2) + "\n", encoding="utf-8")

    print("\n=== LEAD CAPTURED ===")
    print(f"Name: {lead['name']}")
    print(f"Phone: {lead['phone']}")
    print(f"Interest: {lead['interest']}")
    print(f"Captured At (UTC): {lead['captured_at']}")
    print("=====================\n")

    return {"status": "captured"}
