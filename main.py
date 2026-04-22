from __future__ import annotations

import json
from pathlib import Path

from fastapi import FastAPI, Request

from utils import extract_and_clean_fields, safe_append_json_record

app = FastAPI(title="ElevenLabs Post-Call Webhook POC", version="1.0.0")

OUTPUT_FILE = Path("data.json")


@app.get("/")
def health() -> dict:
    return {"status": "ok"}


@app.post("/webhook")
async def webhook(request: Request) -> dict:
    payload = {}

    try:
        payload = await request.json()
        if not isinstance(payload, dict):
            print("WARNING: webhook payload is not a JSON object")
            payload = {}
    except Exception as error:
        print(f"WARNING: invalid JSON payload: {error}")

    print("\n=== INCOMING WEBHOOK PAYLOAD ===")
    try:
        print(json.dumps(payload, indent=2, ensure_ascii=True))
    except Exception:
        print(str(payload))
    print("================================\n")

    parsed_data = extract_and_clean_fields(payload)

    print("\n=== PARSED DATA ===")
    print(json.dumps(parsed_data, indent=2, ensure_ascii=True))
    print("===================\n")

    should_skip = parsed_data.get("name") is None and parsed_data.get("phone") is None
    if should_skip:
        print("INFO: skipping storage because both name and phone are null")
    else:
        try:
            safe_append_json_record(OUTPUT_FILE, parsed_data)
        except Exception as error:
            print(f"WARNING: failed to store webhook record: {error}")

    print("\n=== FINAL STORED OBJECT ===")
    print(json.dumps(parsed_data, indent=2, ensure_ascii=True))
    print("===========================\n")

    # Requirement: always return status ok
    return {"status": "ok"}
