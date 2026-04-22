from __future__ import annotations

import json
import os
import re
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from typing import Any

write_lock = Lock()


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def empty_to_none(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def normalize_phone(value: Any) -> str:
    text = "" if value is None else str(value)
    return "".join(ch for ch in text if ch.isdigit())


def validate_phone_or_none(value: Any) -> str | None:
    digits = normalize_phone(value)
    return digits if len(digits) == 10 else None


def normalize_intent(value: Any) -> str | None:
    text = empty_to_none(value)
    if text is None:
        return None

    lowered = text.lower()

    if lowered in {"join", "visit", "enquiry", "delete"}:
        return lowered

    intent_map = {
        "want to visit": "visit",
        "interested in joining": "join",
    }

    return intent_map.get(lowered)


def normalize_time_to_24h(value: Any) -> str | None:
    text = empty_to_none(value)
    if text is None:
        return None

    normalized = text.lower().strip()
    normalized = normalized.replace("a.m.", "am").replace("p.m.", "pm")
    normalized = normalized.replace("a.m", "am").replace("p.m", "pm")
    normalized = re.sub(r"\s+", " ", normalized)

    formats = [
        "%H:%M",
        "%H.%M",
        "%I:%M %p",
        "%I %p",
        "%I%p",
        "%I:%M%p",
    ]

    for fmt in formats:
        try:
            parsed = datetime.strptime(normalized, fmt)
            return parsed.strftime("%H:%M")
        except ValueError:
            continue

    return None


def get_data_point_value(data_points: Any, key: str) -> Any:
    if not isinstance(data_points, dict):
        return None

    point = data_points.get(key)
    if not isinstance(point, dict):
        return None

    return point.get("value")


def extract_and_clean_fields(payload: dict[str, Any]) -> dict[str, Any]:
    data_points = payload.get("data_points")

    name = empty_to_none(get_data_point_value(data_points, "name"))
    phone = validate_phone_or_none(get_data_point_value(data_points, "phone"))
    preferred_location = empty_to_none(get_data_point_value(data_points, "preferred_location"))
    visit_day = empty_to_none(get_data_point_value(data_points, "visit_day"))
    visit_time = normalize_time_to_24h(get_data_point_value(data_points, "visit_time"))
    intent = normalize_intent(get_data_point_value(data_points, "intent"))

    transcript = payload.get("transcript")
    raw_transcript = transcript if isinstance(transcript, str) else ""

    return {
        "name": name,
        "phone": phone,
        "preferred_location": preferred_location,
        "visit_day": visit_day,
        "visit_time": visit_time,
        "intent": intent,
        "timestamp": now_iso(),
        "raw_transcript": raw_transcript,
    }


def load_json_array(file_path: Path) -> list[dict[str, Any]]:
    if not file_path.exists():
        return []

    try:
        content = json.loads(file_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return []

    if isinstance(content, list):
        return [item for item in content if isinstance(item, dict)]

    return []


def safe_append_json_record(file_path: Path, record: dict[str, Any]) -> None:
    with write_lock:
        existing = load_json_array(file_path)
        existing.append(record)

        target_dir = file_path.parent if str(file_path.parent) not in {"", "."} else Path(".")
        target_dir.mkdir(parents=True, exist_ok=True)

        with tempfile.NamedTemporaryFile(
            mode="w",
            delete=False,
            dir=target_dir,
            suffix=".tmp",
            encoding="utf-8",
        ) as temp_file:
            json.dump(existing, temp_file, indent=2)
            temp_file.write("\n")
            temp_path = Path(temp_file.name)

        os.replace(temp_path, file_path)
