"""
ingest_lesson.py

Inserts lessons from a single JSON file or an entire folder into a specific database.

Usage:
    python ingest_lesson.py

Configure DB, PATH, and DATABASE below before running.
"""

import os
import json
import requests
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Configuration — edit these before running
# ---------------------------------------------------------------------------

API_BASE = "http://localhost:8010"

# Path to a single .json file or a folder containing .json files
PATH = "/Users/adhaar/Desktop/client_documents/LearnX/extracted-json/kavs-extraction/Extracted-JSON/Italian/Unit 21 JSON/unit21_intro.json"

# Target database name
DATABASE = "italian"

DB_HOST     = os.environ.get("DB_HOST", "localhost")
DB_USER     = os.environ.get("DB_USER", "root")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "")

CEFR_MAPPING = [
    {"min": 1,  "max": 5,  "cefr_level": "A1"},
    {"min": 6,  "max": 10,  "cefr_level": "A2"},
    {"min": 11,  "max": 15, "cefr_level": "B1"},
    {"min": 16, "max": 20, "cefr_level": "B2"},
    {"min": 21, "max": 25, "cefr_level": "C1"},
    {"min": 26, "max": 30, "cefr_level": "C2"},
]

CEFR_FALLBACK = "A1"

# ---------------------------------------------------------------------------
# Script
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    payload = {
        "db": {
            "host": DB_HOST,
            "user": DB_USER,
            "password": DB_PASSWORD,
            "database": DATABASE,
        },
        "cefr_level": CEFR_FALLBACK,
        "cefr_mapping": CEFR_MAPPING,
    }

    import os as _os
    if _os.path.isdir(PATH):
        payload["folder"] = PATH
    else:
        payload["files"] = [PATH]

    response = requests.post(f"{API_BASE}/insert-lessons", json=payload, timeout=120)
    response.raise_for_status()
    print(json.dumps(response.json(), indent=2))