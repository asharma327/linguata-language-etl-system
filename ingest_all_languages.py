"""
ingest_all_languages.py

Walks a root directory with the structure:
    <ROOT>/
        Italian/
            Unit 1 JSON/
                *.json
            Unit 2 JSON/
                *.json
        German/
            Unit 1 JSON/
                ...

For each unit subfolder, calls POST /insert-lessons with the subfolder path
and the DB credentials for that language (one DB per language).

Usage:
    python ingest_all_languages.py

Configure the constants below before running.
"""

import os
import json
import requests
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Configuration — edit these before running
# ---------------------------------------------------------------------------

API_BASE = "http://localhost:8010"

# Root folder that contains one subfolder per language
ROOT = "/Users/adhaar/Desktop/client_documents/LearnX/extracted-json/kavs-extraction/Extracted-JSON"

# DB credentials shared across all languages (host/user/password are the same;
# the database name is derived from the language folder name unless overridden below).
DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_USER = os.environ.get("DB_USER", "root")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "")

# Optional: explicitly map language folder names → database names.
# If a language is not listed here, its folder name (lowercased) is used as the DB name.
DB_NAME_OVERRIDES: dict[str, str] = {
    # "Italian": "italian_prod",
    # "German":  "german_prod",
}

# CEFR mapping applied to every language. Unit number is extracted from the
# JSON filename (e.g. unit14_grammar.json → unit 14).
# Set to None to skip CEFR mapping and fall back to CEFR_FALLBACK for everything.
CEFR_MAPPING = [
    {"min": 1,  "max": 5,  "cefr_level": "A1"},
    {"min": 6,  "max": 10,  "cefr_level": "A2"},
    {"min": 11,  "max": 15, "cefr_level": "B1"},
    {"min": 16, "max": 20, "cefr_level": "B2"},
    {"min": 21, "max": 25, "cefr_level": "C1"},
    {"min": 26, "max": 30, "cefr_level": "C2"},
]

# Fallback CEFR level when no mapping matches (or CEFR_MAPPING is None)
CEFR_FALLBACK = "A1"

# ---------------------------------------------------------------------------
# Script
# ---------------------------------------------------------------------------

def db_name_for(language_folder: str) -> str:
    return DB_NAME_OVERRIDES.get(language_folder, language_folder.lower())


def insert_folder(language: str, unit_folder: Path) -> dict:
    """Call /insert-lessons for a single unit subfolder. Returns the response dict."""
    payload = {
        "db": {
            "host": DB_HOST,
            "user": DB_USER,
            "password": DB_PASSWORD,
            "database": db_name_for(language),
        },
        "folder": str(unit_folder),
        "cefr_level": CEFR_FALLBACK,
    }
    if CEFR_MAPPING:
        payload["cefr_mapping"] = CEFR_MAPPING

    response = requests.post(f"{API_BASE}/insert-lessons", json=payload, timeout=120)
    response.raise_for_status()
    return response.json()


def main(languages):
    root = Path(ROOT)
    if not root.is_dir():
        raise SystemExit(f"ROOT folder not found: {root}")

    # Collect language folders (top-level subdirectories)
    language_folders = sorted(p for p in root.iterdir() if p.is_dir() and p.name in languages)
    print(language_folders)
    # language_folders = languages
    if not language_folders:
        raise SystemExit(f"No language folders found under {root}")

    grand_total = {"files_found": 0, "files_succeeded": 0, "files_failed": 0,
                   "lessons_inserted": 0, "articles_inserted": 0,
                   "questions_inserted": 0, "answers_inserted": 0}

    for lang_path in language_folders:
        language = lang_path.name
        print(f"\n{'='*60}")
        print(f"Language: {language}  →  DB: {db_name_for(language)}")
        print(f"{'='*60}")

        # Collect unit subfolders, sorted so Unit 1 comes before Unit 2 etc.
        unit_folders = sorted(p for p in lang_path.iterdir() if p.is_dir())
        if not unit_folders:
            print(f"  [SKIP] No subfolders found under {lang_path}")
            continue

        for unit_path in unit_folders:
            # Skip folders that contain no JSON files
            json_files = list(unit_path.glob("*.json"))
            if not json_files:
                print(f"  [SKIP] {unit_path.name} — no JSON files")
                continue

            print(f"\n  Folder : {unit_path.name}  ({len(json_files)} JSON files)")

            try:
                result = insert_folder(language, unit_path)
            except requests.HTTPError as e:
                print(f"  [ERROR] HTTP {e.response.status_code}: {e.response.text[:200]}")
                continue
            except Exception as e:
                print(f"  [ERROR] {e}")
                continue

            summary = result.get("summary", {})
            status = result.get("status", "?")
            print(f"  Status : {status}")
            print(f"  Summary: {json.dumps(summary)}")

            # Accumulate totals
            for key in grand_total:
                grand_total[key] += summary.get(key, 0)

            # Print per-file failures if any
            for f in result.get("files", []):
                if f.get("status") == "failed":
                    print(f"  [FAIL]  {f['file']}: {f.get('errors')}")

    print(f"\n{'='*60}")
    print("Grand total across all languages and units:")
    print(json.dumps(grand_total, indent=2))


if __name__ == "__main__":
    languages = ['Italian']
    main(languages=languages)