import os
import json
import requests
from dotenv import load_dotenv

load_dotenv()

BASE = "http://localhost:8000"
DATABASE = "extractiondb"  
DRY_RUN = False

# Human-readable source language (helps the model translate single words correctly).
SOURCE_LANGUAGE = "french"
MODEL = "gpt-4o-mini"

TITLES = [
    "unit24_revision_exercise_54",
    "unit24_revision_exercise_56",
    "unit24_revision_exercise_62",
]


def run():
    payload = {
        "db": {
            "host": os.environ["DB_HOST"],
            "user": os.environ["DB_USER"],
            "password": os.environ["DB_PASSWORD"],
            "database": DATABASE,
        },
        "titles": TITLES,
        "source_language": SOURCE_LANGUAGE,
        "model": MODEL,
        "dry_run": DRY_RUN,
        # openai_api_key falls back to OPENAI_API_KEY env var on the server
    }

    with requests.post(f"{BASE}/fill-vocab-answers",
                       json=payload, stream=True, timeout=3600) as resp:
        for raw in resp.iter_lines(decode_unicode=True):
            if not raw:
                continue
            try:
                ev = json.loads(raw)
            except json.JSONDecodeError:
                print(raw); continue

            e = ev.get("event")
            if e == "start":
                print(f"START fill-vocab-answers | src={ev['source_language']} | "
                      f"model={ev['model']} | dry_run={ev['dry_run']}")
                print(f"  titles={ev['titles']}")
            elif e == "lesson_start":
                print(f"\n== {ev['title']} (lesson {ev['lesson_id']}) | {ev['questions']} questions")
            elif e == "fill":
                print(f"   q{ev['question_id']} (seq {ev['sequence_id']}) [{ev['action']}]: "
                      f"{ev['question_text']!r} -> {ev['translation']!r}")
            elif e == "skipped":
                print(f"   q{ev['question_id']} SKIPPED: {ev['reason']}")
            elif e == "translate_error":
                print(f"   q{ev['question_id']} TRANSLATE ERROR: {ev['error']}")
            elif e == "lesson_skipped":
                print(f"  LESSON SKIPPED {ev['title']}: {ev['reason']}")
            elif e == "lesson_done":
                print(f"   = {ev['title']}: filled {ev['filled']} "
                      f"(insert {ev['inserted']}, update {ev['updated']}), "
                      f"already had {ev['already_had_answer']} (of {ev['total']})")
            elif e == "summary":
                print("\n" + "=" * 55)
                print(f"SUMMARY | dry_run={ev['dry_run']} | {ev['totals']}")
                if ev.get("note"):
                    print(f"  NOTE: {ev['note']}")
                print("=" * 55)
            elif e == "error":
                print(f"ERROR: {ev.get('message')}")
            else:
                print(raw)


if __name__ == "__main__":
    run()