import os
import json
import requests
from dotenv import load_dotenv

load_dotenv()

BASE = "http://127.0.0.1:8000"
DATABASE = "spanish"
DRY_RUN = False

# Folder of re-extracted basic_sentences JSON (same shape as insert-lessons),
# or set FILES to specific paths.
FOLDER = r"D:\DATA\tmp\try"                                             # ex:- FOLDER = r"D:\DATA\spanish\reextracted_basic_sentences"
FILES = None                                                  # ex:- FILES = [r"D:\DATA\tmp\try\unit1_basic_sentences.json"]

MODEL = "gpt-4o-mini"
KEEP_USER_HISTORY = False


def run():
    payload = {
        "db": {
            "host": os.environ["DB_HOST"],
            "user": os.environ["DB_USER"],
            "password": os.environ["DB_PASSWORD"],
            "database": DATABASE,
        },
        "model": MODEL,
        "keep_user_history": KEEP_USER_HISTORY,
        "dry_run": DRY_RUN,
        # openai_api_key falls back to OPENAI_API_KEY env var on the server
    }
    if FILES:
        payload["files"] = FILES
    else:
        payload["folder"] = FOLDER

    with requests.post(f"{BASE}/sync-vocab-lesson", json=payload, stream=True) as resp:
        for raw in resp.iter_lines(decode_unicode=True):
            if not raw:
                continue
            try:
                ev = json.loads(raw)
            except json.JSONDecodeError:
                print(raw); continue

            e = ev.get("event")
            if e == "start":
                print(f"START sync-vocab-lesson | files={ev['files']} | dry_run={ev['dry_run']}")
            elif e == "lesson_start":
                print(f"\n== {ev['title']} (lesson {ev['lesson_id']}) | "
                      f"db={ev['db_questions']} new={ev['new_questions']}")
            elif e == "llm_matched":
                print(f"   LLM: perfect={ev['perfect']} fuzzy={ev['fuzzy']} "
                      f"new={ev['new']} removed={ev['removed']}")
            elif e == "fuzzy":
                print(f"   ~ FUZZY q{ev['question_id']}: {ev['old_text']!r} -> {ev['new_text']!r}")
            elif e == "new":
                print(f"   + NEW (seq {ev['sequence_id']}): {ev['question_text']!r}")
            elif e == "removed":
                print(f"   - REMOVED q{ev['question_id']}: {ev['text']!r}")
            elif e == "duplicate_ignored":
                print(f"   ! duplicate match ignored ({ev['db']}<->{ev['new']}) -> treated as unmatched")
            elif e == "lesson_summary":
                print(f"   = {ev['title']}: {ev['perfect']} perfect, {ev['fuzzy']} fuzzy, "
                      f"{ev['new']} new, {ev['removed']} removed")
                if ev.get("regenerate_sequence_ids"):
                    print(f"     regenerate seq_ids: {ev['regenerate_sequence_ids']}")
            elif e in ("file_error", "lesson_error"):
                print(f"   ERROR {ev.get('file') or ev.get('title')}: {ev['error']}")
            elif e == "summary":
                print("\n" + "=" * 55)
                print(f"SUMMARY | dry_run={ev['dry_run']} | {ev['totals']}")
                print("\nNEXT — run /regenerate-lesson-media per lesson:")
                for item in ev.get("regenerate_plan", []):
                    print(f"  title={item['title']!r}  question_sequence_ids={item['question_sequence_ids']}")
                print("=" * 55)
            elif e == "error":
                print(f"ERROR: {ev.get('message')}")
            else:
                print(raw)


if __name__ == "__main__":
    run()