import os
import json
import requests
from dotenv import load_dotenv

load_dotenv()

BASE = "http://language-media-gen-env.eba-jqm7dpsh.us-east-1.elasticbeanstalk.com"
DATABASE = "german"
DRY_RUN = True

# One or many vocabulary lessons.
TITLES = [
    "unit10_vocabulary_drill",
    # "unit11_vocabulary_drill",
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
        "dry_run": DRY_RUN,
    }

    with requests.post(f"{BASE}/swap-vocab-question-answer",
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
                print(f"START swap-vocab-question-answer | titles={ev['titles']} | dry_run={ev['dry_run']}")
            elif e == "lesson_start":
                print(f"\n== {ev['title']} (lesson {ev['lesson_id']}) | {ev['questions']} questions")
            elif e == "swap":
                print(f"  q{ev['question_id']} (seq {ev['sequence_id']}):")
                print(f"      Q: {ev['old_question']!r} -> {ev['new_question']!r}")
                print(f"      A: {ev['old_answer']!r} -> {ev['new_answer']!r}")
            elif e == "skipped":
                print(f"  q{ev['question_id']} (seq {ev['sequence_id']}) SKIPPED: {ev['reason']}")
            elif e == "lesson_skipped":
                print(f"  LESSON SKIPPED {ev['title']}: {ev['reason']}")
            elif e == "lesson_done":
                print(f"  = {ev['title']}: {ev['totals']}")
            elif e == "summary":
                print("\n" + "=" * 55)
                print(f"SUMMARY | dry_run={ev['dry_run']} | {ev['totals']}")
                print("=" * 55)
            elif e == "error":
                print(f"ERROR: {ev.get('message')}")
            else:
                print(raw)


if __name__ == "__main__":
    run()