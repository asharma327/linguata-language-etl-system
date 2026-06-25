import os
import json
import requests
from dotenv import load_dotenv

load_dotenv()

BASE = "http://localhost:8000"     # or your EB URL
DATABASE = "german"
DRY_RUN = True

# Each lesson: target by lesson_id OR title, plus its image_url.
LESSONS = [
    {"title": "unit2_situation_1", "image_url": "https://content-media-generation.s3.us-east-1.amazonaws.com/german/unit_images/unit2_greetings_travel_conversation.png"},
    {"title": "unit2_situation_2", "image_url": "https://content-media-generation.s3.us-east-1.amazonaws.com/german/unit_images/unit2_greetings_travel_conversation.png"},
    # {"lesson_id": 4123, "image_url": "https://..."},
]


def run():
    payload = {
        "db": {
            "host": os.environ["DB_HOST"],
            "user": os.environ["DB_USER"],
            "password": os.environ["DB_PASSWORD"],
            "database": DATABASE,
        },
        "lessons": LESSONS,
        "dry_run": DRY_RUN,
    }

    with requests.post(f"{BASE}/link-reading-lesson-images",
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
                print(f"START link-reading-lesson-images | db={ev['database']} | "
                      f"lessons={ev['lessons']} | dry_run={ev['dry_run']}")
            elif e == "lesson_plan":
                print(f"  plan: [{ev['lesson_id']}] {ev['title']} | first q={ev['first_question_id']} "
                      f"(seq {ev['sequence_id']})")
                print(f"        url: {ev['image_url']}")
            elif e == "lesson_created":
                print(f"  CREATED [{ev['lesson_id']}] {ev['title']} -> image_id={ev['image_id']} "
                      f"(q{ev['question_id']})")
            elif e == "lesson_skipped":
                print(f"  SKIPPED lesson_id={ev.get('lesson_id')} title={ev.get('title')} :: {ev['reason']}")
            elif e == "lesson_error":
                print(f"  ERROR lesson_id={ev.get('lesson_id')} title={ev.get('title')} :: {ev['error']}")
            elif e == "summary":
                print("=" * 55)
                print(f"SUMMARY | dry_run={ev['dry_run']} | {ev['totals']}")
                print("=" * 55)
            elif e == "error":
                print(f"ERROR: {ev.get('message')}")
            else:
                print(raw)


if __name__ == "__main__":
    run()