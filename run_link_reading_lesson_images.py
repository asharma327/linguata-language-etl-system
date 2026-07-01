import os
import json
import requests
from dotenv import load_dotenv

load_dotenv()

BASE = "http://language-media-gen-env.eba-jqm7dpsh.us-east-1.elasticbeanstalk.com"     # or your EB URL
DATABASE = "spanish"
DRY_RUN = False  # True = don't actually update the DB, just show what would happen

# Each lesson: target by lesson_id OR title, plus its image_url.
LESSONS = [
    {
        "title": "unit1_narratives",
        "image_url": "https://content-media-generation.s3.amazonaws.com/spanish/images/3300_40069_unit1_basic_sentences.png",
    },
    {
        "title": "unit2_narratives",
        "image_url": "https://content-media-generation.s3.amazonaws.com/spanish/images/3348_40467_unit2_basic_sentences.png",
    },
    {
        "title": "unit3_narratives",
        "image_url": "https://content-media-generation.s3.us-east-1.amazonaws.com/spanish/unit_images/unit3_travel_embassy_exchange.png",
    },
    {
        "title": "unit4_narratives",
        "image_url": "https://content-media-generation.s3.us-east-1.amazonaws.com/spanish/unit_images/unit4_money_introductions_location.png",
    },
    {
        "title": "unit5_narratives",
        "image_url": "https://content-media-generation.s3.us-east-1.amazonaws.com/spanish/unit_images/unit5_lunch_introductions_verbs.png",
    },
    {
        "title": "unit6_narratives",
        "image_url": "https://content-media-generation.s3.us-east-1.amazonaws.com/spanish/unit_images/unit6_ordering_food_verbs.png",
    },
    {
        "title": "unit7_narratives",
        "image_url": "https://content-media-generation.s3.us-east-1.amazonaws.com/spanish/unit_images/unit7_apartments_renting_housing.png",
    },
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