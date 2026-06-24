import os
import json
import requests
from dotenv import load_dotenv

load_dotenv()

BASE = "http://language-media-gen-env.eba-jqm7dpsh.us-east-1.elasticbeanstalk.com"     # or your EB URL
DATABASE = "spanish"
DRY_RUN = True

UNITS = [1, 2]                  # unit numbers to process


def run():
    payload = {
        "db": {
            "host": os.environ["DB_HOST"],
            "user": os.environ["DB_USER"],
            "password": os.environ["DB_PASSWORD"],
            "database": DATABASE,
        },
        "units": UNITS,
        "dry_run": DRY_RUN,
    }

    with requests.post(f"{BASE}/link-listening-unit-images",
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
                print(f"START link-listening-unit-images | db={ev['database']} | "
                      f"units={ev['units']} | dry_run={ev['dry_run']}")
            elif e == "unit_start":
                print(f"\n== unit {ev['unit']} | image_id={ev['image_id']} | "
                      f"listening lessons={ev['listening_lessons']}")
                print(f"   source url: {ev['image_url']}")
            elif e == "lesson_done":
                print(f"   - {ev['title']} (lesson {ev['lesson_id']}): "
                      f"{ev['questions']} q -> {ev['updated']} updated, {ev['inserted']} inserted")
            elif e == "unit_skipped":
                print(f"   ! unit {ev['unit']} SKIPPED: {ev['reason']}")
            elif e == "unit_done":
                print(f"   = unit {ev['unit']} done: {ev.get('lessons')} lessons, "
                      f"{ev.get('questions')} questions, {ev.get('updated')} updated, "
                      f"{ev.get('inserted')} inserted"
                      + (f" ({ev['note']})" if ev.get('note') else ""))
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