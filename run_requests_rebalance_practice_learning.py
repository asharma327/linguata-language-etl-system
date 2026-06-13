import os
import json
import requests
from dotenv import load_dotenv

load_dotenv()

BASE = "http://127.0.0.1:8000"
DATABASE = "spanish"
DRY_RUN = False

LESSON_IDS = None        # None = all grammar lessons; or a list to stage a subset
MODEL = "gpt-4o-mini"


def run():
    payload = {
        "db": {
            "host": os.environ["DB_HOST"],
            "user": os.environ["DB_USER"],
            "password": os.environ["DB_PASSWORD"],
            "database": DATABASE,
        },
        "model": MODEL,
        "dry_run": DRY_RUN,
        # openai_api_key falls back to OPENAI_API_KEY env var on the server
    }
    if LESSON_IDS is not None:
        payload["lesson_ids"] = LESSON_IDS

    with requests.post(f"{BASE}/rebalance-grammar-practice-learning", json=payload, stream=True) as resp:
        for raw in resp.iter_lines(decode_unicode=True):
            if not raw:
                continue
            try:
                ev = json.loads(raw)
            except json.JSONDecodeError:
                print(raw); continue

            e = ev.get("event")
            if e == "start":
                print(f"START rebalance practice/learning | dry_run={ev.get('dry_run')} ratio={ev.get('learning_ratio')}")
            elif e == "resolved":
                print(f"  grammar lessons in scope: {ev['lessons']}")
            elif e == "lesson_plan":
                print(f"  -- [{ev['lesson_id']}] {ev['title']} | total={ev['total']} "
                      f"(now {ev['current_learning']}L/{ev['current_practice']}P, target {ev['target_learning']}L) "
                      f"-> blank {ev['will_blank_to_learning']}, generate {ev['will_generate_to_practice']}")
            elif e == "blanked":
                print(f"       blanked q{ev['question_id']} -> learning")
            elif e == "generated":
                print(f"       generated q{ev['question_id']} -> practice | answer: {ev['answer']!r}")
            elif e == "generate_error":
                print(f"       ERROR q{ev['question_id']}: {ev['error']}")
            elif e == "backfill":
                print(f"  backfill updated {ev['rows_updated']} rows")
            elif e == "verification":
                print(f"  categories now: {ev['overall_categories']}")
            elif e == "summary":
                print("=" * 50)
                print(f"SUMMARY | dry_run={ev['dry_run']} | {ev['totals']}")
                gids = ev.get("generated_question_ids") or []
                if gids:
                    print(f"  AI-GENERATED ANSWERS — QA must verify these question_ids:")
                    print(f"  {gids}")
                print("=" * 50)
            elif e == "error":
                print(f"ERROR: {ev.get('message')}")
            else:
                print(raw)


if __name__ == "__main__":
    run()