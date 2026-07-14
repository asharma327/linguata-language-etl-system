import os
import json
import requests
from dotenv import load_dotenv

load_dotenv()

BASE = "http://language-media-gen-env.eba-jqm7dpsh.us-east-1.elasticbeanstalk.com"

# --- choose ONE action ---
ACTION = "delete-lessons"          # "delete-questions" or "delete-lessons"

DATABASE = "spanish"
DRY_RUN = True                     # True = preview + rollback; False = actually delete
KEEP_USER_HISTORY = False          # True = don't delete userResponses / user_attempts rows

TITLES = []
LESSON_IDS = []         # used when ACTION == "delete-lessons"  

QUESTION_IDS = []        # used when ACTION == "delete-questions"


def run():
    url = f"{BASE}/{ACTION}"
    payload = {
        "db": {
            "host": os.environ["DB_HOST"],
            "user": os.environ["DB_USER"],
            "password": os.environ["DB_PASSWORD"],
            "database": DATABASE,
        },
        "dry_run": DRY_RUN,
        "keep_user_history": KEEP_USER_HISTORY,
    }
    if ACTION == "delete-lessons":
        payload["lesson_ids"] = LESSON_IDS
        payload["titles"] = TITLES
    else:
        payload["question_ids"] = QUESTION_IDS

    # stream=True + iter_lines = print each progress line the moment it arrives
    with requests.post(url, json=payload, stream=True) as resp:
        for raw in resp.iter_lines(decode_unicode=True):
            if not raw:
                continue
            try:
                ev = json.loads(raw)
            except json.JSONDecodeError:
                print(raw)
                continue

            e = ev.get("event")
            if e == "start":
                print(f"START {ev.get('action')} | targets={ev.get('count')} | dry_run={ev.get('dry_run')}")
            elif e == "preview":
                print(f"  preview: existing={ev.get('existing')} missing={ev.get('missing')}")
            elif e == "lesson_start":
                print(f"  -- lesson {ev['lesson_id']} ({ev.get('type')}) '{ev.get('title')}' | questions={ev.get('questions')}")
            elif e == "step":
                lid = f"[lesson {ev['lesson_id']}] " if "lesson_id" in ev else ""
                print(f"    {lid}{ev['table']}: deleted {ev['deleted']}")
            elif e == "lesson_done":
                print(f"  -- lesson {ev['lesson_id']} done: {ev['deleted']}")
            elif e == "lesson_skipped":
                print(f"  -- lesson {ev['lesson_id']} SKIPPED ({ev.get('reason')})")
            elif e == "commit":
                print("COMMIT")
            elif e == "rollback":
                print(f"ROLLBACK ({ev.get('reason')})")
            elif e == "summary":
                print("=" * 50)
                print(f"SUMMARY | committed={ev.get('committed')} dry_run={ev.get('dry_run')}")
                totals = ev.get("grand_totals") or ev.get("deleted") or {}
                for k, v in totals.items():
                    print(f"  {k}: {v}")
                print("=" * 50)
            elif e == "error":
                print(f"ERROR: {ev.get('message')} | rolled_back={ev.get('rolled_back')}")
            else:
                print(raw)


if __name__ == "__main__":
    run()