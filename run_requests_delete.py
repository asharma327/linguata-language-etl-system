import os
import json
import requests
from dotenv import load_dotenv

load_dotenv()

BASE = "http://language-media-gen-env.eba-jqm7dpsh.us-east-1.elasticbeanstalk.com"

# --- choose ONE action ---
ACTION = "delete-lessons"          # "delete-questions" or "delete-lessons"

DATABASE = "german"
DRY_RUN = True                    # True = preview + rollback; False = actually delete
KEEP_USER_HISTORY = False          # True = don't delete userResponses / user_attempts rows

TITLES = [
    "unit10_dialog_1",
    "unit10_dialog_2",
    "unit10_dialog_3",
    "unit11_dialog_1",
    "unit11_dialog_2",
    "unit11_dialog_3",
    "unit11_dialog_4",
    "unit12_dialog_1",
    "unit12_dialog_2",
    "unit12_dialog_3",
    "unit13_dialog_1",
    "unit13_dialog_2",
    "unit13_dialog_3",
    "unit14_dialog_1",
    "unit14_dialog_2",
    "unit14_dialog_3",
    "unit15_dialog_1",
    "unit15_dialog_2",
    "unit15_dialog_3",
    "unit16_dialog_1",
    "unit16_dialog_2",
    "unit16_dialog_3",
    "unit17_dialog_1",
    "unit17_dialog_2",
    "unit17_dialog_3",
    "unit18_dialog_1",
    "unit18_dialog_2",
    "unit18_dialog_3",
    "unit19_dialog_1",
    "unit19_dialog_2",
    "unit19_dialog_3",
    "unit20_dialog_1",
    "unit20_dialog_2",
    "unit20_dialog_3",
    "unit21_dialog_1",
    "unit21_dialog_2",
    "unit21_dialog_3",
    "unit22_dialog_1",
    "unit22_dialog_2",
    "unit22_dialog_3",
    "unit23_dialog_1",
    "unit23_dialog_2",
    "unit23_dialog_3",
    "unit24_dialog_1",
    "unit24_dialog_2",
    "unit24_dialog_3",
    "unit25_dialog_1",
    "unit25_dialog_2",
    "unit25_dialog_3",
    "unit26_dialog_1",
    "unit26_dialog_2",
    "unit26_dialog_3",
    "unit27_dialog_1",
    "unit27_dialog_2",
    "unit27_dialog_3",
    "unit28_dialog_1",
    "unit28_dialog_2",
    "unit28_dialog_3",
    "unit29_dialog_1",
    "unit29_dialog_2",
    "unit29_dialog_3",
    "unit3_dialog_1",
    "unit3_dialog_2",
    "unit3_dialog_3",
    "unit30_dialog_1",
    "unit30_dialog_2",
    "unit30_dialog_3",
    "unit4_dialog_1",
    "unit4_dialog_2",
    "unit5_dialog_1",
    "unit5_dialog_2",
    "unit5_dialog_3",
    "unit6_dialog_1",
    "unit6_dialog_2",
    "unit6_dialog_3",
    "unit7_dialog_1",
    "unit7_dialog_2",
    "unit7_dialog_3",
    "unit8_dialog_1",
    "unit8_dialog_2",
    "unit8_dialog_3",
    "unit8_dialog_4",
    "unit9_dialog_1",
    "unit9_dialog_2",
    "unit9_dialog_3",
    "unit9_dialog_4"
]
LESSON_IDS = None          # used when ACTION == "delete-lessons"  

QUESTION_IDS = None        # used when ACTION == "delete-questions"


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