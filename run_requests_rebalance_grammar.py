import os
import requests
import json
from dotenv import load_dotenv

load_dotenv()

URL = "http://127.0.0.1:8000/rebalance-grammar-categories"

# Set to True for the FIRST run (verifies + rolls back, changes nothing).
# Flip to False only after the dry-run output looks correct.
DRY_RUN = False

# Restrict to specific grammar lessons for a staged test, e.g. [101, 102, 103].
# Set to None to sweep ALL grammar lessons in one transaction.
LESSON_IDS = None


if __name__ == "__main__":
    payload = {
        "db": {
            "host": os.environ["DB_HOST"],
            "user": os.environ["DB_USER"],
            "password": os.environ["DB_PASSWORD"],
            "database": "italian",
        },
        "dry_run": DRY_RUN,
        # "learning_ratio": 0.5,          # default; uncomment to override
        # "handle_orphan_practice": True, # default; Step A (the 30) → learning
    }
    if LESSON_IDS is not None:
        payload["lesson_ids"] = LESSON_IDS

    response = requests.post(URL, json=payload)
    data = response.json()
    print(json.dumps(data, indent=2))

    # --- Quick verdict on the run ---
    print("\n" + "=" * 60)
    print(f"HTTP {response.status_code} | dry_run={DRY_RUN} | committed={data.get('committed')}")

    v = data.get("verification", {})
    checks = {
        "questions_not_exactly_one_answer": v.get("questions_not_exactly_one_answer"),
        "remaining_multiple_choice_type": v.get("remaining_multiple_choice_type"),
        "remaining_multiple_choice_category": v.get("remaining_multiple_choice_category"),
        "questions_with_null_category": v.get("questions_with_null_category"),
    }
    print("Verification (all should be 0):")
    for k, val in checks.items():
        flag = "OK " if val == 0 else "!! "
        print(f"  {flag}{k}: {val}")

    print(f"Category totals: {v.get('overall_categories')}")
    print(f"Lopsided lessons (learning > practice): {v.get('lopsided_lessons')}")

    s = data.get("summary", {})
    print(f"Step A converted (expect 30 on full run): {s.get('step_a_converted')}")
    print(f"Grammar lessons in scope: {s.get('grammar_lessons_in_scope')}")
    print("=" * 60)

    if DRY_RUN:
        print("This was a DRY RUN — nothing was committed.")
        print("If the verification above is all 0 and totals look ~50/50,")
        print("set DRY_RUN = False and run again to commit.")
    elif data.get("committed"):
        print("COMMITTED. Spot-check in the DB to confirm.")
    else:
        print("NOT committed — verification failed and the transaction rolled back.")
        print("Inspect the 'lessons' array in the JSON above to see why.")