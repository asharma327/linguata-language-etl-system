import os
import requests
import json
from dotenv import load_dotenv

load_dotenv()

URL = "http://language-media-gen-env.eba-jqm7dpsh.us-east-1.elasticbeanstalk.com/rebalance-grammar-categories"

# Set to True for the FIRST run (verifies + rolls back, changes nothing).
# Flip to False only after the dry-run output looks correct.
DRY_RUN = False

# Restrict to specific grammar lessons for a staged test, e.g. [101, 102, 103].
# Set to None to sweep ALL grammar lessons in one transaction.
LESSON_IDS = [
    4061, 4062, 4063, 4064, 4065, 4066, 4086, 4087, 4088, 4091,
    4110, 4111, 4112, 4113, 4114, 4115, 4116, 4144, 4145, 4146,
    4147, 4148, 4149, 4150, 4167, 4168, 4169, 4170, 4171, 4172,
    4187, 4188, 4189, 4190, 4191, 4192, 4211, 4212, 4236, 4237,
    4238, 4261, 4262, 4263, 4284, 4285, 4286, 4301, 4302, 4303,
    4304, 4305, 4306, 4307, 4327, 4328, 4329, 4330, 4331, 4332,
    4333, 4354, 4355, 4356, 4357, 4358, 4380, 4381, 4382, 4383,
    4397, 4398, 4399, 4400, 4417, 4435, 4436, 4437, 4438, 4439,
    4440, 4441, 4466, 4467, 4468, 4469, 4491, 4492, 4493, 4494,
    4495, 4513, 4514, 4515, 4516, 4517, 4518, 4538, 4539, 4540,
    4558, 4559, 4580, 4581, 4582, 4583, 4601, 4602, 4603, 4617,
    4618, 4643, 4644, 4645, 4661, 4662, 4679, 4680, 4702, 4703,
    4704, 4705, 4706, 4724, 4725, 4726, 4727, 4728
]


if __name__ == "__main__":
    payload = {
        "db": {
            "host": os.environ["DB_HOST"],
            "user": os.environ["DB_USER"],
            "password": os.environ["DB_PASSWORD"],
            "database": "japanese",
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