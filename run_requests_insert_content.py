import os
import json
import requests
from dotenv import load_dotenv

load_dotenv()

BASE = "http://language-media-gen-env.eba-jqm7dpsh.us-east-1.elasticbeanstalk.com"
DATABASE = "italian"
DRY_RUN = False    # True = preview + rollback; False = actually insert

# Target the lesson by title (preferred) or lesson_id.
TITLE = "unit11_model8"
LESSON_ID = None

# Put ONLY the new items here — not the whole lesson.
# Vocabulary / writing / grammar question = single answer:
QUESTIONS_AND_ANSWERS = [
    {
        "question_text": "Che giorno era ieri?",
        "answers": [{"answer_text": "Ieri era domenica.", "is_correct": True}],
    },
    {
        "question_text": "Che giorno sarà domani?",
        "answers": [{"answer_text": "Domani sarà martedì.", "is_correct": True}],
    }
]

# Reading / listening question = four options with one correct:
# QUESTIONS_AND_ANSWERS = [
#     {
#         "question_text": "How big is Ana's family?",
#         "answers": [
#             {"answer_text": "Small", "is_correct": False},
#             {"answer_text": "Big", "is_correct": True},
#             {"answer_text": "Medium", "is_correct": False},
#             {"answer_text": "None", "is_correct": False},
#         ],
#     },
# ]

# Article (reading / grammar / speaking):
ARTICLES = None
# ARTICLES = [{"text": "SRA. ALLEN: ¿Cómo está usted?"}]


def run():
    payload = {
        "db": {
            "host": os.environ["DB_HOST"],
            "user": os.environ["DB_USER"],
            "password": os.environ["DB_PASSWORD"],
            "database": DATABASE,
        },
        "dry_run": DRY_RUN,
    }
    if LESSON_ID:
        payload["lesson_id"] = LESSON_ID
    elif TITLE:
        payload["title"] = TITLE
    if QUESTIONS_AND_ANSWERS:
        payload["questions_and_answers"] = QUESTIONS_AND_ANSWERS
    if ARTICLES:
        payload["articles"] = ARTICLES

    with requests.post(f"{BASE}/insert-content", json=payload, stream=True) as resp:
        for raw in resp.iter_lines(decode_unicode=True):
            if not raw:
                continue
            try:
                ev = json.loads(raw)
            except json.JSONDecodeError:
                print(raw); continue

            e = ev.get("event")
            if e == "start":
                print(f"START insert-content | dry_run={ev.get('dry_run')}")
            elif e == "resolved":
                print(f"  target: [{ev['lesson_id']}] {ev['type']} {ev['title']}")
            elif e == "article_inserted":
                print(f"  + article {ev['article_id']} (seq {ev['sequence_id']})")
            elif e == "question_inserted":
                print(f"  + question {ev['question_id']} (seq {ev['sequence_id']}) "
                      f"{ev['type']} answers={ev['answers']} cat={ev['answer_category']}")
            elif e == "commit":
                print("COMMIT")
            elif e == "rollback":
                print(f"ROLLBACK ({ev.get('reason')})")
            elif e == "summary":
                print("=" * 50)
                print(f"SUMMARY | committed={ev['committed']} | "
                      f"+{ev['questions_inserted']}q +{ev['articles_inserted']}a")
                if ev.get("new_question_sequence_ids"):
                    print(f"  new question sequence_ids: {ev['new_question_sequence_ids']}")
                if ev.get("next_step"):
                    print(f"  NEXT: {ev['next_step']}")
                print("=" * 50)
            elif e == "error":
                print(f"ERROR: {ev.get('message')}")
            else:
                print(raw)


if __name__ == "__main__":
    run()