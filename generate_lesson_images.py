import os
import requests
import json
from dotenv import load_dotenv

load_dotenv()

language = "spanish"  

if __name__ == "__main__":
    DB = {
        "host": os.environ["DB_HOST"],
        "user": os.environ["DB_USER"],
        "password": os.environ["DB_PASSWORD"],
        "database": language
    }

    # --- Configure below ---

    # Exact lesson titles (one or many). For a single lesson, use a one-item list.
    TITLES = [
        "unit2_basic_sentences",
        # "unit5_basic_sentences",
    ]

    # NOTE on UNITS: matching is a PREFIX match on title (unit{N}%), so UNITS=[2]
    # also matches unit20, unit21, ... If you need an exact unit, prefer TITLES.
    UNITS = [
        # 1, 22, 23, 24, 25, 26, 27, 28, 29, 30
    ]

    # Explicit lessons: [{"lesson_id": 123, "question_ids": [..]?, "additional_prompt": "..."?}]
    LESSONS = [
        # {"lesson_id": 3306},
    ]

    ADDITIONAL_PROMPT = None    # global prompt applied to all
    LIMIT_QUESTIONS   = None     # GLOBAL cap across whichever mode(s) you use.
                                # Set to 1 to generate only the FIRST question.
    TRANSLATE         = True    # question_text -> English before generating (recommended)

    OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]
    AWS_ACCESS_KEY_ID = os.environ["AWS_ACCESS_KEY_ID"]
    AWS_SECRET_ACCESS_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]
    S3_BUCKET = "content-media-generation"
    S3_PREFIX = f"{language}/images"
    AWS_REGION = "us-east-1"

    # ----------------------

    payload = {
        "db": DB,
        "s3_bucket": S3_BUCKET,
        "s3_prefix": S3_PREFIX,
        "aws_region": AWS_REGION,
        "openai_api_key": OPENAI_API_KEY,
        "aws_access_key_id": AWS_ACCESS_KEY_ID,
        "aws_secret_access_key": AWS_SECRET_ACCESS_KEY,
        "limit_questions": LIMIT_QUESTIONS,
        "translate": TRANSLATE,
    }
    if TITLES:
        payload["titles"] = TITLES
    if UNITS:
        payload["units"] = UNITS
    if LESSONS:
        payload["lessons"] = LESSONS
    if ADDITIONAL_PROMPT:
        payload["additional_prompt"] = ADDITIONAL_PROMPT

    with requests.post("http://language-media-gen-env.eba-jqm7dpsh.us-east-1.elasticbeanstalk.com/generate-lesson-images",
                       json=payload, stream=True) as resp:
        for raw in resp.iter_lines(decode_unicode=True):
            if not raw:
                continue
            try:
                ev = json.loads(raw)
            except json.JSONDecodeError:
                print(raw); continue

            e = ev.get("event")
            if e == "start":
                print(f"START generate-lesson-images | db={ev['database']} | translate={ev['translate']}")
                print(f"  titles={ev.get('titles')} units={ev.get('units')} lessons={ev.get('lessons')}")
            elif e == "found":
                print(f"  questions needing images: {ev['total']}")
            elif e == "processing":
                print(f"  [{ev['n']}/{ev['total']}] {ev['lesson_title']} q{ev['question_id']} "
                      f"(seq {ev['sequence_id']}): {ev['question_text']!r}")
            elif e == "success":
                print(f"       OK ({ev['row']}) -> {ev['image_url']}")
            elif e == "failed":
                print(f"       FAILED q{ev['question_id']}: {ev['error']}")
            elif e == "lesson_error":
                print(f"  LESSON ERROR {ev['lesson_id']}: {ev['error']}")
            elif e == "summary":
                print("=" * 50)
                print(f"SUMMARY | items={ev['total_items']} succeeded={ev['succeeded']} failed={ev['failed']}")
                print("=" * 50)
            elif e == "error":
                print(f"ERROR: {ev.get('message')}")
            else:
                print(raw)