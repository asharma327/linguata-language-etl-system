import os
import requests
import json
from dotenv import load_dotenv

load_dotenv()

if __name__ == "__main__":
    DB = {
        "host": os.environ["DB_HOST"],
        "user": os.environ["DB_USER"],
        "password": os.environ["DB_PASSWORD"],
        "database": "spanish"
    }

    # --- Configure below ---

    S3_OUTPUT_PREFIX = "spanish/reading_questions"  # S3 prefix where generated question JSONs are written

    LESSON_TYPE   = "reading"    # generate for all lessons of this type
    LESSON_IDS    = None         # or specify e.g. [201, 202] to target specific lessons

    NUM_QUESTIONS = 10
    LIMIT         = None         # max number of lessons to process (None = all)
    FORCE         = False        # re-process even if questions already exist
    MODEL         = "gpt-4o-mini"
    TEMPERATURE   = 0.4

    OPENAI_API_KEY        = os.environ["OPENAI_API_KEY"]
    AWS_ACCESS_KEY_ID     = os.environ["AWS_ACCESS_KEY_ID"]
    AWS_SECRET_ACCESS_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]
    S3_BUCKET             = "content-media-generation"
    AWS_REGION            = "us-east-1"

    # ----------------------

    payload = {
        "db": DB,
        "s3_bucket": S3_BUCKET,
        "s3_output_prefix": S3_OUTPUT_PREFIX,
        "aws_region": AWS_REGION,
        "openai_api_key": OPENAI_API_KEY,
        "aws_access_key_id": AWS_ACCESS_KEY_ID,
        "aws_secret_access_key": AWS_SECRET_ACCESS_KEY,
        "num_questions": NUM_QUESTIONS,
        "model": MODEL,
        "temperature": TEMPERATURE,
        "force": FORCE,
    }

    if LIMIT is not None:
        payload["limit"] = LIMIT
    if LESSON_TYPE:
        payload["lesson_type"] = LESSON_TYPE
    if LESSON_IDS:
        payload["lesson_ids"] = LESSON_IDS

    response = requests.post("http://localhost:8020/generate-article-questions", json=payload)
    print(json.dumps(response.json(), indent=2))