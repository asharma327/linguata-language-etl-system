import requests
import json
import os
from dotenv import load_dotenv

load_dotenv()

if __name__ == "__main__":
    DB = {
        "host": os.environ["DB_HOST"],
        "user": os.environ["DB_USER"],
        "password": os.environ["DB_PASSWORD"],
        "database": "italian"
    }

    # --- Configure below ---

    UNITS = [21, 22, 23, 24, 25, 26, 27, 28, 29, 30]         # e.g. [1, 2, 3]  — auto-selects all grammar articles without audio

    LESSONS = [
        # {"lesson_id": 3251},
    ]

    LIMIT_ARTICLES    = 10    # max articles to process when using units

    OPENAI_API_KEY        = os.environ["OPENAI_API_KEY"]
    AWS_ACCESS_KEY_ID     = os.environ["AWS_ACCESS_KEY_ID"]
    AWS_SECRET_ACCESS_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]
    S3_BUCKET             = "content-media-generation"
    S3_PREFIX             = "italian/grammar_audio"
    AWS_REGION            = "us-east-1"

    # ----------------------

    payload = {
        "db": DB,
        "s3_bucket": S3_BUCKET,
        "s3_prefix": S3_PREFIX,
        "aws_region": AWS_REGION,
        "openai_api_key": OPENAI_API_KEY,
        "aws_access_key_id": AWS_ACCESS_KEY_ID,
        "aws_secret_access_key": AWS_SECRET_ACCESS_KEY,
        "limit_articles": LIMIT_ARTICLES,
    }

    if UNITS:
        payload["units"] = UNITS
    if LESSONS:
        payload["lessons"] = LESSONS

    response = requests.post("http://localhost:8010/generate-grammar-audio", json=payload)
    print(json.dumps(response.json(), indent=2))