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
        "database": "hindi"
    }

    # --- Configure below ---

    UNITS = None         # e.g. [1, 2, 3]  — auto-selects all grammar articles without audio
    LESSONS = [
        # {"lesson_id": 3251},
        # {"lesson_id": 3254},
        # {"lesson_id": 3257},
        # {"lesson_id": 3259},
        # {"lesson_id": 3262},
        {"lesson_id": 3266},
        {"lesson_id": 3269},
        {"lesson_id": 3271},
        {"lesson_id": 3275},
        {"lesson_id": 3277},
        {"lesson_id": 3281},
        {"lesson_id": 3284},
        {"lesson_id": 3287},
        {"lesson_id": 3289},
        {"lesson_id": 3292},
        {"lesson_id": 3296},
        {"lesson_id": 3299},
        {"lesson_id": 3300},
        {"lesson_id": 3304},
        {"lesson_id": 3307},
        {"lesson_id": 3311},
        {"lesson_id": 3314},
        {"lesson_id": 3316},
        {"lesson_id": 3338},
        {"lesson_id": 3341},
        {"lesson_id": 3344},
        {"lesson_id": 3346},
        {"lesson_id": 3350},
        {"lesson_id": 3353},
        {"lesson_id": 3357},
        {"lesson_id": 3360},
        {"lesson_id": 3363},
        {"lesson_id": 3365},
        {"lesson_id": 3369},
        {"lesson_id": 3372},
        {"lesson_id": 3375},
        {"lesson_id": 3377},
        {"lesson_id": 3381},
        {"lesson_id": 3384},
        {"lesson_id": 3387},
        {"lesson_id": 3390},
        {"lesson_id": 3393},
        {"lesson_id": 3396},
        {"lesson_id": 3399},
        {"lesson_id": 3404},
        {"lesson_id": 3407},
        {"lesson_id": 3409},
        {"lesson_id": 3411},
        {"lesson_id": 3413},
        {"lesson_id": 3415},
        {"lesson_id": 3417},
    ]

    LIMIT_ARTICLES    = 10    # max articles to process when using units

    OPENAI_API_KEY        = os.environ["OPENAI_API_KEY"]
    AWS_ACCESS_KEY_ID     = os.environ["AWS_ACCESS_KEY_ID"]
    AWS_SECRET_ACCESS_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]
    S3_BUCKET             = "content-media-generation"
    S3_PREFIX             = "hindi/grammar_audio"
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

    response = requests.post("http://localhost:8000/generate-grammar-audio", json=payload)
    print(json.dumps(response.json(), indent=2))