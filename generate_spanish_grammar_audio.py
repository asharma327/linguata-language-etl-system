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
        "database": "spanish"
    }

    # --- Configure below ---

    UNITS = None         # e.g. [1, 2, 3]  — auto-selects all grammar articles without audio

    LESSONS = [
        # {"lesson_id": 3251},
        {"lesson_id": 3253},
        {"lesson_id": 3254},
        {"lesson_id": 3258},
        {"lesson_id": 3259},
        {"lesson_id": 3263},
        {"lesson_id": 3264},
        {"lesson_id": 3265},
        {"lesson_id": 3269},
        {"lesson_id": 3270},
        {"lesson_id": 3273},
        {"lesson_id": 3275},
        {"lesson_id": 3278},
        {"lesson_id": 3279},
        {"lesson_id": 3281},
        {"lesson_id": 3285},
        {"lesson_id": 3287},
        {"lesson_id": 3290},
        {"lesson_id": 3293},
        {"lesson_id": 3294},
        {"lesson_id": 3299},
        {"lesson_id": 3301},
        {"lesson_id": 3302},
        {"lesson_id": 3303},
        {"lesson_id": 3306},
        {"lesson_id": 3310},
        {"lesson_id": 3315},
        {"lesson_id": 3319},
        {"lesson_id": 3324},
        {"lesson_id": 3325},
        {"lesson_id": 3329},
        {"lesson_id": 3330},
        {"lesson_id": 3333},
        {"lesson_id": 3337},
        {"lesson_id": 3341},
        {"lesson_id": 3344},
        {"lesson_id": 3345},
        {"lesson_id": 3349},
        {"lesson_id": 3351},
        {"lesson_id": 3353},
        {"lesson_id": 3356},
        {"lesson_id": 3359},
        {"lesson_id": 3360},
        {"lesson_id": 3364},
        {"lesson_id": 3365},
        {"lesson_id": 3367},
        {"lesson_id": 3370},
        {"lesson_id": 3371},
        {"lesson_id": 3372},
        {"lesson_id": 3376},
        {"lesson_id": 3377},
        {"lesson_id": 3378},
        {"lesson_id": 3382},
        {"lesson_id": 3383},
        {"lesson_id": 3384},
        {"lesson_id": 3386},
        {"lesson_id": 3390},
        {"lesson_id": 3391},
        {"lesson_id": 3395},
        {"lesson_id": 3396}
    ]

    LIMIT_ARTICLES    = 10    # max articles to process when using units

    OPENAI_API_KEY        = os.environ["OPENAI_API_KEY"]
    AWS_ACCESS_KEY_ID     = os.environ["AWS_ACCESS_KEY_ID"]
    AWS_SECRET_ACCESS_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]
    S3_BUCKET             = "content-media-generation"
    S3_PREFIX             = "spanish/grammar_audio"
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