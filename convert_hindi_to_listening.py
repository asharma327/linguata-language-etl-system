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

    LESSON_IDS = [3317, 3383, 3400, 3253, 3298, 3308, 3263, 3272, 3280, 3290, 3323, 3340, 3352, 3368, 3378]

    OPENAI_API_KEY        = os.environ["OPENAI_API_KEY"]
    AWS_ACCESS_KEY_ID     = os.environ["AWS_ACCESS_KEY_ID"]
    AWS_SECRET_ACCESS_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]
    S3_BUCKET             = "content-media-generation"
    S3_PREFIX             = "hindi/listening_audio"
    AWS_REGION            = "us-east-1"

    # ----------------------

    response = requests.post("http://localhost:8010/convert-to-listening", json={
        "db": DB,
        "lesson_ids": LESSON_IDS,
        "s3_bucket": S3_BUCKET,
        "s3_prefix": S3_PREFIX,
        "aws_region": AWS_REGION,
        "openai_api_key": OPENAI_API_KEY,
        "aws_access_key_id": AWS_ACCESS_KEY_ID,
        "aws_secret_access_key": AWS_SECRET_ACCESS_KEY,
    })

    print(json.dumps(response.json(), indent=2))