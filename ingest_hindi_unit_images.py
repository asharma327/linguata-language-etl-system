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

    AWS_ACCESS_KEY_ID     = os.environ["AWS_ACCESS_KEY_ID"]
    AWS_SECRET_ACCESS_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]
    S3_BUCKET             = "content-media-generation"
    S3_PREFIX             = "hindi/unit_images"
    AWS_REGION            = "us-east-1"
    FORCE                 = False
    LESSON_IDS            = [3318, 3403]   # link existing unit images to these specific lessons

    # ----------------------

    response = requests.post("http://localhost:8010/ingest-unit-images", json={
        "db": DB,
        "s3_bucket": S3_BUCKET,
        "s3_prefix": S3_PREFIX,
        "aws_region": AWS_REGION,
        "aws_access_key_id": AWS_ACCESS_KEY_ID,
        "aws_secret_access_key": AWS_SECRET_ACCESS_KEY,
        "force": FORCE,
        "lesson_ids": LESSON_IDS,
    })

    print(json.dumps(response.json(), indent=2))