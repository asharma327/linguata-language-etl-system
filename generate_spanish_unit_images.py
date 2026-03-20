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

    S3_PREFIX = "spanish/unit_images"  # S3 prefix where generated PNGs are written

    UNITS         = None         # e.g. [1, 2, 3] to target specific units; None = auto-detect all
    FORCE         = False        # re-generate even if an image already exists in S3
    IMAGE_MODEL   = "gpt-image-1"
    THEME_MODEL   = "gpt-4o-mini"
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
        "s3_prefix": S3_PREFIX,
        "aws_region": AWS_REGION,
        "openai_api_key": OPENAI_API_KEY,
        "aws_access_key_id": AWS_ACCESS_KEY_ID,
        "aws_secret_access_key": AWS_SECRET_ACCESS_KEY,
        "image_model": IMAGE_MODEL,
        "theme_model": THEME_MODEL,
        "temperature": TEMPERATURE,
        "force": FORCE,
    }

    if UNITS:
        payload["units"] = UNITS

    response = requests.post("http://localhost:8020/generate-unit-images", json=payload)
    print(json.dumps(response.json(), indent=2))