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
        "database": "spanish"  # change to "spanish" etc. as needed
    }

    # --- Configure below ---

    S3_AUDIO_PREFIX  = "spanish/audio"   # S3 prefix where source MP3 files live
    S3_OUTPUT_PREFIX = "spanish/listening_questions"  # S3 prefix where generated question JSONs are written

    NUM_QUESTIONS        = 10      # number of questions to generate per audio file
    LIMIT                = None    # max number of audio files to process (None = all)
    FORCE                = False   # re-process even if output JSON already exists in S3
    MODEL                = "gpt-4o-mini"
    TRANSCRIPTION_MODEL  = "whisper-1"
    TEMPERATURE          = 0.4
    LESSON_TYPE          = "listening"
    CEFR_LEVEL           = "A1"   # fallback when no cefr_mapping matches

    # Optional unit-range → CEFR level mapping, e.g.:
    # CEFR_MAPPING = [
    #     {"min": 1,  "max": 3,  "cefr_level": "A1"},
    #     {"min": 4,  "max": 6,  "cefr_level": "A2"},
    # ]
    CEFR_MAPPING = [
        {"min": 1, "max": 5, "cefr_level": "A1"},
        {"min": 6, "max": 10, "cefr_level": "A2"},
        {"min": 11, "max": 15, "cefr_level": "B1"},
        {"min": 16, "max": 20, "cefr_level": "B2"},
        {"min": 21, "max": 25, "cefr_level": "C1"},
        {"min": 26, "max": 30, "cefr_level": "C2"},
    ]

    OPENAI_API_KEY        = os.environ["OPENAI_API_KEY"]
    AWS_ACCESS_KEY_ID     = os.environ["AWS_ACCESS_KEY_ID"]
    AWS_SECRET_ACCESS_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]
    S3_BUCKET             = "content-media-generation"
    AWS_REGION            = "us-east-1"

    # ----------------------

    payload = {
        "db": DB,
        "s3_bucket": S3_BUCKET,
        "s3_audio_prefix": S3_AUDIO_PREFIX,
        "s3_output_prefix": S3_OUTPUT_PREFIX,
        "aws_region": AWS_REGION,
        "openai_api_key": OPENAI_API_KEY,
        "aws_access_key_id": AWS_ACCESS_KEY_ID,
        "aws_secret_access_key": AWS_SECRET_ACCESS_KEY,
        "num_questions": NUM_QUESTIONS,
        "model": MODEL,
        "transcription_model": TRANSCRIPTION_MODEL,
        "temperature": TEMPERATURE,
        "force": FORCE,
        "lesson_type": LESSON_TYPE,
        "cefr_level": CEFR_LEVEL,
    }

    if LIMIT is not None:
        payload["limit"] = LIMIT
    if CEFR_MAPPING:
        payload["cefr_mapping"] = CEFR_MAPPING

    response = requests.post("http://localhost:8010/generate-listening-questions", json=payload)
    print(json.dumps(response.json(), indent=2))