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

    LIMIT            = None   # max number of vocab questions to process (None = all)
    TTS_MODEL        = "gpt-4o-mini-tts"
    VOICE            = "alloy"
    TTS_INSTRUCTIONS = "Speak slowly and clearly with a warm, friendly, teacher-like tone."
    SOURCE_LANGUAGE  = "en"   # AWS Translate source language
    TARGET_LANGUAGE  = "es"   # AWS Translate target language (Spanish)

    OPENAI_API_KEY        = os.environ["OPENAI_API_KEY"]
    AWS_ACCESS_KEY_ID     = os.environ["AWS_ACCESS_KEY_ID"]
    AWS_SECRET_ACCESS_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]
    S3_BUCKET             = "content-media-generation"
    S3_PREFIX             = "spanish/vocab_audio"
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
        "tts_model": TTS_MODEL,
        "voice": VOICE,
        "tts_instructions": TTS_INSTRUCTIONS,
        "source_language": SOURCE_LANGUAGE,
        "target_language": TARGET_LANGUAGE,
    }

    if LIMIT is not None:
        payload["limit"] = LIMIT

    response = requests.post("http://localhost:8000/generate-vocab-audio", json=payload)
    print(json.dumps(response.json(), indent=2))