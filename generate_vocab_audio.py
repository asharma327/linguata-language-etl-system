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
        "database": "spanish"          # <-- change only this
    }

    # Lessons to process. One or many — for a single lesson use a one-item list.
    # Set TITLES = None to scan the whole database.
    TITLES = [
        "unit21_basic_sentences",
        "unit22_basic_sentences",
        "unit23_basic_sentences",
        "unit24_basic_sentences",
        "unit25_basic_sentences",
        "unit26_basic_sentences",
        "unit27_basic_sentences",
        "unit28_basic_sentences",
        "unit29_basic_sentences",
        "unit30_basic_sentences",
    ]

    LIMIT = None  # limit number of questions to process (for testing); set to None for no limit

    TTS_MODEL = "gpt-4o-mini-tts"
    SOURCE_LANGUAGE = "en"
    TARGET_LANGUAGE = "es"

    OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]
    AWS_ACCESS_KEY_ID = os.environ["AWS_ACCESS_KEY_ID"]
    AWS_SECRET_ACCESS_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]

    S3_BUCKET = "content-media-generation"
    S3_PREFIX = "spanish/vocab_audio"
    AWS_REGION = "us-east-1"

    payload = {
        "db": DB,
        "s3_bucket": S3_BUCKET,
        "s3_prefix": S3_PREFIX,
        "aws_region": AWS_REGION,
        "openai_api_key": OPENAI_API_KEY,
        "aws_access_key_id": AWS_ACCESS_KEY_ID,
        "aws_secret_access_key": AWS_SECRET_ACCESS_KEY,
        "tts_model": TTS_MODEL,
        "source_language": SOURCE_LANGUAGE,
        "target_language": TARGET_LANGUAGE,
    }
    if TITLES:
        payload["titles"] = TITLES
    if LIMIT:
        payload["limit"] = LIMIT

    with requests.post("http://localhost:8000/generate-vocab-audio",
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
                print(f"START generate-vocab-audio | db={ev['database']} | scope={ev['scope']}")
            elif e == "found":
                print(f"  questions needing audio: {ev['total']}")
            elif e == "processing":
                print(f"  [{ev['n']}/{ev['total']}] {ev['lesson_title']} q{ev['question_id']} "
                      f"(seq {ev['sequence_id']}): {ev['question_text']!r}")
            elif e == "success":
                print(f"       OK ({ev['row']}) -> {ev['audio_url']}")
            elif e == "failed":
                print(f"       FAILED q{ev['question_id']}: {ev['error']}")
            elif e == "summary":
                print("=" * 50)
                print(f"SUMMARY | found={ev['total_found']} succeeded={ev['succeeded']} failed={ev['failed']}")
                print("=" * 50)
            elif e == "error":
                print(f"ERROR: {ev.get('message')}")
            else:
                print(raw)