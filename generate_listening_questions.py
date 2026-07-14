import requests
import json
import os
from dotenv import load_dotenv

load_dotenv()

BASE = "http://language-media-gen-env.eba-jqm7dpsh.us-east-1.elasticbeanstalk.com"

DB = {
    "host": os.environ.get("DB_HOST"),
    "user": os.environ.get("DB_USER"),
    "password": os.environ.get("DB_PASSWORD"),
    "database": "japanese",          # change as needed
}

S3_AUDIO_PREFIX  = "japanese/listening_audio"
S3_OUTPUT_PREFIX = "japanese/listening_questions"

NUM_QUESTIONS       = 10
LIMIT               = None     # max files to PROCESS (skips don't count). Set 1 for a test run.
FORCE               = False    # re-process even if output JSON exists / lesson already in DB
MODEL               = "gpt-4o-mini"
TRANSCRIPTION_MODEL = "whisper-1"
TEMPERATURE         = 0.4
LESSON_TYPE         = "listening"
CEFR_LEVEL          = "A1"

CEFR_MAPPING = [
    {"min": 1,  "max": 7,  "cefr_level": "A1"},
    {"min": 8,  "max": 15, "cefr_level": "A2"},
    {"min": 16, "max": 23, "cefr_level": "B1"},
    {"min": 24, "max": 30, "cefr_level": "B2"},
]

OPENAI_API_KEY        = os.environ["OPENAI_API_KEY"]
AWS_ACCESS_KEY_ID     = os.environ["AWS_ACCESS_KEY_ID"]
AWS_SECRET_ACCESS_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]
S3_BUCKET             = "content-media-generation"
AWS_REGION            = "us-east-1"


def run():
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

    with requests.post(f"{BASE}/generate-listening-questions",
                       json=payload, stream=True, timeout=7200) as resp:
        for raw in resp.iter_lines(decode_unicode=True):
            if not raw:
                continue
            try:
                ev = json.loads(raw)
            except json.JSONDecodeError:
                print(raw); continue

            e = ev.get("event")
            if e == "start":
                print(f"START generate-listening-questions | db={ev['database']}")
                print(f"  mp3 files found: {ev['audio_files']} | force={ev['force']} | limit={ev.get('limit')}")
            elif e == "processing":
                print(f"\n[{ev['n']}/{ev['total']}] {ev['lesson_title']}")
                print(f"    audio: {ev['audio_key']}")
            elif e == "transcribed":
                print(f"    transcribed ({ev['chars']} chars)")
            elif e == "questions_generated":
                print(f"    generated {ev['questions']} questions")
            elif e == "success":
                print(f"    OK -> lesson {ev['lesson_id']} ({ev['cefr_level']}), "
                      f"{ev['questions']} questions")
            elif e == "skipped":
                print(f"[{ev['n']}/{ev['total']}] SKIPPED {ev['lesson_title']}: {ev['reason']}")
            elif e == "failed":
                print(f"    FAILED {ev['lesson_title']}: {ev['error']}")
            elif e == "limit_reached":
                print(f"\n(limit of {ev['limit']} processed files reached — stopping)")
            elif e == "summary":
                print("\n" + "=" * 55)
                print(f"SUMMARY | files={ev['total_audio_files']} processed={ev['processed']} "
                      f"ok={ev['succeeded']} failed={ev['failed']} skipped={ev['skipped']}")
                print("=" * 55)
            elif e == "error":
                print(f"ERROR: {ev.get('message')}")
            else:
                print(raw)


if __name__ == "__main__":
    run()