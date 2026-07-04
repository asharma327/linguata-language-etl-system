import os
import json
import requests
from dotenv import load_dotenv

load_dotenv()

language = "italian"

DB = {
    "host": os.environ.get("DB_HOST"),
    "user": os.environ.get("DB_USER"),
    "password": os.environ.get("DB_PASSWORD"),
    "database": language,
}

# Restrict to specific grammar lessons (the ones whose video you regenerated).
# Leave empty to process every .mp4 in the prefix.
TITLES = [
    "unit12_language_usage",
    "unit20_language_usage",
]

# False = link only lessons with no video yet.
# True  = REPLACE existing video for the matched lessons (use after QA regenerate).
FORCE = False

AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
S3_BUCKET = "content-media-generation"
S3_PREFIX = f"{language}/grammar_videos"
AWS_REGION = "us-east-1"


def run():
    payload = {
        "db": DB,
        "s3_bucket": S3_BUCKET,
        "s3_prefix": S3_PREFIX,
        "aws_region": AWS_REGION,
        "aws_access_key_id": AWS_ACCESS_KEY_ID,
        "aws_secret_access_key": AWS_SECRET_ACCESS_KEY,
        "force": FORCE,
    }
    if TITLES:
        payload["titles"] = TITLES

    with requests.post("http://language-media-gen-env.eba-jqm7dpsh.us-east-1.elasticbeanstalk.com/link-grammar-videos",
                       json=payload, stream=True, timeout=3600) as resp:
        for raw in resp.iter_lines(decode_unicode=True):
            if not raw:
                continue
            try:
                ev = json.loads(raw)
            except json.JSONDecodeError:
                print(raw); continue

            e = ev.get("event")
            if e == "start":
                print(f"START link-grammar-videos | db={ev['database']} | force={ev['force']}")
                print(f"  prefix={ev['s3_prefix']} | titles={ev.get('titles')}")
            elif e == "found":
                print(f"  .mp4 files in prefix: {ev['total_mp4']}")
            elif e == "inserted":
                print(f"  + INSERTED {ev['title']} (lesson {ev['lesson_id']}) -> video {ev['video_id']}")
            elif e == "replaced":
                print(f"  ~ REPLACED {ev['title']} (lesson {ev['lesson_id']}) -> video {ev['video_id']} "
                      f"(removed {ev['removed_video_ids']})")
            elif e == "skipped":
                print(f"  - skipped lesson {ev['lesson_id']} ({ev.get('title')}): {ev['reason']}")
            elif e == "file_skipped":
                print(f"  . file skipped (lesson {ev['lesson_id']}): {ev['reason']}")
            elif e == "file_error":
                print(f"  ! ERROR {ev.get('key')}: {ev['error']}")
            elif e == "warning":
                print(f"  WARNING: {ev['message']} -> {ev.get('titles')}")
            elif e == "summary":
                print("=" * 55)
                print(f"SUMMARY | {ev['totals']}")
                print("=" * 55)
            elif e == "error":
                print(f"ERROR: {ev.get('message')}")
            else:
                print(raw)


if __name__ == "__main__":
    run()