import os
import json
import requests
from dotenv import load_dotenv

load_dotenv()

BASE = "http://127.0.0.1:8000"
DATABASE = "spanish"
DRY_RUN = True

# --- selection: set ONE (titles is best after a re-insert, since ids change) ---
TITLES = ["unit16_population_and_cliches"]   # exact Lesson.title(s)
UNITS = None                                  # e.g. [16]
LESSON_IDS = None                             # e.g. [4001]

# --- optional: top-up only specific vocab questions by their JSON sequence_id ---
QUESTION_SEQUENCE_IDS = None                  # e.g. [11, 12]

S3_BUCKET = "my-bucket"
S3_IMAGE_PREFIX = "spanish/images"
S3_AUDIO_PREFIX = "spanish/audio"
AWS_REGION = "us-east-1"

TRANSLATE_VOCAB_AUDIO = False
SOURCE_LANGUAGE = "en"
TARGET_LANGUAGE = "es"
TRANSLATE_IMAGES = False


def run():
    payload = {
        "db": {
            "host": os.environ["DB_HOST"],
            "user": os.environ["DB_USER"],
            "password": os.environ["DB_PASSWORD"],
            "database": DATABASE,
        },
        "s3_bucket": S3_BUCKET,
        "s3_image_prefix": S3_IMAGE_PREFIX,
        "s3_audio_prefix": S3_AUDIO_PREFIX,
        "aws_region": AWS_REGION,
        "translate_vocab_audio": TRANSLATE_VOCAB_AUDIO,
        "source_language": SOURCE_LANGUAGE,
        "target_language": TARGET_LANGUAGE,
        "translate_images": TRANSLATE_IMAGES,
        "dry_run": DRY_RUN,
        # AWS / OpenAI keys fall back to server env vars if omitted
    }
    if LESSON_IDS:
        payload["lesson_ids"] = LESSON_IDS
    elif TITLES:
        payload["titles"] = TITLES
    elif UNITS:
        payload["units"] = UNITS
    if QUESTION_SEQUENCE_IDS:
        payload["question_sequence_ids"] = QUESTION_SEQUENCE_IDS

    with requests.post(f"{BASE}/regenerate-lesson-media", json=payload, stream=True) as resp:
        for raw in resp.iter_lines(decode_unicode=True):
            if not raw:
                continue
            try:
                ev = json.loads(raw)
            except json.JSONDecodeError:
                print(raw); continue

            e = ev.get("event")
            if e == "start":
                print(f"START regenerate-lesson-media | dry_run={ev.get('dry_run')} | by {ev.get('selector')}")
            elif e == "resolved":
                print(f"  resolved {ev['count']} lesson(s):")
                for l in ev["lessons"]:
                    print(f"    [{l['lesson_id']}] {l['type']:11} {l['title']}")
            elif e == "lesson_start":
                print(f"  -- lesson {ev['lesson_id']} ({ev['type']}) {ev['title']}")
            elif e == "vocab_plan":
                print(f"     plan: {ev['questions']} questions | need_image={ev['need_image']} need_audio={ev['need_audio']}")
            elif e == "image":
                print(f"     image  q{ev['question_id']}: {ev['url']}")
            elif e == "audio":
                print(f"     audio  q{ev['question_id']}: {ev['url']}")
            elif e in ("image_error", "audio_error"):
                print(f"     ERROR  q{ev['question_id']}: {ev['error']}")
            elif e == "manual_todo":
                print(f"     GRAMMAR manual steps (video_linked={ev.get('video_linked')}):")
                for s in ev["steps"]:
                    print(f"        - {s}")
            elif e == "link_via_ingest":
                print(f"     {ev['type']}: images_present={ev['images_present']} | {ev['note']}")
            elif e == "lesson_done":
                if ev.get("note"):
                    print(f"     done ({ev['note']})")
            elif e == "summary":
                print("=" * 50)
                print(f"SUMMARY | dry_run={ev['dry_run']} | {ev['totals']}")
                print("=" * 50)
            elif e == "error":
                print(f"ERROR: {ev.get('message')}")
            else:
                print(raw)


if __name__ == "__main__":
    run()