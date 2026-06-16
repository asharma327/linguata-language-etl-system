import requests
import json
import os
from dotenv import load_dotenv

load_dotenv()

if __name__ == "__main__":

    # ==========================================================
    # CHANGE ONLY THIS WHEN SWITCHING LANGUAGES
    # ==========================================================
    DATABASE_NAME = "japanese"
    # ==========================================================

    DB = {
        "host": os.environ["DB_HOST"],
        "user": os.environ["DB_USER"],
        "password": os.environ["DB_PASSWORD"],
        "database": DATABASE_NAME,
    }

    # ----------------------------------------------------------
    # OPTION 1: exact lesson titles (one or many)
    # ----------------------------------------------------------
    TITLES = [
        "unit1_grammar_do_the_japanese_understand_english"
        # "unit22_grammar",
    ]

    # ----------------------------------------------------------
    # OPTION 2: all grammar lessons in these units (REGEXP ^unit{N}_ )
    # ----------------------------------------------------------
    UNITS = [
        # 21, 22, 23, 24, 25, 26, 27, 28, 29, 30
        ]

    # ----------------------------------------------------------
    # OPTION 3: explicit lessons (optionally specific article_ids)
    # ----------------------------------------------------------
    LESSONS = [
        # {"lesson_id": 3251},
        # {"lesson_id": 3252, "article_ids": [901, 902]},
    ]

    # GLOBAL cap across whichever mode(s) you use. Set 1 to generate only the FIRST article.
    LIMIT_ARTICLES = 1000

    OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]
    AWS_ACCESS_KEY_ID = os.environ["AWS_ACCESS_KEY_ID"]
    AWS_SECRET_ACCESS_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]

    S3_BUCKET = "content-media-generation"
    S3_PREFIX = f"{DATABASE_NAME}/grammar_audio"
    AWS_REGION = "us-east-1"

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
    if TITLES:
        payload["titles"] = TITLES
    if UNITS:
        payload["units"] = UNITS
    if LESSONS:
        payload["lessons"] = LESSONS

    print("=" * 70)
    print(f"Database      : {DATABASE_NAME}")
    print(f"Titles        : {TITLES}")
    print(f"Units         : {UNITS}")
    print(f"Lessons       : {LESSONS}")
    print(f"S3 Prefix     : {S3_PREFIX}")
    print(f"Limit Articles: {LIMIT_ARTICLES}")
    print("=" * 70)

    with requests.post("http://language-media-gen-env.eba-jqm7dpsh.us-east-1.elasticbeanstalk.com/generate-grammar-audio",
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
                print(f"START grammar-audio | db={ev['database']}")
                print(f"  titles={ev.get('titles')} units={ev.get('units')} lessons={ev.get('lessons')}")
            elif e == "found":
                print(f"  articles to process: {ev['total']}")
            elif e == "processing":
                print(f"  [{ev['n']}/{ev['total']}] {ev['lesson_title']} "
                      f"(lesson {ev['lesson_id']}, article {ev['article_id']}, seq {ev['sequence_id']})")
            elif e == "success":
                print(f"       OK -> {ev['audio_url']}")
            elif e == "failed":
                print(f"       FAILED article {ev['article_id']}: {ev['error']}")
            elif e == "lesson_error":
                print(f"  LESSON ERROR {ev['lesson_id']}: {ev['error']}")
            elif e == "summary":
                print("=" * 50)
                print(f"SUMMARY | items={ev['total_items']} succeeded={ev['succeeded']} failed={ev['failed']}")
                print("=" * 50)
            elif e == "error":
                print(f"ERROR: {ev.get('message')}")
            else:
                print(raw)