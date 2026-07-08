import requests
import json
import os
from dotenv import load_dotenv

load_dotenv()

language = "japanese"  # Change this to the desired language

if __name__ == "__main__":

    DB = {
        "host": os.environ["DB_HOST"],
        "user": os.environ["DB_USER"],
        "password": os.environ["DB_PASSWORD"],
        "database": language          
    }

    # Lessons to process. One or many — for a single lesson use a one-item list.
    # Set TITLES = None to scan the whole database.
    TITLES = [
        "unit1_vocabulary_greetings",
        "unit1_vocabulary_loanwords",
        "unit1_vocabulary_notes",
        "unit1_vocabulary_numbers",
        "unit1_vocabulary_presentation",
        "unit4_reading_kanji",
        "unit4_vocabulary_dialogs",
        "unit4_vocabulary_notes",
        "unit4_vocabulary_practice",
        "unit5_reading_review_kanji",
        "unit5_vocabulary_days_of_week",
        "unit5_vocabulary_dialogs",
        "unit5_vocabulary_notes",
        "unit5_vocabulary_practice",
        "unit2_vocabulary",
        "unit3_reading_kanji_buildings",
        "unit3_vocabulary_dialogs",
        "unit3_vocabulary_foods",
        "unit3_vocabulary_japanese_objects",
        "unit3_vocabulary_loan_words",
        "unit3_vocabulary_notes",
        "unit10_pronunciation_drill_a_part1",
        "unit10_pronunciation_drill_a_part2",
        "unit10_reading_kanji",
        "unit10_supplementary_b_telling_time",
        "unit10_vocabulary_dialogs",
        "unit10_vocabulary_notes",
        "unit10_vocabulary_practice",
        "unit10_vocabulary_supplement_time",
        "unit6_reading_kanji",
        "unit6_response_drill_b",
        "unit6_vocabulary_place_names_in_tokyo",
        "unit7_reading_kanji",
        "unit7_response_drill_c",
        "unit7_vocabulary_dialogs",
        "unit7_vocabulary_notes",
        "unit7_vocabulary_practice",
        "unit8_reading_kanji",
        "unit8_response_drill_c",
        "unit8_vocabulary_dialogs",
        "unit8_vocabulary_notes",
        "unit8_vocabulary_practice",
        "unit9_reading_kanji",
        "unit9_vocabulary_dialogs",
        "unit9_vocabulary_notes",
        "unit9_vocabulary_practice",
        "unit9_vocabulary_street_names",
        "unit11_reading_kanji",
        "unit11_reading_numbers_e_part1",
        "unit11_reading_numbers_e_part2",
        "unit11_vocabulary_dialogs",
        "unit11_vocabulary_notes",
        "unit11_vocabulary_practice",
        "unit12_reading_kanji",
        "unit12_reading_numbers_a_part1",
        "unit12_reading_numbers_a_part2",
        "unit12_vocabulary_dialogs",
        "unit12_vocabulary_notes",
        "unit12_vocabulary_practice",
        "unit13_reading_kanji",
        "unit13_vocabulary_dialogs",
        "unit13_vocabulary_notes",
        "unit14_reading_kanji",
        "unit14_vocabulary_dialogs",
        "unit14_vocabulary_notes",
        "unit15_reading_kanji",
        "unit15_vocabulary_dialogs",
        "unit15_vocabulary_notes",
        "unit16_number_reading_a",
        "unit16_pronunciation_practice_b",
        "unit16_reading_kanji",
        "unit16_vocabulary_dialogs",
        "unit16_vocabulary_practice",
        "unit17_reading_kanji",
        "unit17_vocabulary_dialogs",
        "unit17_vocabulary_notes",
        "unit17_vocabulary_practice",
        "unit18_kanji_numerals_b",
        "unit18_number_reading_a",
        "unit18_reading_kanji",
        "unit18_vocabulary_dialogs",
        "unit18_vocabulary_notes",
        "unit18_vocabulary_practice",
        "unit19_reading_kanji",
        "unit19_vocabulary_dialogs",
        "unit19_vocabulary_practice",
        "unit20_days_of_month",
        "unit20_greetings_a",
        "unit20_months_kanji",
        "unit20_reading_kanji",
        "unit20_supplement_days",
        "unit20_vocabulary_dialogs",
        "unit20_vocabulary_months",
        "unit20_vocabulary_notes",
        "unit20_vocabulary_practice",
        "unit21_person_counter_a",
        "unit21_reading_kanji",
        "unit21_response_drill_b",
        "unit21_vocabulary_dialogs",
        "unit21_vocabulary_notes",
        "unit21_vocabulary_practice",
        "unit22_reading_kanji",
        "unit22_repetition_drill_a",
        "unit22_vocabulary_dialogs",
        "unit22_vocabulary_notes",
        "unit22_vocabulary_practice",
        "unit23_reading_review_kanji",
        "unit23_response_drill_c",
        "unit23_vocabulary_dialogs",
        "unit23_vocabulary_notes",
        "unit23_vocabulary_practice",
        "unit24_reading_review_kanji",
        "unit24_vocabulary_dialogs",
        "unit24_vocabulary_notes",
        "unit25_reading_kanji",
        "unit25_vocabulary_dialogs",
        "unit25_vocabulary_notes",
        "unit25_vocabulary_practice",
        "unit25_word_study_a",
        "unit25_word_study_c",
        "unit26_reading_review_kanji",
        "unit26_vocabulary_dialogs",
        "unit26_vocabulary_notes",
        "unit27_reading_review_kanji",
        "unit27_vocabulary_dialogs",
        "unit27_vocabulary_notes",
        "unit27_vocabulary_practice",
        "unit27_vocabulary_reference",
        "unit27_word_study_a",
        "unit28_reading_review_kanji",
        "unit28_vocabulary_dialogs",
        "unit28_vocabulary_notes",
        "unit28_vocabulary_practice",
        "unit28_vocabulary_supplement",
        "unit28_word_study_c",
        "unit28_word_study_d",
        "unit29_reading_kanji",
        "unit29_vocabulary_dialogs",
        "unit29_vocabulary_practice",
        "unit30_reading_review_kanji",
        "unit30_response_drill_e",
        "unit30_vocabulary_dialogs",
        "unit30_vocabulary_practice"
    ]
    LIMIT = None  # limit number of questions to process (for testing); set to None for no limit

    TTS_MODEL = "gpt-4o-mini-tts"
    SOURCE_LANGUAGE = "en"
    TARGET_LANGUAGE = "ja"  # Change this to the desired target language code (e.g., "fr" for French, "es" for Spanish, etc.)

    OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]
    AWS_ACCESS_KEY_ID = os.environ["AWS_ACCESS_KEY_ID"]
    AWS_SECRET_ACCESS_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]

    S3_BUCKET = "content-media-generation"
    S3_PREFIX = f"{language}/vocab_audio" #for languages other than french, it is --> f"{language}/vocab_audio" and for french it is --> "french/audio"
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