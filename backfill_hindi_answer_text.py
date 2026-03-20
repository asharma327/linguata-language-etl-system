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

    FOLDER   = "/Users/adhaar/desktop/client_documents/learnx/hindi_pdf_audit/grammar_missing_answers_lessons"   # set to folder path, or None to use FILENAME
    FILENAME = None                              # set to a single file path, or None to use FOLDER
    LIMIT    = None                              # max number of files to process (None = all)

    # ----------------------

    payload = {"db": DB}

    if FOLDER:
        payload["folder"] = FOLDER
    if FILENAME:
        payload["filename"] = FILENAME
    if LIMIT is not None:
        payload["limit"] = LIMIT

    response = requests.post("http://localhost:8010/backfill-answer-text", json=payload)
    print(json.dumps(response.json(), indent=2))