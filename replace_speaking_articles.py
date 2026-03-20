import os
import requests
import json
from dotenv import load_dotenv

load_dotenv()

if __name__ == "__main__":
    # --- Select database ---
    LANGUAGE = "hindi"   # "hindi" or "spanish"

    # --- Configure below ---

    FOLDER = "/Users/adhaar/desktop/client_documents/learnx/hindi_pdf_audit/speaking_missing_lessons"   # path to folder containing JSON files
    LIMIT  = None                              # max number of files to process (None = all)

    # ----------------------

    DB_CONFIGS = {
        "hindi": {
            "host": os.environ["DB_HOST"],
            "user": os.environ["DB_USER"],
            "password": os.environ["DB_PASSWORD"],
            "database": "hindi",
        },
        "spanish": {
            "host": os.environ["DB_HOST"],
            "user": os.environ["DB_USER"],
            "password": os.environ["DB_PASSWORD"],
            "database": "spanish",
        },
    }

    if LANGUAGE not in DB_CONFIGS:
        raise ValueError(f"Unknown LANGUAGE '{LANGUAGE}'. Must be 'hindi' or 'spanish'.")

    payload = {
        "db": DB_CONFIGS[LANGUAGE],
        "folder": FOLDER,
    }

    if LIMIT is not None:
        payload["limit"] = LIMIT

    response = requests.post("http://localhost:8010/replace-speaking-articles", json=payload)
    print(json.dumps(response.json(), indent=2))