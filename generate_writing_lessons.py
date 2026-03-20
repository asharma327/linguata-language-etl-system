import requests
import json
import os
from dotenv import load_dotenv

load_dotenv()

if __name__ == "__main__":
    # --- Select database ---
    LANGUAGE = "hindi"   # "hindi" or "spanish"

    # --- Configure below ---

    UNITS          = [41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51]       # list of unit numbers to process
    NUM_QUESTIONS  = 7
    FORCE          = False      # set True to regenerate even if writing lesson already exists
    LIMIT          = None       # not used by this endpoint, kept for reference

    OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]

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
        "units": UNITS,
        "num_questions": NUM_QUESTIONS,
        "force": FORCE,
        "openai_api_key": OPENAI_API_KEY,
    }

response = requests.post("http://localhost:8010/generate-writing-lessons", json=payload)
print(json.dumps(response.json(), indent=2))