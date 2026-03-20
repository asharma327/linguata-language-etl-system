import os
import requests
import json
from dotenv import load_dotenv

load_dotenv()

if __name__ == "__main__":
    # Run this once per database to add the audio_metadata column if it's missing.
    # Safe to re-run — it checks before altering.

    DATABASES = [
        {
            "host": os.environ["DB_HOST"],
            "user": os.environ["DB_USER"],
            "password": os.environ["DB_PASSWORD"],
            "database": "spanish",
        },
        {
            "host": os.environ["DB_HOST"],
            "user": os.environ["DB_USER"],
            "password": os.environ["DB_PASSWORD"],
            "database": "hindi",
        },
    ]

    for db in DATABASES:
        response = requests.post("http://localhost:8000/migrate-audio-table", json=db)
        print(json.dumps(response.json(), indent=2))