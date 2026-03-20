import os
import requests
import json
from dotenv import load_dotenv

load_dotenv()

# url = "http://localhost:8000/clone-schema"

BASE = "/Users/adhaar/desktop/client_documents/learnx/extracted-json/hindi"

if __name__ == "__main__":
    response = requests.post("http://localhost:8000/insert-lessons", json={
        "db": {
            "host": os.environ["DB_HOST"],
            "user": os.environ["DB_USER"],
            "password": os.environ["DB_PASSWORD"],
            "database": "hindi"
        },
        "files": [
            f"{BASE}/unit31_basic_sentences.json",
            f"{BASE}/unit31_past_obligation_grammar.json",
            f"{BASE}/unit32_basic_sentences.json",
            f"{BASE}/unit32_present_continuous_grammar.json",
            f"{BASE}/unit33_basic_sentences.json",
            f"{BASE}/unit33_indirect_object_grammar.json",
            f"{BASE}/unit34_basic_sentences.json",
            f"{BASE}/unit34_future_tense_permission_grammar.json",
            f"{BASE}/unit35_basic_sentences.json",
            f"{BASE}/unit35_numbers_time_grammar.json",
        ],
        "cefr_mapping": [
            {"min": 1,  "max": 9,  "cefr_level": "A1"},
            {"min": 10,  "max": 18, "cefr_level": "A2"},
            {"min": 19, "max": 27, "cefr_level": "B1"},
            {"min": 28, "max": 35, "cefr_level": "B2"},
            {"min": 36, "max": 43, "cefr_level": "C1"},
            {"min": 44, "max": 51, "cefr_level": "C2"},
        ]
    })

    print(json.dumps(response.json(), indent=2))

