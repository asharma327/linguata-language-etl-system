import os
import requests
import json
from dotenv import load_dotenv

load_dotenv()

# url = "http://localhost:8000/clone-schema"

# BASE = "/Users/adhaar/desktop/client_documents/learnx/extracted-json/japanese"     #Fow Mac
BASE = r"D:\DATA\tmp\try"                                                           # For Windows

if __name__ == "__main__":
    response = requests.post("http://127.0.0.1:8000/insert-lessons", json={
        "db": {
            "host": os.environ["DB_HOST"],
            "user": os.environ["DB_USER"],
            "password": os.environ["DB_PASSWORD"],
            "database": "spanish",
        },

        # "files": [
        #     f"{BASE}/unit1_finder_list.json",
        #     f"{BASE}/unit2_finder_list.json",
        #     f"{BASE}/unit3_finder_list.json",
        #     f"{BASE}/unit4_finder_list.json"
        #     f"{BASE}/unit33_basic_sentences.json",
        #     f"{BASE}/unit33_indirect_object_grammar.json",
        #     f"{BASE}/unit34_basic_sentences.json",
        #     f"{BASE}/unit34_future_tense_permission_grammar.json",
        #     f"{BASE}/unit35_basic_sentences.json",
        #     f"{BASE}/unit35_numbers_time_grammar.json",
        # ],
        "folder": BASE,

        "cefr_mapping": [
            {"min": 1,  "max": 5,  "cefr_level": "A1"},
            {"min": 6,  "max": 10, "cefr_level": "A2"},
            # {"min": 11, "max": 15, "cefr_level": "B1"},
            # {"min": 16, "max": 20, "cefr_level": "B2"},
            # {"min": 21, "max": 25, "cefr_level": "C1"},
            # {"min": 26, "max": 30, "cefr_level": "C2"},
        ]
    })

    print(json.dumps(response.json(), indent=2))

