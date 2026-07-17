import os
import requests
import json
from dotenv import load_dotenv

load_dotenv()

# BASE = "/Users/adhaar/desktop/client_documents/learnx/extracted-json/japanese"     #Fow Mac
BASE = r"D:\DATA\tmp\try"                                                            #For Windows

if __name__ == "__main__":
    response = requests.post("http://127.0.0.1:8000/insert-lessons", json={
        "db": {
            "host": os.environ["DB_HOST"],
            "user": os.environ["DB_USER"],
            "password": os.environ["DB_PASSWORD"],
            "database": "chinese",
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
            {"min": 1,  "max": 9,  "cefr_level": "A1"},
            {"min": 10,  "max": 18, "cefr_level": "A2"},
            {"min": 19, "max": 27, "cefr_level": "B1"},
            {"min": 28, "max": 36, "cefr_level": "B2"},
            {"min": 37, "max": 45, "cefr_level": "C1"},
            {"min": 46, "max": 55, "cefr_level": "C2"},
        ]
    })

    print(json.dumps(response.json(), indent=2))

