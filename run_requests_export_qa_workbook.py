import os
import requests
import json
from dotenv import load_dotenv

load_dotenv()

URL = "http://127.0.0.1:8000/export-qa-workbook"

LANGUAGE = "extractiondb"        # also used in the output filename
UNITS = None                # None = all units; or a list e.g. [1] for one unit, [1, 2, 3] for a subset
OUTPUT_DIR = "D:\\DATA\\tmp"         # where the .xlsx is written on the server host


if __name__ == "__main__":
    payload = {
        "db": {
            "host": os.environ["DB_HOST"],
            "user": os.environ["DB_USER"],
            "password": os.environ["DB_PASSWORD"],
            "database": LANGUAGE,
        },
        "language_label": LANGUAGE,
        "output_dir": OUTPUT_DIR,
    }
    if UNITS is not None:
        payload["units"] = UNITS

    response = requests.post(URL, json=payload)
    data = response.json()
    print(json.dumps(data, indent=2))

    if data.get("status") == "ok":
        print(f"\nWrote {data['tabs']} unit tab(s) to: {data['output_path']}")
        print(f"Units: {data['units_exported']}")