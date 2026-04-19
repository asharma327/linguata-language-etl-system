import os
import requests
import json
from dotenv import load_dotenv

load_dotenv()

language = "german"
if __name__ == "__main__":
    DB = {
        "host": os.environ["DB_HOST"],
        "user": os.environ["DB_USER"],
        "password": os.environ["DB_PASSWORD"],
        "database": language
    }

    # --- Configure below ---

    UNITS = [21, 22, 23, 24, 25, 26, 27, 28, 29, 30]        # e.g. [1, 2, 3]  — auto-selects all vocab questions without images

    # 3249, 3252, 3255, 3258, 3261, 3264, 3267, 3270, 3273, 3276
    # 3279, 3282, 3285, 3288, 3291, 3294, 3297, 3301, 3303,
    # 3306, 3309, 3312, 3315, 3334, 3335, 3336, 3339, 3342, 3345, 3348
    # 3351, 3354, 3355, 3358, 3361, 3364, 3367, 3370, 3373, 3376
    # 3379, 3382, 3385, 3388, 3391, 3394, 3397, 3398, 3401, 3405, 3408, 3410, 3412, 3414, 3416
    LESSONS = [
        # {"lesson_id": 3306},
        # {"lesson_id": 3309},
        # {"lesson_id": 3312},
        # {"lesson_id": 3315},
        # {"lesson_id": 3334},
        # {"lesson_id": 3335},
        # {"lesson_id": 3336},
        # {"lesson_id": 3339},
        # {"lesson_id": 3342},
        # {"lesson_id": 3345},
        # {"lesson_id": 3348},
        # {"lesson_id": 3351},
        # {"lesson_id": 3354},
        # {"lesson_id": 3355},
        # {"lesson_id": 3358},
        # {"lesson_id": 3361},
        # {"lesson_id": 3364},
        # {"lesson_id": 3367},
        # {"lesson_id": 3370},
        # {"lesson_id": 3373},
        # {"lesson_id": 3376},
        # {"lesson_id": 3379},
        # {"lesson_id": 3382},
        # {"lesson_id": 3385},
        # {"lesson_id": 3388},
        # {"lesson_id": 3391},
        # {"lesson_id": 3394},
        # {"lesson_id": 3397},
        # {"lesson_id": 3398},
        # {"lesson_id": 3401},
        # {"lesson_id": 3405},
        # {"lesson_id": 3408},
        # {"lesson_id": 3410},
        # {"lesson_id": 3412},
        # {"lesson_id": 3414},
        # {"lesson_id": 3416},
    ]

    ADDITIONAL_PROMPT = None   # global prompt applied to all, e.g. "Clean white background"
    LIMIT_QUESTIONS   = 500    # max questions to process when using units
    TRANSLATE         = False  # True if question_text is NOT already in English

    OPENAI_API_KEY= os.environ["OPENAI_API_KEY"]
    AWS_ACCESS_KEY_ID=os.environ["AWS_ACCESS_KEY_ID"]
    AWS_SECRET_ACCESS_KEY=os.environ["AWS_SECRET_ACCESS_KEY"]
    S3_BUCKET            = "content-media-generation"
    S3_PREFIX            = f"{language}/images"
    AWS_REGION           = "us-east-1"

    # ----------------------

    payload = {
        "db": DB,
        "s3_bucket": S3_BUCKET,
        "s3_prefix": S3_PREFIX,
        "aws_region": AWS_REGION,
        "openai_api_key": OPENAI_API_KEY,
        "aws_access_key_id": AWS_ACCESS_KEY_ID,
        "aws_secret_access_key": AWS_SECRET_ACCESS_KEY,
        "limit_questions": LIMIT_QUESTIONS,
        "translate": TRANSLATE,
    }

    if UNITS:
        payload["units"] = UNITS
    if LESSONS:
        payload["lessons"] = LESSONS
    if ADDITIONAL_PROMPT:
        payload["additional_prompt"] = ADDITIONAL_PROMPT

    response = requests.post("http://localhost:8010/generate-lesson-images", json=payload)
    print(json.dumps(response.json(), indent=2))