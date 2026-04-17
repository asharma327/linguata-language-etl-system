from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import pymysql
import re
import traceback
import json
import io
import base64
import os
import logging
from pathlib import Path
from typing import Optional
from mangum import Mangum
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("linguata")
import boto3
from PIL import Image as PILImage
from openai import OpenAI
import openai

load_dotenv()

app = FastAPI()


# --- Pydantic Models ---

class DBConfig(BaseModel):
    host: str
    user: str
    password: str
    database: str


class CloneSchemaRequest(BaseModel):
    source: DBConfig
    dest: DBConfig


class CefrRange(BaseModel):
    min: int
    max: int
    cefr_level: str


class InsertLessonsRequest(BaseModel):
    db: DBConfig
    folder: str | None = None
    files: list[str] | None = None
    cefr_level: str = "A1"                    # fallback if no mapping matches
    cefr_mapping: list[CefrRange] | None = None


class LessonImageTarget(BaseModel):
    lesson_id: int
    question_ids: list[int] | None = None   # if None + vocab lesson → all questions
    additional_prompt: str | None = None    # overrides global prompt for this lesson


class GenerateImagesRequest(BaseModel):
    db: DBConfig
    units: list[int] | None = None                  # path 1: select vocab lessons by unit number
    lessons: list[LessonImageTarget] | None = None  # path 2: explicit lesson/question targets
    additional_prompt: str | None = None            # global; per-lesson value overrides this
    limit_questions: int = 200                      # max questions to process (units path)
    translate: bool = False                         # True if question_text is NOT already in English
    s3_bucket: str
    s3_prefix: str = "spanish"
    aws_region: str = "us-east-1"
    openai_api_key: str | None = None               # falls back to OPENAI_API_KEY env var
    aws_access_key_id: str | None = None            # falls back to AWS_ACCESS_KEY_ID env var
    aws_secret_access_key: str | None = None        # falls back to AWS_SECRET_ACCESS_KEY env var


class GrammarAudioTarget(BaseModel):
    lesson_id: int
    article_ids: list[int] | None = None  # if None → all articles for the lesson


class GenerateGrammarAudioRequest(BaseModel):
    db: DBConfig
    units: list[int] | None = None                    # path 1: select grammar lessons by unit number
    lessons: list[GrammarAudioTarget] | None = None   # path 2: explicit lesson/article targets
    limit_articles: int = 200                         # max articles to process (units path)
    s3_bucket: str
    s3_prefix: str = "spanish"
    aws_region: str = "us-east-1"
    openai_api_key: str | None = None                 # falls back to OPENAI_API_KEY env var
    aws_access_key_id: str | None = None              # falls back to AWS_ACCESS_KEY_ID env var
    aws_secret_access_key: str | None = None          # falls back to AWS_SECRET_ACCESS_KEY env var


class GenerateListeningQuestionsRequest(BaseModel):
    db: DBConfig
    s3_bucket: str
    s3_audio_prefix: str                              # S3 prefix where MP3 files live
    s3_output_prefix: str                             # S3 prefix where question JSONs are written
    aws_region: str = "us-east-1"
    aws_access_key_id: str | None = None
    aws_secret_access_key: str | None = None
    openai_api_key: str | None = None
    num_questions: int = 10
    model: str = "gpt-4o-mini"
    transcription_model: str = "whisper-1"
    temperature: float = 0.4
    force: bool = False                               # re-process even if output JSON already in S3
    limit: int | None = None                          # max number of audio files to process
    lesson_type: str = "listening"                    # Lesson.type value for inserted lessons
    cefr_level: str = "A1"                           # fallback when no mapping matches
    cefr_mapping: list[CefrRange] | None = None      # unit-range → CEFR level (same as other routes)


class GenerateVocabAudioRequest(BaseModel):
    db: DBConfig
    s3_bucket: str
    s3_prefix: str                                    # S3 prefix where MP3s are uploaded
    aws_region: str = "us-east-1"
    aws_access_key_id: str | None = None
    aws_secret_access_key: str | None = None
    openai_api_key: str | None = None
    limit: int | None = None                          # cap number of questions to process
    tts_model: str = "gpt-4o-mini-tts"
    voice: str = "alloy"
    tts_instructions: str | None = (
        "Speak slowly and clearly with a warm, friendly, teacher-like tone."
    )
    source_language: str = "en"                       # AWS Translate source language code
    target_language: str = "hi"                       # AWS Translate target language code (e.g. "hi", "es")


class GenerateArticleQuestionsRequest(BaseModel):
    db: DBConfig
    s3_bucket: str
    s3_output_prefix: str                             # S3 prefix where question JSONs are written
    aws_region: str = "us-east-1"
    aws_access_key_id: str | None = None
    aws_secret_access_key: str | None = None
    openai_api_key: str | None = None
    num_questions: int = 10
    model: str = "gpt-4o-mini"
    temperature: float = 0.4
    force: bool = False                               # re-process even if output JSON already in S3
    limit: int | None = None                          # max number of lessons to process
    lesson_type: str | None = None                   # generate for all lessons of this type
    lesson_ids: list[int] | None = None              # generate for specific lesson IDs


class GenerateUnitImagesRequest(BaseModel):
    db: DBConfig
    s3_bucket: str
    s3_prefix: str                                    # e.g. "spanish/unit_images"
    aws_region: str = "us-east-1"
    aws_access_key_id: str | None = None
    aws_secret_access_key: str | None = None
    openai_api_key: str | None = None
    units: list[int] | None = None                   # specific unit numbers; None = auto-detect all
    force: bool = False                               # re-generate even if image already exists in S3
    image_model: str = "gpt-image-1"
    theme_model: str = "gpt-4o-mini"
    temperature: float = 0.4


class LinkGrammarVideosRequest(BaseModel):
    db: DBConfig
    s3_bucket: str
    s3_prefix: str                                    # prefix under which .mp4 files live
    aws_region: str = "us-east-1"
    aws_access_key_id: str | None = None
    aws_secret_access_key: str | None = None
    force: bool = False                               # re-insert even if VideoLesson row already exists


class IngestUnitImagesRequest(BaseModel):
    db: DBConfig
    s3_bucket: str
    s3_prefix: str                                    # prefix under which unit .png files live
    aws_region: str = "us-east-1"
    aws_access_key_id: str | None = None
    aws_secret_access_key: str | None = None
    force: bool = False                               # re-insert even if UnitImages row already exists
    lesson_ids: list[int] | None = None              # if provided, link existing unit images to these lessons only


class BackfillAnswerTextRequest(BaseModel):
    db: DBConfig
    folder: str | None = None       # path to directory containing JSON files
    filename: str | None = None     # single JSON file path
    limit: int | None = None        # max number of files to process


class ReplaceSpeakingArticlesRequest(BaseModel):
    db: DBConfig
    folder: str
    limit: int | None = None


class GenerateWritingLessonsRequest(BaseModel):
    db: DBConfig
    units: list[int]
    num_questions: int = 5
    model: str = "gpt-4o-mini"
    temperature: float = 0.7
    openai_api_key: str | None = None
    cefr_mapping: list[CefrRange] | None = None   # unit-range → CEFR level; overrides DB value
    cefr_level: str = "A1"                         # fallback if no mapping and no DB value
    force: bool = False                             # re-generate even if writing lesson already exists


class ConvertToListeningRequest(BaseModel):
    db: DBConfig
    lesson_ids: list[int]
    s3_bucket: str
    s3_prefix: str                                    # e.g. "spanish/listening_audio"
    aws_region: str = "us-east-1"
    aws_access_key_id: str | None = None
    aws_secret_access_key: str | None = None
    openai_api_key: str | None = None                 # falls back to OPENAI_API_KEY env var


# --- Helpers ---

def connect_to_db(config: DBConfig, use_database: bool = True):
    """Open a PyMySQL connection. If use_database=False, connects without selecting a DB."""
    kwargs = dict(
        host=config.host,
        user=config.user,
        password=config.password,
        cursorclass=pymysql.cursors.DictCursor,
    )
    if use_database:
        kwargs["database"] = config.database
    return pymysql.connect(**kwargs)


def resolve_cefr(filename: str, mapping: list[CefrRange] | None, fallback: str) -> str:
    """Extract unit number from filename and return the matching CEFR level."""
    if mapping:
        match = re.search(r"^unit(\d+)", filename, re.IGNORECASE)
        if match:
            unit = int(match.group(1))
            for r in mapping:
                if r.min <= unit <= r.max:
                    return r.cefr_level
    return fallback


def make_create_if_not_exists(ddl: str) -> str:
    """Transform 'CREATE TABLE `name`' → 'CREATE TABLE IF NOT EXISTS `name`'."""
    return re.sub(
        r"CREATE TABLE\s+(`[^`]+`|\S+)",
        r"CREATE TABLE IF NOT EXISTS \1",
        ddl,
        count=1,
        flags=re.IGNORECASE,
    )


# --- Routes ---

@app.get("/test")
def test():
    return {"status": "ok"}


@app.post("/clone-schema")
def clone_schema(body: CloneSchemaRequest):
    """
    Reads all table DDLs from the source MySQL database and recreates them
    in the destination database (created if it doesn't exist). No data is copied.
    """
    # 1. Connect to source and collect DDLs
    try:
        src_conn = connect_to_db(body.source, use_database=True)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Could not connect to source database: {e}")

    table_ddls: dict[str, str] = {}
    try:
        with src_conn.cursor() as cur:
            cur.execute("SHOW TABLES")
            rows = cur.fetchall()
            # Result rows look like: {"Tables_in_<db>": "table_name"}
            table_names = [list(row.values())[0] for row in rows]

            for table in table_names:
                cur.execute(f"SHOW CREATE TABLE `{table}`")
                result = cur.fetchone()
                # Key is either "Create Table" or "Create View"
                ddl = result.get("Create Table") or result.get("Create View", "")
                table_ddls[table] = make_create_if_not_exists(ddl)
    except Exception as e:
        src_conn.close()
        raise HTTPException(status_code=500, detail=f"Failed to read source schema: {e}")
    finally:
        src_conn.close()

    # 2. Connect to destination host (no database selected yet)
    try:
        dest_conn = connect_to_db(body.dest, use_database=False)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Could not connect to destination host: {e}")

    cloned_tables = []
    errors = []
    dest_db = body.dest.database

    try:
        with dest_conn.cursor() as cur:
            # 3. Create destination database if it doesn't exist
            cur.execute(f"CREATE DATABASE IF NOT EXISTS `{dest_db}`")
            cur.execute(f"USE `{dest_db}`")

            # 4. Disable FK checks so tables can be created regardless of reference order
            cur.execute("SET FOREIGN_KEY_CHECKS=0")

            # 5. Create each table
            for table, ddl in table_ddls.items():
                try:
                    cur.execute(ddl)
                    cloned_tables.append(table)
                except Exception as table_err:
                    errors.append({"table": table, "error": str(table_err)})

            cur.execute("SET FOREIGN_KEY_CHECKS=1")

        dest_conn.commit()
    except Exception as e:
        dest_conn.close()
        raise HTTPException(status_code=500, detail=f"Failed to create destination schema: {e}\n{traceback.format_exc()}")
    finally:
        dest_conn.close()

    response = {
        "status": "ok" if not errors else "partial",
        "created_database": dest_db,
        "tables_cloned": cloned_tables,
        "tables_total": len(table_ddls),
    }
    if errors:
        response["errors"] = errors

    return JSONResponse(content=response, status_code=200 if not errors else 207)


@app.post("/insert-lessons")
def insert_lessons(body: InsertLessonsRequest):
    """
    Reads all JSON files from the given folder and inserts each into
    Lesson, Article, Question, and Answer tables. Each file is its own
    transaction — a failure in one file does not affect others.
    """
    if not body.folder and not body.files:
        raise HTTPException(status_code=400, detail="Provide at least one of 'folder' or 'files'")

    json_files: list[Path] = []

    if body.folder:
        folder = Path(body.folder)
        if not folder.is_dir():
            raise HTTPException(status_code=400, detail=f"Folder not found: {body.folder}")
        json_files.extend(sorted(folder.glob("*.json")))

    if body.files:
        for f in body.files:
            p = Path(f)
            if not p.is_file():
                raise HTTPException(status_code=400, detail=f"File not found: {f}")
            if p not in json_files:
                json_files.append(p)

    if not json_files:
        raise HTTPException(status_code=400, detail="No JSON files found")

    try:
        conn = connect_to_db(body.db)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Could not connect to database: {e}")

    files_processed = []
    files_failed = []
    total_lessons = total_articles = total_questions = total_answers = 0

    for json_file in json_files:
        file_log = {
            "file": json_file.name,
            "status": None,
            "lesson_id": None,
            "articles_inserted": 0,
            "questions_inserted": 0,
            "answers_inserted": 0,
            "errors": [],
        }

        # --- Parse JSON ---
        try:
            raw_data = json.loads(json_file.read_text(encoding="utf-8-sig"))
        except Exception as e:
            file_log["status"] = "failed"
            file_log["errors"].append(f"JSON parse error: {e}")
            files_failed.append(file_log)
            continue

        # Support both a single lesson object {...} and a list of lessons [...]
        lesson_entries = raw_data if isinstance(raw_data, list) else [raw_data]

        # --- Insert into DB (one transaction per file) ---
        try:
            with conn.cursor() as cur:
              for data in lesson_entries:

                # 1. Lesson
                has_question = 1 if data.get("questions_and_answers") else 0
                cefr = resolve_cefr(json_file.name, body.cefr_mapping, body.cefr_level)
                file_log["cefr_level"] = cefr
                cur.execute(
                    "INSERT INTO Lesson (title, type, cefr_level, has_question, metadata) "
                    "VALUES (%s, %s, %s, %s, %s)",
                    (
                        data["title"],
                        data["type"],
                        cefr,
                        has_question,
                        json.dumps(data.get("metadata")) if data.get("metadata") else None,
                    ),
                )
                lesson_id = cur.lastrowid
                file_log["lesson_id"] = lesson_id

                # 2. Articles
                for article in data.get("articles", []):
                    cur.execute(
                        "INSERT INTO Article (lesson_id, sequence_id, content) VALUES (%s, %s, %s)",
                        (lesson_id, article["sequence_id"], article["text"]),
                    )
                    file_log["articles_inserted"] += 1

                # 3. Questions + Answers
                for qa in data.get("questions_and_answers", []):
                    question_text = qa.get("question_text") or qa.get("question", "")

                    inferred_type = qa.get("type") or (
                        "multiple_choice" if "answers" in qa else "short_answer"
                    )
                    cur.execute(
                        "INSERT INTO Question (lesson_id, sequence_id, question_text, type, has_answer) "
                        "VALUES (%s, %s, %s, %s, %s)",
                        (
                            lesson_id,
                            qa["sequence_id"],
                            question_text,
                            inferred_type,
                            int(qa.get("has_answer", False)),
                        ),
                    )
                    question_id = cur.lastrowid
                    file_log["questions_inserted"] += 1

                    # Answers — handle all formats:
                    # A) answers[].answer_text  (vocabulary short_answer)
                    # B) flat answer + is_correct on the question  (grammar short_answer)
                    # C) answers[].answer  (multiple_choice)
                    if "answers" in qa:
                        for ans in qa["answers"]:
                            answer_text = ans.get("answer_text") or ans.get("answer") or ans.get("text", "")
                            cur.execute(
                                "INSERT INTO Answer (lesson_id, question_id, answer_text, is_correct) "
                                "VALUES (%s, %s, %s, %s)",
                                (lesson_id, question_id, answer_text, int(ans.get("is_correct", False))),
                            )
                            file_log["answers_inserted"] += 1
                    elif "answer" in qa:
                        raw = qa["answer"]
                        # Format D: answer is a list of objects e.g. [{"answer": "...", "is_correct": true}]
                        if isinstance(raw, list):
                            for ans in raw:
                                cur.execute(
                                    "INSERT INTO Answer (lesson_id, question_id, answer_text, is_correct) "
                                    "VALUES (%s, %s, %s, %s)",
                                    (lesson_id, question_id, ans.get("answer", ""), int(ans.get("is_correct", True))),
                                )
                                file_log["answers_inserted"] += 1
                        elif isinstance(raw, dict):
                            # Format E: answer is a dict with "text" and "is_correct" keys
                            cur.execute(
                                "INSERT INTO Answer (lesson_id, question_id, answer_text, is_correct) "
                                "VALUES (%s, %s, %s, %s)",
                                (lesson_id, question_id, raw.get("text") or raw.get("answer", ""), int(raw.get("is_correct", True))),
                            )
                            file_log["answers_inserted"] += 1
                        else:
                            # Format B: answer is a plain string
                            cur.execute(
                                "INSERT INTO Answer (lesson_id, question_id, answer_text, is_correct) "
                                "VALUES (%s, %s, %s, %s)",
                                (lesson_id, question_id, raw, int(qa.get("is_correct", True))),
                            )
                            file_log["answers_inserted"] += 1

            conn.commit()
            file_log["status"] = "success"
            total_lessons += 1
            total_articles += file_log["articles_inserted"]
            total_questions += file_log["questions_inserted"]
            total_answers += file_log["answers_inserted"]
            files_processed.append(file_log)

        except Exception as e:
            conn.rollback()
            file_log["status"] = "failed"
            file_log["errors"].append(str(e))
            files_failed.append(file_log)

    conn.close()

    all_succeeded = len(files_failed) == 0
    response = {
        "status": "ok" if all_succeeded else "partial",
        "summary": {
            "files_found": len(json_files),
            "files_succeeded": len(files_processed),
            "files_failed": len(files_failed),
            "lessons_inserted": total_lessons,
            "articles_inserted": total_articles,
            "questions_inserted": total_questions,
            "answers_inserted": total_answers,
        },
        "files": files_processed + files_failed,
    }
    return JSONResponse(content=response, status_code=200 if all_succeeded else 207)


# --- Image generation helpers ---

def _resize_to_256_png_bytes(raw_bytes: bytes) -> bytes:
    with PILImage.open(io.BytesIO(raw_bytes)) as im:
        im = im.convert("RGBA")
        im = im.resize((256, 256), PILImage.LANCZOS)
        out = io.BytesIO()
        im.save(out, format="PNG", optimize=True)
        return out.getvalue()


def _safe_slug(s: str) -> str:
    return "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in (s or "")).strip("_") or "item"


def _upload_to_s3_public(data_bytes: bytes, key: str, bucket: str, region: str,
                          access_key: str | None, secret_key: str | None,
                          content_type: str = "image/png") -> str:
    kwargs = {"region_name": region}
    if access_key and secret_key:
        kwargs["aws_access_key_id"] = access_key
        kwargs["aws_secret_access_key"] = secret_key
    s3 = boto3.client("s3", **kwargs)
    s3.put_object(Bucket=bucket, Key=key, Body=data_bytes, ContentType=content_type)
    return f"https://{bucket}.s3.amazonaws.com/{key}"


def _insert_image_row(conn, lesson_id: int, question_id: int,
                      image_url: str, image_metadata_json: str, sequence_id=None):
    sql = """
        INSERT INTO Image (lesson_id, question_id, sequence_id, image_url, image_metadata)
        VALUES (%s, %s, %s, %s, CAST(%s AS JSON))
    """
    with conn.cursor() as cur:
        cur.execute(sql, (lesson_id, question_id, sequence_id, image_url, image_metadata_json))


def _insert_audio_row(conn, lesson_id: int, article_id: int,
                      audio_url: str, audio_metadata_json: str, sequence_id=None):
    sql = """
        INSERT INTO Audio (lesson_id, article_id, sequence_id, audio_url, audio_metadata)
        VALUES (%s, %s, %s, %s, CAST(%s AS JSON))
    """
    with conn.cursor() as cur:
        cur.execute(sql, (lesson_id, article_id, sequence_id, audio_url, audio_metadata_json))


# --- Listening-questions helpers ---

def _make_s3_client(region: str, access_key: str | None, secret_key: str | None):
    kwargs: dict = {"region_name": region}
    if access_key and secret_key:
        kwargs["aws_access_key_id"] = access_key
        kwargs["aws_secret_access_key"] = secret_key
    return boto3.client("s3", **kwargs)


def _list_s3_mp3_keys(s3_client, bucket: str, prefix: str) -> list[str]:
    """Return sorted list of all .mp3 object keys under prefix (handles pagination)."""
    keys = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix.rstrip("/") + "/"):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.lower().endswith(".mp3"):
                keys.append(key)
    return sorted(keys)


def _s3_key_exists(s3_client, bucket: str, key: str) -> bool:
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except Exception:
        return False


def _transcribe_from_s3(s3_client, bucket: str, key: str,
                         openai_client: OpenAI, model: str) -> str:
    """Download an MP3 from S3 and return its Whisper transcription text."""
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    audio_bytes = obj["Body"].read()
    filename = Path(key).name
    # Pass as (name, file_object) tuple — accepted by all recent OpenAI SDK versions
    audio_file = (filename, io.BytesIO(audio_bytes))
    transcript = openai_client.audio.transcriptions.create(model=model, file=audio_file)
    return transcript.text


def _extract_unit_number_from_filename(filename: str) -> int | None:
    """Return the unit number found in the filename, or None.

    Handles formats like:
        Spanish Basic Course - Volume 2 - Unit 28C.mp3  →  28
        unit05_listening_a.mp3                          →  5
    """
    m = re.search(r"Unit\s+(\d+)", filename, re.IGNORECASE)
    if m:
        try:
            return int(m.group(1))
        except ValueError:
            pass
    m2 = re.search(r"(^|[^\d])(\d+)([^\d]|$)", filename)
    if m2:
        try:
            return int(m2.group(2))
        except ValueError:
            pass
    return None


def _build_listening_lesson_title(filename: str) -> str:
    """Derive a normalised lesson title from an audio filename.

    Examples
    --------
    Spanish Basic Course - Volume 2 - Unit 28C.mp3  →  unit28_listening_c
    Spanish Basic Course - Volume 1 - Unit 02A.mp3  →  unit2_listening_a
    some_other_file.mp3                              →  some_other_file  (fallback)
    """
    stem = Path(filename).stem
    m = re.search(r"Unit\s+(\d+)([A-Za-z]?)", stem, re.IGNORECASE)
    if m:
        unit_num = int(m.group(1))          # strips leading zeros (02 → 2)
        part = m.group(2).lower()           # 'a', 'b', 'c', or ''
        part_suffix = f"_{part}" if part else ""
        return f"unit{unit_num}_listening{part_suffix}"
    return stem


def _build_listening_mcq_messages(transcript_text: str, unit_number: int | None,
                                   num_questions: int) -> list[dict]:
    system = (
        "You are a careful item-writer for language learning. "
        "Create high-quality multiple-choice questions based ONLY on the provided transcript. "
        "Do not invent facts not supported by the transcript."
    )
    unit_repr = "null" if unit_number is None else str(unit_number)
    unit_type = "integer" if unit_number is not None else "null"
    user = (
        f"TRANSCRIPT (for Unit {unit_repr})\n"
        f"--------------------------------\n"
        f"{transcript_text}\n"
        f"--------------------------------\n\n"
        f"TASK:\n"
        f"- Produce {num_questions} multiple choice questions in ENGLISH derived ONLY from the transcript.\n"
        f"- Each question must have exactly 4 plausible answer choices.\n"
        f"- Include a field \"correct_answer\" as an integer index 0, 1, 2, or 3 pointing to the correct choice.\n"
        f"- Do NOT include explanations, rationales, or extra fields.\n"
        f"- Set \"unit_number\" to {unit_repr} ({unit_type} value).\n\n"
        f"OUTPUT FORMAT:\n"
        f"Return ONLY a JSON array (no surrounding prose) where each element has exactly:\n"
        f"  - \"question\" : string\n"
        f"  - \"answer_choices\" : array of exactly 4 strings\n"
        f"  - \"correct_answer\" : integer (0, 1, 2, or 3)\n"
        f"  - \"unit_number\" : {unit_repr}\n\n"
        f"IMPORTANT: valid JSON only, no code fences, exactly {num_questions} items."
    )
    return [
        {"role": "system", "content": system},
        {"role": "user", "content": user},
    ]


def _parse_mcq_json_response(content: str) -> list:
    """Parse MCQ JSON, stripping accidental code fences if present."""
    try:
        return json.loads(content)
    except json.JSONDecodeError:
        cleaned = content.strip()
        if cleaned.startswith("```"):
            cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned)
            cleaned = re.sub(r"\s*```$", "", cleaned)
        return json.loads(cleaned)


def _validate_mcq_list(data: list, num_questions: int) -> None:
    if not isinstance(data, list):
        raise ValueError("Model did not return a JSON array.")
    if len(data) != num_questions:
        raise ValueError(f"Expected {num_questions} items; got {len(data)}.")
    for i, item in enumerate(data):
        for key in ("question", "answer_choices", "correct_answer", "unit_number"):
            if key not in item:
                raise ValueError(f"Item {i} missing key: '{key}'.")
        choices = item["answer_choices"]
        if not isinstance(choices, list) or len(choices) != 4:
            raise ValueError(f"Item {i} 'answer_choices' must be a list of exactly 4 strings.")
        ca = item["correct_answer"]
        if not isinstance(ca, int) or ca < 0 or ca > 3:
            raise ValueError(f"Item {i} 'correct_answer' must be an integer 0–3.")


def _insert_listening_lesson(conn, title: str, lesson_type: str,
                              cefr_level: str, mcq_items: list,
                              audio_url: str) -> int:
    """Insert Lesson + Audio + Questions + Answers for one audio file's MCQs. Returns lesson_id."""
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO Lesson (title, type, cefr_level, has_question) VALUES (%s, %s, %s, 1)",
            (title, lesson_type, cefr_level),
        )
        lesson_id = cur.lastrowid

        # Store the source audio link
        audio_metadata = json.dumps({"source": "s3"})
        cur.execute(
            "INSERT INTO Audio (lesson_id, sequence_id, audio_url, audio_metadata) "
            "VALUES (%s, %s, %s, CAST(%s AS JSON))",
            (lesson_id, 1, audio_url, audio_metadata),
        )

        for seq, item in enumerate(mcq_items, start=1):
            cur.execute(
                "INSERT INTO Question (lesson_id, sequence_id, question_text, type, has_answer) "
                "VALUES (%s, %s, %s, 'multiple_choice', 1)",
                (lesson_id, seq, item["question"]),
            )
            question_id = cur.lastrowid
            for idx, choice_text in enumerate(item["answer_choices"]):
                cur.execute(
                    "INSERT INTO Answer (lesson_id, question_id, answer_text, is_correct) "
                    "VALUES (%s, %s, %s, %s)",
                    (lesson_id, question_id, choice_text, 1 if idx == item["correct_answer"] else 0),
                )
    return lesson_id


# --- Article-questions helpers ---

def _build_article_mcq_messages(articles_content: str, lesson_title: str,
                                  num_questions: int) -> list[dict]:
    system = (
        "You are a careful item-writer for language learning. "
        "Create high-quality multiple-choice questions based ONLY on the provided article content. "
        "Do not invent facts not supported by the articles."
    )
    user = (
        f"LESSON: {lesson_title}\n"
        f"ARTICLE CONTENT:\n"
        f"--------------------------------\n"
        f"{articles_content}\n"
        f"--------------------------------\n\n"
        f"TASK:\n"
        f"- Produce {num_questions} multiple choice questions in ENGLISH derived ONLY from the content above.\n"
        f"- Each question must have exactly 4 plausible answer choices.\n"
        f"- Include a field \"correct_answer\" as an integer index 0, 1, 2, or 3 pointing to the correct choice.\n"
        f"- Do NOT include explanations, rationales, or extra fields.\n\n"
        f"OUTPUT FORMAT:\n"
        f"Return ONLY a JSON array (no surrounding prose) where each element has exactly:\n"
        f"  - \"question\" : string\n"
        f"  - \"answer_choices\" : array of exactly 4 strings\n"
        f"  - \"correct_answer\" : integer (0, 1, 2, or 3)\n\n"
        f"IMPORTANT: valid JSON only, no code fences, exactly {num_questions} items."
    )
    return [
        {"role": "system", "content": system},
        {"role": "user", "content": user},
    ]


def _validate_article_mcq_list(data: list, num_questions: int) -> None:
    if not isinstance(data, list):
        raise ValueError("Model did not return a JSON array.")
    if len(data) != num_questions:
        raise ValueError(f"Expected {num_questions} items; got {len(data)}.")
    for i, item in enumerate(data):
        for key in ("question", "answer_choices", "correct_answer"):
            if key not in item:
                raise ValueError(f"Item {i} missing key: '{key}'.")
        choices = item["answer_choices"]
        if not isinstance(choices, list) or len(choices) != 4:
            raise ValueError(f"Item {i} 'answer_choices' must be a list of exactly 4 strings.")
        ca = item["correct_answer"]
        if not isinstance(ca, int) or ca < 0 or ca > 3:
            raise ValueError(f"Item {i} 'correct_answer' must be an integer 0–3.")


def _insert_article_lesson_questions(conn, lesson_id: int, mcq_items: list) -> None:
    """Insert Questions + Answers into an existing lesson and set has_question=1."""
    with conn.cursor() as cur:
        cur.execute("UPDATE Lesson SET has_question = 1 WHERE lesson_id = %s", (lesson_id,))
        for seq, item in enumerate(mcq_items, start=1):
            cur.execute(
                "INSERT INTO Question (lesson_id, sequence_id, question_text, type, has_answer) "
                "VALUES (%s, %s, %s, 'multiple_choice', 1)",
                (lesson_id, seq, item["question"]),
            )
            question_id = cur.lastrowid
            for idx, choice_text in enumerate(item["answer_choices"]):
                cur.execute(
                    "INSERT INTO Answer (lesson_id, question_id, answer_text, is_correct) "
                    "VALUES (%s, %s, %s, %s)",
                    (lesson_id, question_id, choice_text, 1 if idx == item["correct_answer"] else 0),
                )


# --- Unit-image helpers ---

def _extract_unit_num_from_title(title: str) -> int | None:
    """Return the unit number from a lesson title like 'unit14_grammar', or None."""
    m = re.match(r"^unit(\d+)", title, re.IGNORECASE)
    return int(m.group(1)) if m else None


def _s3_unit_image_exists(s3_client, bucket: str, prefix: str, unit_number: int) -> bool:
    """Return True if any object at prefix/ starts with unit{n}_."""
    search_prefix = f"{prefix.rstrip('/')}/unit{unit_number}_"
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=search_prefix):
        if page.get("Contents"):
            return True
    return False


def _build_unit_theme_messages(content_summary: str, unit_number: int) -> list[dict]:
    system = "You are a curriculum analyst for a language learning app."
    user = (
        f"Below is content from Unit {unit_number} of a language course, "
        f"including lesson articles and vocabulary Q&A.\n\n"
        f"CONTENT:\n"
        f"--------------------------------\n"
        f"{content_summary}\n"
        f"--------------------------------\n\n"
        f"TASK: Identify 1 to 3 words that best capture the central THEME of this unit "
        f"(e.g. 'greetings', 'family home', 'food market'). "
        f"Output ONLY the theme words separated by spaces, nothing else. No punctuation."
    )
    return [{"role": "system", "content": system}, {"role": "user", "content": user}]


# --- Vocab-audio helpers ---

# Vocabulary questions that have no Audio row yet (joined on question_id)
_VOCAB_NO_AUDIO_SQL = """
    SELECT
        q.question_id,
        q.lesson_id,
        q.sequence_id,
        q.question_text,
        l.title AS lesson_title,
        a.answer_text
    FROM Question q
    JOIN Lesson l ON q.lesson_id = l.lesson_id
    LEFT JOIN Answer a ON a.question_id = q.question_id AND a.is_correct = 1
    LEFT JOIN Audio au ON au.question_id = q.question_id
    WHERE l.type = 'vocabulary'
      AND au.question_id IS NULL
    ORDER BY q.lesson_id, q.sequence_id
"""


def _generate_tts_bytes(openai_client: OpenAI, text: str,
                         model: str, voice: str, instructions: str | None) -> bytes:
    """Call OpenAI TTS and return raw MP3 bytes (fully in-memory, no disk I/O)."""
    kwargs: dict = {"model": model, "voice": voice, "input": text}
    if instructions:
        kwargs["instructions"] = instructions
    return openai_client.audio.speech.create(**kwargs).content


def _translate_text(translate_client, text: str, source_lang: str, target_lang: str) -> str:
    """Translate text using AWS Translate and return the translated string."""
    response = translate_client.translate_text(
        Text=text,
        SourceLanguageCode=source_lang,
        TargetLanguageCode=target_lang,
    )
    return response["TranslatedText"]


def _insert_vocab_audio_row(conn, lesson_id: int, question_id: int,
                             sequence_id: int, audio_url: str,
                             tts_model: str, voice: str) -> None:
    """Insert one Audio row keyed by question_id (no article_id for vocab questions).

    Falls back to an insert without audio_metadata if the column does not yet exist
    in the target database (schema version mismatch). Use /migrate-audio-table to add
    the column and avoid the fallback path.
    """
    metadata = json.dumps({"source": "tts", "tts_model": tts_model, "voice": voice})
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO Audio (lesson_id, question_id, sequence_id, audio_url, audio_metadata) "
                "VALUES (%s, %s, %s, %s, CAST(%s AS JSON))",
                (lesson_id, question_id, sequence_id, audio_url, metadata),
            )
    except pymysql.err.OperationalError as e:
        if e.args[0] == 1054:  # Unknown column — schema is missing audio_metadata
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO Audio (lesson_id, question_id, sequence_id, audio_url) "
                    "VALUES (%s, %s, %s, %s)",
                    (lesson_id, question_id, sequence_id, audio_url),
                )
        else:
            raise


# --- GrammarScriptToAudio class ---

class GrammarScriptToAudio:
    """
    Converts a grammar article into an instructional audio clip entirely in memory.
    Step 1 — generate_script(): rewrites raw content into a 100–120 word narration.
    Step 2 — generate_audio_bytes(): synthesises the script to MP3 bytes via TTS.
    """

    SCRIPT_MODEL = "gpt-5-mini"
    TTS_MODEL = "gpt-4o-mini-tts"
    TTS_VOICE = "marin"
    TTS_INSTRUCTIONS = (
        "Use a warm, friendly, teacher-like adult female voice. "
        "Speak slowly and clearly with gentle pauses. "
        "Sound encouraging, calm, and natural."
    )

    def __init__(self, api_key: str | None = None):
        self.client = OpenAI(api_key=api_key or os.getenv("OPENAI_API_KEY"))

    def generate_script(self, content: str) -> str:
        """Rewrite grammar article content into a 100–120 word instructional script."""
        prompt = (
            "You are a helpful educator. Rewrite the following into a clear, friendly, "
            "instructional script that is between 100 and 120 words. Keep it positive, "
            "encouraging, and easy to follow. Do not include any extra commentary, only the script. "
            "Make sure it is smooth and natural, as it will be translated to speech."
            "Make sure the script is direct and to the point without any introduction or preamble. "
            "This will be used on a Duolingo-style app but for working professionals, "
            "so the script should be to the point and get the information across quickly and directly.\n\n"
            f"Content:\n{content}\n\n"
            "Output:"
        )
        response = self.client.responses.create(model=self.SCRIPT_MODEL, input=prompt)
        return response.output_text.strip()

    def generate_audio_bytes(self, script: str) -> bytes:
        """Synthesise script to MP3 and return raw bytes (no disk I/O)."""
        response = self.client.audio.speech.create(
            model=self.TTS_MODEL,
            voice=self.TTS_VOICE,
            input=script,
            instructions=self.TTS_INSTRUCTIONS,
        )
        return response.content


# --- VocabToPictures class ---

class VocabToPictures:
    """Generate images for vocabulary words using OpenAI, return bytes (no local save)."""

    def __init__(self, api_key: str | None = None, model: str = "gpt-image-1", size: str = "1024x1024"):
        self.client = OpenAI(api_key=api_key or os.getenv("OPENAI_API_KEY"))
        self.model = model
        self.size = size

    def generate_one(self, word: str, translate: bool = False,
                     additional_prompt: str | None = None) -> dict | None:
        """
        Generate a single image for `word`.
        If translate=True, first translates the word to English via GPT-4o-mini.
        additional_prompt is appended to the image generation prompt if provided.
        Returns dict with image_bytes and metadata, or None on failure.
        """
        try:
            if translate:
                resp = self.client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[{"role": "user", "content": (
                        "Here is a word or phrase from a language learning textbook's vocabulary section. "
                        "Respond with ONLY the English translation. No leading or trailing text.\n\n"
                        f"Vocab:\n{word}\n\nTranslation:"
                    )}],
                    temperature=0,
                )
                english = (resp.choices[0].message.content or "").strip()
                if not english:
                    raise RuntimeError("Translation returned empty")
            else:
                english = word

            gen_prompt = (
                "Render the following word or phrase in a realistic style for a language learning app. "
                "Keep it simple and contain **no text**. The word or phrase:\n"
                f"'{english}'"
            )
            if additional_prompt:
                gen_prompt += f"\n\n{additional_prompt}"

            result = self.client.images.generate(model=self.model, prompt=gen_prompt, size=self.size)

            if result.data and hasattr(result.data[0], "b64_json") and result.data[0].b64_json:
                return {
                    "image_bytes": base64.b64decode(result.data[0].b64_json),
                    "original_text": word,
                    "translated_text": english,
                    "model": self.model,
                    "requested_size": self.size,
                }

        except openai.PermissionDeniedError:
            print(f"OpenAI permission denied. Skipping: {word}")
        except Exception as e:
            print(f"generate_one failed for '{word}': {e}")

        return None


# --- Route ---

@app.post("/generate-lesson-images")
def generate_lesson_images(body: GenerateImagesRequest):
    """
    Generates images for vocabulary lesson questions and stores them in S3 + Image table.

    Two input modes (can combine both):
      - units: auto-selects all vocab lessons for those unit numbers (skips questions that
               already have images)
      - lessons: explicit list of {lesson_id, question_ids?, additional_prompt?}
                 If lesson is vocabulary and no question_ids given → all questions
                 If question_ids given → only those questions
    """
    if not body.units and not body.lessons:
        raise HTTPException(status_code=400, detail="Provide at least one of 'units' or 'lessons'")

    try:
        conn = connect_to_db(body.db)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Could not connect to database: {e}")

    # --- Build work list ---
    # Each item: {lesson_id, question_id, question_text, lesson_title, additional_prompt}
    work_items: list[dict] = []

    try:
        with conn.cursor() as cur:

            # Path 1: units → all vocab lessons, questions without an existing image
            if body.units:
                like_clauses = " OR ".join(["l.title LIKE %s" for _ in body.units])
                like_params = [f"unit{u}%" for u in body.units]
                cur.execute(f"""
                    SELECT l.lesson_id, l.title, q.question_id, q.question_text
                    FROM Lesson l
                    JOIN Question q ON q.lesson_id = l.lesson_id
                    LEFT JOIN Image i ON i.lesson_id = l.lesson_id AND i.question_id = q.question_id
                    WHERE ({like_clauses})
                      AND l.type = 'vocabulary'
                      AND i.image_id IS NULL
                    LIMIT %s
                """, (*like_params, body.limit_questions))
                for row in cur.fetchall():
                    work_items.append({
                        "lesson_id": row["lesson_id"],
                        "question_id": row["question_id"],
                        "question_text": row["question_text"],
                        "lesson_title": row["title"],
                        "additional_prompt": body.additional_prompt,
                    })

            # Path 2: explicit lesson targets
            if body.lessons:
                for target in body.lessons:
                    cur.execute("SELECT lesson_id, title, type FROM Lesson WHERE lesson_id = %s",
                                (target.lesson_id,))
                    lesson_row = cur.fetchone()
                    if not lesson_row:
                        work_items.append({
                            "lesson_id": target.lesson_id,
                            "error": f"Lesson {target.lesson_id} not found",
                        })
                        continue

                    effective_prompt = target.additional_prompt or body.additional_prompt
                    is_vocab = lesson_row["type"] == "vocabulary"

                    if target.question_ids:
                        # Explicit question IDs
                        fmt = ",".join(["%s"] * len(target.question_ids))
                        cur.execute(
                            f"SELECT question_id, question_text FROM Question "
                            f"WHERE lesson_id = %s AND question_id IN ({fmt})",
                            (target.lesson_id, *target.question_ids),
                        )
                    elif is_vocab:
                        # Vocab lesson, no IDs specified → all questions
                        cur.execute(
                            "SELECT question_id, question_text FROM Question WHERE lesson_id = %s",
                            (target.lesson_id,),
                        )
                    else:
                        work_items.append({
                            "lesson_id": target.lesson_id,
                            "error": f"Lesson {target.lesson_id} is type '{lesson_row['type']}' — "
                                     "specify question_ids explicitly for non-vocabulary lessons",
                        })
                        continue

                    for qrow in cur.fetchall():
                        work_items.append({
                            "lesson_id": target.lesson_id,
                            "question_id": qrow["question_id"],
                            "question_text": qrow["question_text"],
                            "lesson_title": lesson_row["title"],
                            "additional_prompt": effective_prompt,
                        })

    except Exception as e:
        conn.close()
        raise HTTPException(status_code=500, detail=f"Failed to build work list: {e}")

    # --- Generate images ---
    generator = VocabToPictures(
        api_key=body.openai_api_key,
        model="gpt-image-1",
        size="1024x1024",
    )

    results = []
    total_succeeded = total_failed = 0

    for item in work_items:
        # Items without question_id are pre-flagged errors (e.g. lesson not found)
        if "error" in item:
            results.append({
                "lesson_id": item["lesson_id"],
                "status": "failed",
                "error": item["error"],
            })
            total_failed += 1
            continue

        log = {
            "lesson_id": item["lesson_id"],
            "question_id": item["question_id"],
            "lesson_title": item["lesson_title"],
            "question_text": item["question_text"],
            "status": None,
            "image_url": None,
            "error": None,
        }

        try:
            gen = generator.generate_one(
                word=item["question_text"],
                translate=body.translate,
                additional_prompt=item["additional_prompt"],
            )
            if not gen or not gen.get("image_bytes"):
                raise RuntimeError("No image returned from generator")

            png256 = _resize_to_256_png_bytes(gen["image_bytes"])

            key = (f"{body.s3_prefix}/"
                   f"{item['lesson_id']}_{item['question_id']}_"
                   f"{_safe_slug(item['lesson_title'])}.png")

            public_url = _upload_to_s3_public(
                png256, key, body.s3_bucket, body.aws_region,
                body.aws_access_key_id, body.aws_secret_access_key,
            )

            meta = {
                "original_text": gen["original_text"],
                "translated_text": gen["translated_text"],
                "model": gen["model"],
                "requested_size": gen["requested_size"],
                "final_size": "256x256",
                "s3_bucket": body.s3_bucket,
                "s3_key": key,
                "public_url": public_url,
                "lesson_title": item["lesson_title"],
            }
            _insert_image_row(
                conn,
                lesson_id=item["lesson_id"],
                question_id=item["question_id"],
                image_url=public_url,
                image_metadata_json=json.dumps(meta, ensure_ascii=False),
            )
            conn.commit()

            log["status"] = "success"
            log["image_url"] = public_url
            total_succeeded += 1

        except Exception as e:
            conn.rollback()
            log["status"] = "failed"
            log["error"] = str(e)
            total_failed += 1

        results.append(log)

    conn.close()

    all_ok = total_failed == 0
    return JSONResponse(
        content={
            "status": "ok" if all_ok else "partial",
            "summary": {
                "total_items": len(work_items),
                "succeeded": total_succeeded,
                "failed": total_failed,
            },
            "results": results,
        },
        status_code=200 if all_ok else 207,
    )


@app.post("/generate-grammar-audio")
def generate_grammar_audio(body: GenerateGrammarAudioRequest):
    """
    Generates instructional audio for grammar lesson articles and stores them in S3 + Audio table.

    Two input modes (can combine both):
      - units: auto-selects all grammar articles for those unit numbers (skips articles that
               already have audio)
      - lessons: explicit list of {lesson_id, article_ids?}
                 If article_ids is omitted → all articles for that lesson
    """
    if not body.units and not body.lessons:
        raise HTTPException(status_code=400, detail="Provide at least one of 'units' or 'lessons'")

    try:
        conn = connect_to_db(body.db)
    except Exception as e:
        print(traceback.format_exc())
        raise HTTPException(status_code=400, detail=f"Could not connect to database: {e}")

    # --- Build work list ---
    # Each item: {lesson_id, article_id, content, lesson_title, sequence_id}
    work_items: list[dict] = []

    try:
        with conn.cursor() as cur:

            # Path 1: units → all grammar articles
            if body.units:
                regexp_clauses = " OR ".join(["l.title REGEXP %s" for _ in body.units])
                regexp_params = [f"^unit{u}_" for u in body.units]
                cur.execute(f"""
                    SELECT a.article_id, a.lesson_id, a.sequence_id, a.content, l.title
                    FROM Article a
                    JOIN Lesson l ON l.lesson_id = a.lesson_id
                    WHERE ({regexp_clauses})
                      AND l.type = 'grammar'
                    LIMIT %s
                """, (*regexp_params, body.limit_articles))
                for row in cur.fetchall():
                    work_items.append({
                        "lesson_id": row["lesson_id"],
                        "article_id": row["article_id"],
                        "sequence_id": row["sequence_id"],
                        "content": row["content"],
                        "lesson_title": row["title"],
                    })

            # Path 2: explicit lesson targets
            if body.lessons:
                for target in body.lessons:
                    cur.execute(
                        "SELECT lesson_id, title, type FROM Lesson WHERE lesson_id = %s",
                        (target.lesson_id,),
                    )
                    lesson_row = cur.fetchone()
                    if not lesson_row:
                        work_items.append({
                            "lesson_id": target.lesson_id,
                            "error": f"Lesson {target.lesson_id} not found",
                        })
                        continue

                    if lesson_row["type"] != "grammar":
                        work_items.append({
                            "lesson_id": target.lesson_id,
                            "error": (
                                f"Lesson {target.lesson_id} is type '{lesson_row['type']}' — "
                                "only grammar lessons are supported by this route"
                            ),
                        })
                        continue

                    if target.article_ids:
                        fmt = ",".join(["%s"] * len(target.article_ids))
                        cur.execute(
                            f"SELECT article_id, sequence_id, content FROM Article "
                            f"WHERE lesson_id = %s AND article_id IN ({fmt})",
                            (target.lesson_id, *target.article_ids),
                        )
                    else:
                        cur.execute(
                            "SELECT article_id, sequence_id, content FROM Article WHERE lesson_id = %s",
                            (target.lesson_id,),
                        )

                    for arow in cur.fetchall():
                        work_items.append({
                            "lesson_id": target.lesson_id,
                            "article_id": arow["article_id"],
                            "sequence_id": arow["sequence_id"],
                            "content": arow["content"],
                            "lesson_title": lesson_row["title"],
                        })

    except Exception as e:
        conn.close()
        raise HTTPException(status_code=500, detail=f"Failed to build work list: {e}")

    # --- Generate audio ---
    generator = GrammarScriptToAudio(api_key=body.openai_api_key)

    results = []
    total_succeeded = total_failed = 0

    for item in work_items:
        # Items without article_id are pre-flagged errors (e.g. lesson not found)
        if "error" in item:
            results.append({
                "lesson_id": item["lesson_id"],
                "status": "failed",
                "error": item["error"],
            })
            total_failed += 1
            continue

        log = {
            "lesson_id": item["lesson_id"],
            "article_id": item["article_id"],
            "lesson_title": item["lesson_title"],
            "status": None,
            "audio_url": None,
            "error": None,
        }

        try:
            # 1) Rewrite content into instructional script
            script = generator.generate_script(item["content"])

            # 2) Synthesise to MP3 bytes (no disk I/O)
            audio_bytes = generator.generate_audio_bytes(script)

            # 3) Upload to S3
            key = (
                f"{body.s3_prefix}/"
                f"{item['lesson_id']}_{item['article_id']}_"
                f"{_safe_slug(item['lesson_title'])}.mp3"
            )
            public_url = _upload_to_s3_public(
                audio_bytes, key, body.s3_bucket, body.aws_region,
                body.aws_access_key_id, body.aws_secret_access_key,
                content_type="audio/mpeg",
            )

            log["status"] = "success"
            log["audio_url"] = public_url
            total_succeeded += 1

        except Exception as e:
            log["status"] = "failed"
            log["error"] = str(e)
            total_failed += 1

        results.append(log)

    conn.close()

    all_ok = total_failed == 0
    return JSONResponse(
        content={
            "status": "ok" if all_ok else "partial",
            "summary": {
                "total_items": len(work_items),
                "succeeded": total_succeeded,
                "failed": total_failed,
            },
            "results": results,
        },
        status_code=200 if all_ok else 207,
    )


@app.post("/generate-listening-questions")
def generate_listening_questions(body: GenerateListeningQuestionsRequest):
    """
    For each MP3 in s3_bucket/s3_audio_prefix/:
      1. Transcribes the audio via OpenAI Whisper.
      2. Generates MCQ questions from the transcript via GPT.
      3. Uploads the question JSON to s3_bucket/s3_output_prefix/<stem>.json.
      4. Inserts Lesson + Questions + Answers into the database.

    Files whose output JSON already exists in S3 are skipped unless force=True.
    Use limit to cap how many new files are processed in a single call.
    """
    effective_api_key = body.openai_api_key or os.getenv("OPENAI_API_KEY")
    if not effective_api_key:
        raise HTTPException(status_code=400, detail="OpenAI API key is required.")

    openai_client = OpenAI(api_key=effective_api_key)
    s3 = _make_s3_client(body.aws_region, body.aws_access_key_id, body.aws_secret_access_key)

    try:
        audio_keys = _list_s3_mp3_keys(s3, body.s3_bucket, body.s3_audio_prefix)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list S3 objects: {e}")

    if not audio_keys:
        raise HTTPException(
            status_code=404,
            detail=f"No .mp3 files found under s3://{body.s3_bucket}/{body.s3_audio_prefix}",
        )

    try:
        conn = connect_to_db(body.db)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Could not connect to database: {e}")

    output_prefix = body.s3_output_prefix.rstrip("/")
    results = []
    succeeded = failed = skipped = 0

    for audio_key in audio_keys:
        if body.limit is not None and (succeeded + failed) >= body.limit:
            break

        stem = Path(audio_key).stem
        out_key = f"{output_prefix}/{stem}.json"
        title = _build_listening_lesson_title(Path(audio_key).name)

        log: dict = {
            "audio_key": audio_key,
            "output_key": out_key,
            "lesson_title": title,
            "status": None,
            "lesson_id": None,
            "error": None,
        }

        # Skip if output already exists and force=False
        if not body.force and _s3_key_exists(s3, body.s3_bucket, out_key):
            log["status"] = "skipped"
            skipped += 1
            results.append(log)
            continue

        try:
            # 1. Transcribe audio from S3
            transcript = _transcribe_from_s3(
                s3, body.s3_bucket, audio_key, openai_client, body.transcription_model
            )

            # 2. Generate MCQ questions
            unit_number = _extract_unit_number_from_filename(Path(audio_key).name)
            messages = _build_listening_mcq_messages(transcript, unit_number, body.num_questions)
            resp = openai_client.chat.completions.create(
                model=body.model,
                messages=messages,
                temperature=body.temperature,
            )
            raw_content = resp.choices[0].message.content.strip()

            # 3. Parse and validate
            mcq_items = _parse_mcq_json_response(raw_content)
            _validate_mcq_list(mcq_items, body.num_questions)

            # 4. Upload JSON to S3
            s3.put_object(
                Bucket=body.s3_bucket,
                Key=out_key,
                Body=json.dumps(mcq_items, ensure_ascii=False, indent=2).encode("utf-8"),
                ContentType="application/json",
            )

            # 5. Insert Lesson + Audio + Questions + Answers into DB
            audio_url = (
                f"https://{body.s3_bucket}.s3.{body.aws_region}.amazonaws.com/{audio_key}"
            )
            cefr = resolve_cefr(Path(audio_key).name, body.cefr_mapping, body.cefr_level)
            lesson_id = _insert_listening_lesson(
                conn, title, body.lesson_type, cefr, mcq_items, audio_url
            )
            conn.commit()

            log["status"] = "success"
            log["lesson_id"] = lesson_id
            succeeded += 1

        except Exception as e:
            conn.rollback()
            log["status"] = "failed"
            log["error"] = str(e)
            failed += 1

        results.append(log)

    conn.close()

    all_ok = failed == 0
    return JSONResponse(
        content={
            "status": "ok" if all_ok else "partial",
            "summary": {
                "total_audio_files": len(audio_keys),
                "processed": succeeded + failed,
                "succeeded": succeeded,
                "failed": failed,
                "skipped": skipped,
            },
            "results": results,
        },
        status_code=200 if all_ok else 207,
    )


## TODO: make sure the spoken audio is natural for the language (DOES NOT HAVE AN ACCENT)
@app.post("/generate-vocab-audio")
def generate_vocab_audio(body: GenerateVocabAudioRequest):
    """
    Finds all vocabulary questions that have no Audio row (matched on question_id),
    For each vocabulary question:
      1. Check the existing answer text.
         - If blank/missing: translate question_text (English → target_language) via AWS Translate
           and save the translation back to the Answer table.
         - If populated: use as-is.
      2. Generate TTS audio from the answer text.
      3. Upload the MP3 to S3.
      4. Insert an Audio row into the database.

    Each question is processed independently — a failure on one does not block the rest.
    """
    effective_api_key = body.openai_api_key or os.getenv("OPENAI_API_KEY")
    if not effective_api_key:
        raise HTTPException(status_code=400, detail="OpenAI API key is required.")

    openai_client = OpenAI(api_key=effective_api_key)
    s3 = _make_s3_client(body.aws_region, body.aws_access_key_id, body.aws_secret_access_key)

    # AWS Translate client (shares same credentials as S3)
    translate_kwargs: dict = {"region_name": body.aws_region}
    if body.aws_access_key_id and body.aws_secret_access_key:
        translate_kwargs["aws_access_key_id"] = body.aws_access_key_id
        translate_kwargs["aws_secret_access_key"] = body.aws_secret_access_key
    translate_client = boto3.client("translate", **translate_kwargs)

    try:
        conn = connect_to_db(body.db)
    except Exception as e:
        print(traceback.format_exc())
        raise HTTPException(status_code=400, detail=f"Could not connect to database: {e}")

    # Fetch vocab questions that are missing audio
    sql = _VOCAB_NO_AUDIO_SQL
    if body.limit is not None:
        sql += f" LIMIT {int(body.limit)}"

    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
    except Exception as e:
        conn.close()
        raise HTTPException(status_code=500, detail=f"Failed to query vocab questions: {e}")

    if not rows:
        conn.close()
        return JSONResponse(content={
            "status": "ok",
            "message": "No vocabulary questions without audio found.",
            "summary": {"total_found": 0, "succeeded": 0, "failed": 0},
            "results": [],
        })

    prefix = body.s3_prefix.rstrip("/")
    results = []
    succeeded = failed = 0

    for row in rows:
        question_id   = row["question_id"]
        lesson_id     = row["lesson_id"]
        sequence_id   = row["sequence_id"]
        question_text = row["question_text"]
        lesson_title  = row["lesson_title"]
        answer_text   = row.get("answer_text") or ""

        safe_title = _safe_slug(lesson_title)
        s3_key    = f"{prefix}/{lesson_id}_{question_id}_{safe_title}.mp3"
        audio_url = f"https://{body.s3_bucket}.s3.{body.aws_region}.amazonaws.com/{s3_key}"

        log: dict = {
            "question_id":   question_id,
            "lesson_id":     lesson_id,
            "lesson_title":  lesson_title,
            "question_text": question_text,
            "answer_text":   answer_text or None,
            "translated":    False,
            "audio_url":     None,
            "status":        None,
            "error":         None,
        }

        try:
            # 1. Resolve the text to speak — translate if answer is blank
            if not answer_text.strip():
                translated = _translate_text(
                    translate_client, question_text,
                    body.source_language, body.target_language,
                )
                # Persist the translation back to the Answer table
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT COUNT(*) AS cnt FROM Answer WHERE question_id = %s",
                        (question_id,),
                    )
                    has_row = cur.fetchone()["cnt"] > 0

                    if has_row:
                        cur.execute(
                            "UPDATE Answer SET answer_text = %s WHERE question_id = %s AND is_correct = 1",
                            (translated, question_id),
                        )
                    else:
                        cur.execute(
                            "INSERT INTO Answer (lesson_id, question_id, answer_text, is_correct) "
                            "VALUES (%s, %s, %s, 1)",
                            (lesson_id, question_id, translated),
                        )

                answer_text = translated
                log["answer_text"] = translated
                log["translated"] = True

            # 2. Generate TTS audio from the answer text
            audio_bytes = _generate_tts_bytes(
                openai_client, answer_text,
                body.tts_model, body.voice, body.tts_instructions,
            )

            # 3. Upload MP3 to S3
            s3.put_object(
                Bucket=body.s3_bucket,
                Key=s3_key,
                Body=audio_bytes,
                ContentType="audio/mpeg",
            )

            # 4. Insert Audio row into DB
            _insert_vocab_audio_row(
                conn, lesson_id, question_id, sequence_id, audio_url,
                body.tts_model, body.voice,
            )
            conn.commit()

            log["audio_url"] = audio_url
            log["status"] = "success"
            succeeded += 1

        except Exception as e:
            conn.rollback()
            log["status"] = "failed"
            log["error"] = str(e)
            failed += 1

        results.append(log)

    conn.close()

    all_ok = failed == 0
    return JSONResponse(
        content={
            "status": "ok" if all_ok else "partial",
            "summary": {
                "total_found": len(rows),
                "succeeded": succeeded,
                "failed": failed,
            },
            "results": results,
        },
        status_code=200 if all_ok else 207,
    )


@app.post("/generate-unit-images")
def generate_unit_images(body: GenerateUnitImagesRequest):
    """
    For each unit (auto-detected from lesson titles or from the provided units list):
      1. Gathers all Article content and Q&A text from every lesson in that unit.
      2. If no content is found, skips the unit.
      3. Sends the content to GPT to derive a 1–3 word theme.
      4. Generates an image representing that theme via the image model.
      5. Uploads the image to s3_bucket/s3_prefix/unit{n}_{theme_slug}.png.

    Units that already have an image at the prefix are skipped unless force=True.
    """
    effective_api_key = body.openai_api_key or os.getenv("OPENAI_API_KEY")
    if not effective_api_key:
        raise HTTPException(status_code=400, detail="OpenAI API key is required.")

    openai_client = OpenAI(api_key=effective_api_key)
    s3 = _make_s3_client(body.aws_region, body.aws_access_key_id, body.aws_secret_access_key)

    try:
        conn = connect_to_db(body.db)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Could not connect to database: {e}")

    # --- Resolve unit numbers ---
    try:
        if body.units:
            unit_numbers = sorted(set(body.units))
        else:
            with conn.cursor() as cur:
                cur.execute("SELECT DISTINCT title FROM Lesson WHERE title REGEXP '^unit[0-9]'")
                titles = [row["title"] for row in cur.fetchall()]
            unit_numbers = sorted({
                n for t in titles
                if (n := _extract_unit_num_from_title(t)) is not None
            })
    except Exception as e:
        conn.close()
        raise HTTPException(status_code=500, detail=f"Failed to resolve unit numbers: {e}")

    prefix = body.s3_prefix.rstrip("/")
    results = []
    succeeded = failed = skipped = no_content = 0

    for unit_number in unit_numbers:
        log: dict = {
            "unit": unit_number,
            "theme": None,
            "image_url": None,
            "status": None,
            "error": None,
        }

        try:
            # 1. Skip if an image already exists in S3 and force=False
            if not body.force and _s3_unit_image_exists(s3, body.s3_bucket, prefix, unit_number):
                log["status"] = "skipped"
                skipped += 1
                results.append(log)
                continue

            # 2. Gather all content for this unit
            unit_like = f"unit{unit_number}\\_%"
            with conn.cursor() as cur:
                # Articles
                cur.execute(
                    "SELECT a.content FROM Article a "
                    "JOIN Lesson l ON l.lesson_id = a.lesson_id "
                    "WHERE l.title LIKE %s ORDER BY l.lesson_id, a.sequence_id",
                    (unit_like,),
                )
                articles = [row["content"] for row in cur.fetchall()]

                # Q&A
                cur.execute(
                    "SELECT q.question_text, ans.answer_text "
                    "FROM Question q "
                    "JOIN Lesson l ON l.lesson_id = q.lesson_id "
                    "LEFT JOIN Answer ans ON ans.question_id = q.question_id AND ans.is_correct = 1 "
                    "WHERE l.title LIKE %s ORDER BY l.lesson_id, q.sequence_id",
                    (unit_like,),
                )
                qa_rows = cur.fetchall()

            content_parts = []
            if articles:
                content_parts.append("=== Articles ===\n" + "\n\n".join(articles))
            if qa_rows:
                qa_lines = [
                    f"Q: {r['question_text']}" + (f"  A: {r['answer_text']}" if r["answer_text"] else "")
                    for r in qa_rows
                ]
                content_parts.append("=== Vocabulary Q&A ===\n" + "\n".join(qa_lines))

            if not content_parts:
                log["status"] = "skipped_no_content"
                no_content += 1
                results.append(log)
                continue

            # Truncate to avoid token limits (~6000 chars is safe for theme extraction)
            combined = "\n\n".join(content_parts)[:6000]

            # 3. Derive theme via GPT
            messages = _build_unit_theme_messages(combined, unit_number)
            theme_resp = openai_client.chat.completions.create(
                model=body.theme_model,
                messages=messages,
                temperature=body.temperature,
            )
            raw_theme = (theme_resp.choices[0].message.content or "").strip()
            # Normalise: keep only word chars, collapse spaces to underscore
            theme_slug = re.sub(r"\s+", "_", re.sub(r"[^\w\s]", "", raw_theme).strip()).lower()
            if not theme_slug:
                theme_slug = f"unit{unit_number}"

            log["theme"] = raw_theme

            # 4. Generate image
            image_prompt = (
                f"A vibrant, simple illustration representing the theme '{raw_theme}' "
                f"for a language learning app. Realistic style, no text in the image."
            )
            img_result = openai_client.images.generate(
                model=body.image_model,
                prompt=image_prompt,
                size="1024x1024",
            )
            if not img_result.data or not getattr(img_result.data[0], "b64_json", None):
                raise RuntimeError("No image data returned from image model.")
            image_bytes = base64.b64decode(img_result.data[0].b64_json)

            # 5. Upload to S3
            s3_key = f"{prefix}/unit{unit_number}_{theme_slug}.png"
            public_url = _upload_to_s3_public(
                image_bytes, s3_key, body.s3_bucket, body.aws_region,
                body.aws_access_key_id, body.aws_secret_access_key,
            )

            log["status"] = "success"
            log["image_url"] = public_url
            succeeded += 1

        except Exception as e:
            log["status"] = "failed"
            log["error"] = str(e)
            failed += 1

        results.append(log)

    conn.close()

    all_ok = failed == 0
    return JSONResponse(
        content={
            "status": "ok" if all_ok else "partial",
            "summary": {
                "total_units": len(unit_numbers),
                "succeeded": succeeded,
                "failed": failed,
                "skipped": skipped,
                "skipped_no_content": no_content,
            },
            "results": results,
        },
        status_code=200 if all_ok else 207,
    )


@app.post("/generate-article-questions")
def generate_article_questions(body: GenerateArticleQuestionsRequest):
    """
    For each lesson (filtered by lesson_type or lesson_ids):
      1. Fetches all articles for the lesson.
      2. If no articles exist, skips the lesson.
      3. Concatenates all article content and generates MCQ questions via GPT.
      4. Uploads the question JSON to s3_bucket/s3_output_prefix/<lesson_id>.json.
      5. Inserts Questions + Answers into the existing lesson in the database.

    Lessons whose output JSON already exists in S3 are skipped unless force=True.
    Requires at least one of lesson_type or lesson_ids.
    """
    if not body.lesson_type and not body.lesson_ids:
        raise HTTPException(
            status_code=400,
            detail="Provide at least one of 'lesson_type' or 'lesson_ids'.",
        )

    effective_api_key = body.openai_api_key or os.getenv("OPENAI_API_KEY")
    if not effective_api_key:
        raise HTTPException(status_code=400, detail="OpenAI API key is required.")

    openai_client = OpenAI(api_key=effective_api_key)
    s3 = _make_s3_client(body.aws_region, body.aws_access_key_id, body.aws_secret_access_key)

    try:
        conn = connect_to_db(body.db)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Could not connect to database: {e}")

    # --- Fetch target lessons ---
    try:
        with conn.cursor() as cur:
            lesson_rows: list[dict] = []

            if body.lesson_type:
                cur.execute(
                    "SELECT lesson_id, title FROM Lesson WHERE type = %s",
                    (body.lesson_type,),
                )
                lesson_rows.extend(cur.fetchall())

            if body.lesson_ids:
                fmt = ",".join(["%s"] * len(body.lesson_ids))
                cur.execute(
                    f"SELECT lesson_id, title FROM Lesson WHERE lesson_id IN ({fmt})",
                    tuple(body.lesson_ids),
                )
                lesson_rows.extend(cur.fetchall())

        # Deduplicate by lesson_id (in case lesson_type and lesson_ids overlap)
        seen: set[int] = set()
        unique_lessons: list[dict] = []
        for row in lesson_rows:
            if row["lesson_id"] not in seen:
                seen.add(row["lesson_id"])
                unique_lessons.append(row)

    except Exception as e:
        conn.close()
        raise HTTPException(status_code=500, detail=f"Failed to fetch lessons: {e}")

    output_prefix = body.s3_output_prefix.rstrip("/")
    results = []
    succeeded = failed = skipped = no_articles = has_questions = 0

    for lesson in unique_lessons:
        if body.limit is not None and succeeded >= body.limit:
            break

        lesson_id = lesson["lesson_id"]
        lesson_title = lesson["title"]
        out_key = f"{output_prefix}/{lesson_id}.json"

        log: dict = {
            "lesson_id": lesson_id,
            "lesson_title": lesson_title,
            "output_key": out_key,
            "status": None,
            "num_questions": None,
            "error": None,
        }

        try:
            # 1. Fetch articles for this lesson
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT sequence_id, content FROM Article "
                    "WHERE lesson_id = %s ORDER BY sequence_id",
                    (lesson_id,),
                )
                articles = cur.fetchall()

            if not articles:
                log["status"] = "skipped_no_articles"
                no_articles += 1
                results.append(log)
                continue

            # 2. Skip if questions already exist in the DB and force=False
            if not body.force:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT COUNT(*) AS cnt FROM Question WHERE lesson_id = %s",
                        (lesson_id,),
                    )
                    if cur.fetchone()["cnt"] > 0:
                        log["status"] = "skipped_has_questions"
                        has_questions += 1
                        results.append(log)
                        continue

            # 3. Skip if output already exists in S3 and force=False
            if not body.force and _s3_key_exists(s3, body.s3_bucket, out_key):
                log["status"] = "skipped"
                skipped += 1
                results.append(log)
                continue

            # 3. Concatenate article content
            combined_content = "\n\n".join(
                f"[Article {a['sequence_id']}]\n{a['content']}" for a in articles
            )

            # 4. Generate MCQ questions
            messages = _build_article_mcq_messages(combined_content, lesson_title, body.num_questions)
            resp = openai_client.chat.completions.create(
                model=body.model,
                messages=messages,
                temperature=body.temperature,
            )
            raw_content = resp.choices[0].message.content.strip()

            # 5. Parse and validate
            mcq_items = _parse_mcq_json_response(raw_content)
            _validate_article_mcq_list(mcq_items, body.num_questions)

            # 6. Upload JSON to S3
            s3.put_object(
                Bucket=body.s3_bucket,
                Key=out_key,
                Body=json.dumps(mcq_items, ensure_ascii=False, indent=2).encode("utf-8"),
                ContentType="application/json",
            )

            # 7. Insert Questions + Answers into the existing lesson
            _insert_article_lesson_questions(conn, lesson_id, mcq_items)
            conn.commit()

            log["status"] = "success"
            log["num_questions"] = len(mcq_items)
            succeeded += 1

        except Exception as e:
            conn.rollback()
            log["status"] = "failed"
            log["error"] = str(e)
            failed += 1

        results.append(log)

    conn.close()

    all_ok = failed == 0
    return JSONResponse(
        content={
            "status": "ok" if all_ok else "partial",
            "summary": {
                "total_lessons": len(unique_lessons),
                "succeeded": succeeded,
                "failed": failed,
                "skipped_s3_exists": skipped,
                "skipped_has_questions": has_questions,
                "skipped_no_articles": no_articles,
            },
            "results": results,
        },
        status_code=200 if all_ok else 207,
    )


@app.post("/migrate-audio-table")
def migrate_audio_table(body: DBConfig):
    """
    Adds missing columns to the Audio table for databases created from an older schema.
    Currently adds: audio_metadata JSON NULL (if absent).
    Safe to run multiple times — uses ALTER TABLE ... ADD COLUMN IF NOT EXISTS.
    """
    try:
        conn = connect_to_db(body)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Could not connect to database: {e}")

    applied = []
    already_present = []

    migrations = [
        ("audio_metadata", "ALTER TABLE Audio ADD COLUMN audio_metadata JSON NULL"),
    ]

    try:
        with conn.cursor() as cur:
            for col_name, ddl in migrations:
                cur.execute("SHOW COLUMNS FROM Audio LIKE %s", (col_name,))
                if cur.fetchone():
                    already_present.append(col_name)
                else:
                    cur.execute(ddl)
                    applied.append(col_name)
        conn.commit()
    except Exception as e:
        conn.rollback()
        conn.close()
        raise HTTPException(status_code=500, detail=f"Migration failed: {e}")
    finally:
        conn.close()

    return JSONResponse(content={
        "status": "ok",
        "database": body.database,
        "columns_added": applied,
        "columns_already_present": already_present,
    })


@app.post("/link-grammar-videos")
def link_grammar_videos(body: LinkGrammarVideosRequest):
    """
    Scans an S3 prefix for .mp4 files, extracts the lesson_id from each filename
    (the first underscore-separated segment, e.g. '3251' from
    '3251_3875_unit10_direct_clitic_pronouns_generated.mp4'), inserts a row into the
    Video table (url = public S3 URL) and a corresponding row into VideoLesson.

    Skips a file if a VideoLesson row for that lesson_id already exists, unless
    force=True.
    """
    try:
        conn = connect_to_db(body.db)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Could not connect to database: {e}")

    s3 = _make_s3_client(body.aws_region, body.aws_access_key_id, body.aws_secret_access_key)

    # List all .mp4 keys under the prefix
    prefix = body.s3_prefix.rstrip("/") + "/"
    video_keys: list[str] = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=body.s3_bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.lower().endswith(".mp4"):
                video_keys.append(key)
    video_keys.sort()

    inserted = []
    skipped = []
    errors = []

    try:
        for key in video_keys:
            filename = Path(key).name
            # Extract lesson_id from the first segment of the filename
            first_segment = filename.split("_")[0]
            if not first_segment.isdigit():
                errors.append({"key": key, "error": f"Cannot parse lesson_id from filename '{filename}'"})
                continue
            lesson_id = int(first_segment)

            try:
                with conn.cursor() as cur:
                    # Verify lesson exists and is a grammar lesson
                    cur.execute(
                        "SELECT lesson_id FROM Lesson WHERE lesson_id = %s AND type = 'grammar'",
                        (lesson_id,),
                    )
                    if not cur.fetchone():
                        errors.append({"key": key, "error": f"No grammar lesson found with lesson_id={lesson_id}"})
                        continue

                    # Check for existing VideoLesson row
                    if not body.force:
                        cur.execute(
                            "SELECT video_id FROM VideoLesson WHERE lesson_id = %s",
                            (lesson_id,),
                        )
                        if cur.fetchone():
                            skipped.append({"key": key, "lesson_id": lesson_id, "reason": "VideoLesson already exists"})
                            continue

                    video_url = f"https://{body.s3_bucket}.s3.{body.aws_region}.amazonaws.com/{key}"

                    # Insert into Video table and retrieve the auto-generated video_id
                    cur.execute("INSERT INTO Video (url) VALUES (%s)", (video_url,))
                    video_id = cur.lastrowid

                    # Insert into VideoLesson table
                    cur.execute(
                        "INSERT INTO VideoLesson (video_id, lesson_id) VALUES (%s, %s)",
                        (video_id, lesson_id),
                    )

                conn.commit()
                inserted.append({"key": key, "lesson_id": lesson_id, "video_id": video_id, "url": video_url})

            except Exception as e:
                conn.rollback()
                errors.append({"key": key, "error": str(e)})

    finally:
        conn.close()

    return JSONResponse(content={
        "status": "ok",
        "inserted": inserted,
        "skipped": skipped,
        "errors": errors,
        "summary": {
            "total_files": len(video_keys),
            "inserted": len(inserted),
            "skipped": len(skipped),
            "errors": len(errors),
        },
    })


@app.post("/ingest-unit-images")
def ingest_unit_images(body: IngestUnitImagesRequest):
    """
    Scans an S3 prefix for .png files whose names follow the pattern
    unit{N}_{slug}.png (e.g. unit10_direct_clitic_pronouns.png), inserts a row
    into the UnitImages table for each file (url = public S3 URL, unit_number = N).

    Skips a file if a UnitImages row for that unit_number already exists,
    unless force=True (in which case a new row is always inserted).
    """
    try:
        conn = connect_to_db(body.db)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Could not connect to database: {e}")

    s3 = _make_s3_client(body.aws_region, body.aws_access_key_id, body.aws_secret_access_key)

    # List all .png keys under the prefix
    prefix = body.s3_prefix.rstrip("/") + "/"
    png_keys: list[str] = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=body.s3_bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.lower().endswith(".png"):
                png_keys.append(key)
    png_keys.sort()

    results = []
    skipped = []
    errors = []

    try:
        # --- Lesson-IDs path: link existing UnitImages to specific lessons ---
        if body.lesson_ids:
            for lesson_id in body.lesson_ids:
                log: dict = {
                    "lesson_id": lesson_id,
                    "unit_number": None,
                    "status": None,
                    "listening_questions_linked": 0,
                    "reading_lessons_linked": 0,
                }
                try:
                    with conn.cursor() as cur:
                        # 1. Resolve lesson → unit number
                        cur.execute(
                            "SELECT title, type FROM Lesson WHERE lesson_id = %s",
                            (lesson_id,),
                        )
                        lesson_row = cur.fetchone()
                        if not lesson_row:
                            log["status"] = "error"
                            log["error"] = f"Lesson {lesson_id} not found"
                            errors.append(log)
                            continue

                        unit_number = _extract_unit_num_from_title(lesson_row["title"])
                        if unit_number is None:
                            log["status"] = "error"
                            log["error"] = f"Cannot parse unit number from title '{lesson_row['title']}'"
                            errors.append(log)
                            continue

                        log["unit_number"] = unit_number

                        # 2. Find the existing UnitImages entry → get image_url via Image
                        cur.execute("""
                            SELECT i.image_url
                            FROM UnitImages ui
                            JOIN Image i ON i.image_id = ui.image_id
                            WHERE ui.unit_number = %s
                            LIMIT 1
                        """, (unit_number,))
                        ui_row = cur.fetchone()
                        if not ui_row:
                            log["status"] = "skipped"
                            log["reason"] = f"No UnitImages entry found for unit {unit_number}"
                            skipped.append(log)
                            continue

                        image_url = ui_row["image_url"]
                        lesson_type = lesson_row["type"]
                        listening_linked = 0
                        reading_linked = 0

                        # 3a. Listening: one Image row per unlinked question in this lesson
                        if lesson_type == "listening":
                            cur.execute("""
                                SELECT q.question_id
                                FROM Question q
                                LEFT JOIN Image i ON i.question_id = q.question_id
                                WHERE q.lesson_id = %s
                                  AND i.image_id IS NULL
                            """, (lesson_id,))
                            for q in cur.fetchall():
                                cur.execute(
                                    "INSERT INTO Image (lesson_id, question_id, image_url) VALUES (%s, %s, %s)",
                                    (lesson_id, q["question_id"], image_url),
                                )
                                listening_linked += 1

                        # 3b. Reading: one Image + LessonImages row if lesson not already linked
                        elif lesson_type == "reading" and "assessment" not in lesson_row["title"].lower():
                            cur.execute(
                                "SELECT image_id FROM LessonImages WHERE lesson_id = %s LIMIT 1",
                                (lesson_id,),
                            )
                            if not cur.fetchone():
                                cur.execute(
                                    "SELECT MIN(question_id) AS first_question_id FROM Question WHERE lesson_id = %s",
                                    (lesson_id,),
                                )
                                fq = cur.fetchone()
                                if fq and fq["first_question_id"]:
                                    cur.execute(
                                        "INSERT INTO Image (lesson_id, question_id, image_url) VALUES (%s, %s, %s)",
                                        (lesson_id, fq["first_question_id"], image_url),
                                    )
                                    img_id = cur.lastrowid
                                    cur.execute(
                                        "INSERT INTO LessonImages (lesson_id, image_id) VALUES (%s, %s)",
                                        (lesson_id, img_id),
                                    )
                                    reading_linked += 1

                    conn.commit()
                    log["status"] = "success"
                    log["listening_questions_linked"] = listening_linked
                    log["reading_lessons_linked"] = reading_linked
                    results.append(log)

                except Exception as e:
                    conn.rollback()
                    log["status"] = "error"
                    log["error"] = str(e)
                    errors.append(log)

            return JSONResponse(content={
                "status": "ok",
                "results": results,
                "skipped": skipped,
                "errors": errors,
                "summary": {
                    "total_lessons": len(body.lesson_ids),
                    "processed": len(results),
                    "skipped": len(skipped),
                    "errors": len(errors),
                    "total_listening_questions_linked": sum(r.get("listening_questions_linked", 0) for r in results),
                    "total_reading_lessons_linked": sum(r.get("reading_lessons_linked", 0) for r in results),
                },
            })

        # --- S3 scan path: process all PNG files under the prefix ---
        for key in png_keys:
            filename = Path(key).name
            unit_number = _extract_unit_num_from_title(filename)
            if unit_number is None:
                errors.append({"key": key, "error": f"Cannot parse unit number from filename '{filename}'"})
                continue

            image_url = f"https://{body.s3_bucket}.s3.{body.aws_region}.amazonaws.com/{key}"
            unit_like = f"unit{unit_number}\\_%"

            try:
                with conn.cursor() as cur:
                    # Skip entire unit if UnitImages already has an entry and force=False
                    if not body.force:
                        cur.execute(
                            "SELECT image_id FROM UnitImages WHERE unit_number = %s",
                            (unit_number,),
                        )
                        if cur.fetchone():
                            skipped.append({"key": key, "unit_number": unit_number, "reason": "UnitImages row already exists"})
                            continue

                    first_image_id = None
                    listening_linked = 0
                    reading_linked = 0

                    # --- Listening: one Image row per unlinked question ---
                    cur.execute("""
                        SELECT q.question_id, q.lesson_id
                        FROM Question q
                        JOIN Lesson l ON l.lesson_id = q.lesson_id
                        LEFT JOIN Image i ON i.question_id = q.question_id
                        WHERE l.type = 'listening'
                          AND l.title LIKE %s
                          AND i.image_id IS NULL
                    """, (unit_like,))
                    listening_questions = cur.fetchall()

                    for q in listening_questions:
                        cur.execute(
                            "INSERT INTO Image (lesson_id, question_id, image_url) VALUES (%s, %s, %s)",
                            (q["lesson_id"], q["question_id"], image_url),
                        )
                        img_id = cur.lastrowid
                        if first_image_id is None:
                            first_image_id = img_id
                        listening_linked += 1

                    # --- Reading: one Image + LessonImages row per unlinked lesson ---
                    # Fetch the first question_id per lesson to satisfy the NOT NULL FK on Image.question_id
                    cur.execute("""
                        SELECT l.lesson_id, MIN(q.question_id) AS first_question_id
                        FROM Lesson l
                        JOIN Question q ON q.lesson_id = l.lesson_id
                        LEFT JOIN LessonImages li ON li.lesson_id = l.lesson_id
                        WHERE l.type = 'reading'
                          AND l.title LIKE %s
                          AND l.title NOT LIKE '%%assessment%%'
                          AND li.image_id IS NULL
                        GROUP BY l.lesson_id
                    """, (unit_like,))
                    reading_lessons = cur.fetchall()

                    for r in reading_lessons:
                        cur.execute(
                            "INSERT INTO Image (lesson_id, question_id, image_url) VALUES (%s, %s, %s)",
                            (r["lesson_id"], r["first_question_id"], image_url),
                        )
                        img_id = cur.lastrowid
                        if first_image_id is None:
                            first_image_id = img_id
                        cur.execute(
                            "INSERT INTO LessonImages (lesson_id, image_id) VALUES (%s, %s)",
                            (r["lesson_id"], img_id),
                        )
                        reading_linked += 1

                    # --- UnitImages: one canonical row per unit ---
                    # If no new Image rows were created (everything already linked),
                    # fall back to any existing Image already tied to this unit's lessons.
                    if first_image_id is None:
                        cur.execute("""
                            SELECT i.image_id
                            FROM Image i
                            JOIN Lesson l ON l.lesson_id = i.lesson_id
                            WHERE l.title LIKE %s
                            LIMIT 1
                        """, (unit_like,))
                        row = cur.fetchone()
                        if row:
                            first_image_id = row["image_id"]

                    if first_image_id is not None:
                        cur.execute(
                            "INSERT INTO UnitImages (image_id, unit_number) VALUES (%s, %s)",
                            (first_image_id, unit_number),
                        )

                conn.commit()
                results.append({
                    "key": key,
                    "unit_number": unit_number,
                    "unit_image_id": first_image_id,
                    "listening_questions_linked": listening_linked,
                    "reading_lessons_linked": reading_linked,
                    "url": image_url,
                })

            except Exception as e:
                conn.rollback()
                errors.append({"key": key, "unit_number": unit_number, "error": str(e)})

    finally:
        conn.close()

    return JSONResponse(content={
        "status": "ok",
        "results": results,
        "skipped": skipped,
        "errors": errors,
        "summary": {
            "total_files": len(png_keys),
            "processed": len(results),
            "skipped": len(skipped),
            "errors": len(errors),
            "total_listening_questions_linked": sum(r["listening_questions_linked"] for r in results),
            "total_reading_lessons_linked": sum(r["reading_lessons_linked"] for r in results),
        },
    })


@app.post("/convert-to-listening")
def convert_to_listening(body: ConvertToListeningRequest):
    """
    For each lesson_id in the request:
      - Skips if the lesson is already type='listening'.
      - Skips if the lesson has no articles.
      - Runs GrammarScriptToAudio on each article (rewrites content → instructional
        script → MP3 bytes), uploads the MP3 to S3, inserts an Audio row, then
        flips Lesson.type to 'listening'.
    All articles for a lesson are committed together; if any article fails the
    lesson type is NOT changed and the error is reported.
    """
    generator = GrammarScriptToAudio(api_key=body.openai_api_key)

    try:
        conn = connect_to_db(body.db)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Could not connect to database: {e}")

    s3 = _make_s3_client(body.aws_region, body.aws_access_key_id, body.aws_secret_access_key)
    results = []
    total_succeeded = total_failed = total_skipped = 0

    try:
        for lesson_id in body.lesson_ids:
            log: dict = {
                "lesson_id": lesson_id,
                "status": None,
                "original_type": None,
                "articles_combined": 0,
                "audio_urls": [],
                "error": None,
            }

            try:
                with conn.cursor() as cur:
                    # 1. Fetch the lesson
                    cur.execute(
                        "SELECT lesson_id, title, type FROM Lesson WHERE lesson_id = %s",
                        (lesson_id,),
                    )
                    lesson = cur.fetchone()

                if not lesson:
                    log["status"] = "skipped"
                    log["error"] = f"Lesson {lesson_id} not found"
                    total_skipped += 1
                    results.append(log)
                    continue

                log["original_type"] = lesson["type"]

                if lesson["type"] == "listening":
                    log["status"] = "skipped"
                    log["error"] = "Already type='listening'"
                    total_skipped += 1
                    results.append(log)
                    continue

                # 2. Fetch all articles for this lesson
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT article_id, sequence_id, content "
                        "FROM Article WHERE lesson_id = %s ORDER BY sequence_id",
                        (lesson_id,),
                    )
                    articles = cur.fetchall()

                if not articles:
                    log["status"] = "skipped"
                    log["error"] = "No articles found"
                    total_skipped += 1
                    results.append(log)
                    continue

                # 3. Combine all articles; generate audio only if not already on S3
                lesson_title = lesson["title"]
                combined_content = "\n\n".join(a["content"] for a in articles)
                first_article_id = articles[0]["article_id"]

                s3_key = (
                    f"{body.s3_prefix.rstrip('/')}/"
                    f"{lesson_id}_{_safe_slug(lesson_title)}.mp3"
                )
                audio_url = (
                    f"https://{body.s3_bucket}.s3.{body.aws_region}.amazonaws.com/{s3_key}"
                )

                if not _s3_key_exists(s3, body.s3_bucket, s3_key):
                    script = generator.generate_script(combined_content)
                    audio_bytes = generator.generate_audio_bytes(script)
                    _upload_to_s3_public(
                        audio_bytes, s3_key, body.s3_bucket, body.aws_region,
                        body.aws_access_key_id, body.aws_secret_access_key,
                        content_type="audio/mpeg",
                    )

                # Insert Audio row; fall back gracefully if article_id column is absent
                metadata = json.dumps({
                    "source": "tts",
                    "tts_model": GrammarScriptToAudio.TTS_MODEL,
                    "voice": GrammarScriptToAudio.TTS_VOICE,
                })
                try:
                    _insert_audio_row(conn, lesson_id, first_article_id, audio_url, metadata, sequence_id=1)
                except pymysql.err.OperationalError as e:
                    if e.args[0] == 1054:  # Unknown column — minimal schema (lesson_id, sequence_id, audio_url only)
                        with conn.cursor() as cur:
                            cur.execute(
                                "INSERT INTO Audio (lesson_id, sequence_id, audio_url) VALUES (%s, %s, %s)",
                                (lesson_id, 1, audio_url),
                            )
                    else:
                        raise

                audio_urls = [audio_url]

                # 4. Flip lesson type to 'listening'
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE Lesson SET type = 'listening' WHERE lesson_id = %s",
                        (lesson_id,),
                    )

                conn.commit()

                log["status"] = "success"
                log["articles_combined"] = len(articles)
                log["audio_urls"] = audio_urls
                total_succeeded += 1

            except Exception as e:
                conn.rollback()
                log["status"] = "failed"
                log["error"] = str(e)
                total_failed += 1

            results.append(log)

    finally:
        conn.close()

    all_ok = total_failed == 0
    return JSONResponse(
        content={
            "status": "ok" if all_ok else "partial",
            "summary": {
                "total": len(body.lesson_ids),
                "succeeded": total_succeeded,
                "skipped": total_skipped,
                "failed": total_failed,
            },
            "results": results,
        },
        status_code=200 if all_ok else 207,
    )


# --- Writing-lesson helpers ---

def _build_writing_messages(unit_number: int, cefr_level: str,
                             context: str, num_questions: int) -> list[dict]:
    cefr_guidance = {
        "A1": "very simple and direct. Ask the learner to write 1-2 sentences about basic, concrete topics using vocabulary from the unit.",
        "A2": "simple and guided. Ask the learner to write 3-5 sentences about familiar topics, using structures seen in the unit.",
        "B1": "moderately open-ended. Ask the learner to write a short paragraph (5-8 sentences) describing, narrating, or giving an opinion on a topic from the unit.",
        "B2": "open-ended and analytical. Ask the learner to write a paragraph or short essay (8-12 sentences) that argues a point, compares ideas, or reflects on a theme from the unit.",
        "C1": "sophisticated and discursive. Ask the learner to produce a well-structured piece (one or more paragraphs) with nuanced vocabulary and complex grammar structures related to unit themes.",
        "C2": "highly open-ended and creative. Ask the learner to write an extended, polished piece that demonstrates mastery — e.g. a critical essay, creative narrative, or formal argument based on unit themes.",
    }.get(cefr_level.upper(), "appropriate to the learner's level.")

    system = (
        "You are an expert language-learning curriculum designer specializing in writing tasks. "
        "Your writing prompts should be engaging, pedagogically sound, and grounded in the unit content provided."
    )
    user = (
        f"UNIT: {unit_number}\n"
        f"CEFR LEVEL: {cefr_level}\n\n"
        f"UNIT CONTENT (articles, questions, and answers from existing lessons):\n"
        f"--------------------------------\n"
        f"{context}\n"
        f"--------------------------------\n\n"
        f"TASK:\n"
        f"- Create {num_questions} writing prompts for a {cefr_level} learner studying this unit.\n"
        f"- Each prompt should be {cefr_guidance}\n"
        f"- Prompts must be firmly grounded in the vocabulary, grammar, and themes of the unit content above.\n"
        f"- Do NOT include sample answers, model responses, or any evaluation criteria.\n"
        f"- Write prompts in ENGLISH.\n\n"
        f"OUTPUT FORMAT:\n"
        f"Return ONLY a JSON array (no surrounding prose, no code fences) where each element has exactly:\n"
        f"  - \"question\" : string  (the writing prompt shown to the learner)\n\n"
        f"IMPORTANT: valid JSON only, exactly {num_questions} items."
    )
    return [{"role": "system", "content": system}, {"role": "user", "content": user}]


def _validate_writing_list(data: list, num_questions: int) -> None:
    if not isinstance(data, list):
        raise ValueError("Model did not return a JSON array.")
    if len(data) != num_questions:
        raise ValueError(f"Expected {num_questions} items; got {len(data)}.")
    for i, item in enumerate(data):
        if "question" not in item:
            raise ValueError(f"Item {i} missing key: 'question'.")


@app.post("/replace-speaking-articles")
def replace_speaking_articles(body: ReplaceSpeakingArticlesRequest):
    """
    For each JSON in the folder (e.g. unit19_model_conversation_cleaned.json):
      1. Extracts the unit number from the filename.
      2. Finds the single speaking lesson for that unit in the Lesson table.
      3. Deletes all existing Article rows for that lesson.
      4. Inserts new Article rows from the JSON's 'articles' array.
    """
    logger.info("replace-speaking-articles started | folder=%s limit=%s db=%s",
                body.folder, body.limit, body.db.database)

    folder = Path(body.folder)
    if not folder.is_dir():
        logger.error("Folder not found: %s", body.folder)
        raise HTTPException(status_code=400, detail=f"Folder not found: {body.folder}")

    json_files = sorted(folder.glob("*.json"))
    if not json_files:
        logger.error("No JSON files found in folder: %s", body.folder)
        raise HTTPException(status_code=400, detail="No JSON files found in folder")

    if body.limit is not None:
        json_files = json_files[: body.limit]

    logger.info("Files to process: %d", len(json_files))

    try:
        conn = connect_to_db(body.db)
        logger.info("Connected to database: %s", body.db.database)
    except Exception as e:
        logger.error("Could not connect to database: %s", e)
        raise HTTPException(status_code=400, detail=f"Could not connect to database: {e}")

    files_processed = []
    files_failed = []

    try:
        for json_file in json_files:
            logger.info("Processing file: %s", json_file.name)
            file_log = {
                "file": json_file.name,
                "status": None,
                "lesson_id": None,
                "articles_deleted": 0,
                "articles_inserted": 0,
                "errors": [],
            }

            # Extract unit number from filename (e.g. unit19_model_conversation_cleaned.json → 19)
            m = re.match(r"^unit(\d+)", json_file.name, re.IGNORECASE)
            if not m:
                msg = "Could not extract unit number from filename"
                logger.warning("[%s] %s", json_file.name, msg)
                file_log["status"] = "failed"
                file_log["errors"].append(msg)
                files_failed.append(file_log)
                continue
            unit_num = int(m.group(1))
            logger.info("[%s] Extracted unit number: %d", json_file.name, unit_num)

            # Parse JSON
            try:
                data = json.loads(json_file.read_text(encoding="utf-8-sig"))
                logger.info("[%s] JSON parsed successfully", json_file.name)
            except Exception as e:
                logger.warning("[%s] JSON parse error: %s", json_file.name, e)
                file_log["status"] = "failed"
                file_log["errors"].append(f"JSON parse error: {e}")
                files_failed.append(file_log)
                continue

            articles = data.get("articles", [])
            if not articles:
                msg = "No articles found in JSON"
                logger.warning("[%s] %s", json_file.name, msg)
                file_log["status"] = "failed"
                file_log["errors"].append(msg)
                files_failed.append(file_log)
                continue
            logger.info("[%s] Articles in JSON: %d", json_file.name, len(articles))

            try:
                with conn.cursor() as cur:
                    # Find the speaking lesson for this unit
                    cur.execute(
                        "SELECT lesson_id FROM Lesson "
                        "WHERE title LIKE %s AND type = 'speaking' LIMIT 1",
                        (f"unit{unit_num}\\_%",),
                    )
                    row = cur.fetchone()
                    if not row:
                        msg = f"No speaking lesson found for unit {unit_num}"
                        logger.warning("[%s] %s", json_file.name, msg)
                        file_log["status"] = "failed"
                        file_log["errors"].append(msg)
                        files_failed.append(file_log)
                        continue
                    lesson_id = row["lesson_id"]
                    file_log["lesson_id"] = lesson_id
                    logger.info("[%s] Found speaking lesson: lesson_id=%d", json_file.name, lesson_id)

                    # Delete existing articles
                    cur.execute("DELETE FROM Article WHERE lesson_id = %s", (lesson_id,))
                    file_log["articles_deleted"] = cur.rowcount
                    logger.info("[%s] Deleted %d existing articles for lesson_id=%d",
                                json_file.name, file_log["articles_deleted"], lesson_id)

                    # Insert new articles
                    for article in articles:
                        cur.execute(
                            "INSERT INTO Article (lesson_id, sequence_id, content) "
                            "VALUES (%s, %s, %s)",
                            (lesson_id, article["sequence_id"], article["text"]),
                        )
                        file_log["articles_inserted"] += 1
                    logger.info("[%s] Inserted %d articles for lesson_id=%d",
                                json_file.name, file_log["articles_inserted"], lesson_id)

                conn.commit()
                file_log["status"] = "success"
                files_processed.append(file_log)
                logger.info("[%s] Committed successfully", json_file.name)

            except Exception as e:
                conn.rollback()
                logger.error("[%s] DB error, rolling back: %s", json_file.name, e)
                file_log["status"] = "failed"
                file_log["errors"].append(str(e))
                files_failed.append(file_log)

    finally:
        conn.close()
        logger.info("DB connection closed")

    all_ok = len(files_failed) == 0
    logger.info("replace-speaking-articles finished | succeeded=%d failed=%d",
                len(files_processed), len(files_failed))
    return JSONResponse(
        content={
            "status": "ok" if all_ok else "partial",
            "summary": {
                "files_found": len(json_files),
                "files_succeeded": len(files_processed),
                "files_failed": len(files_failed),
                "articles_inserted": sum(f["articles_inserted"] for f in files_processed),
            },
            "files": files_processed + files_failed,
        },
        status_code=200 if all_ok else 207,
    )


@app.post("/generate-writing-lessons")
def generate_writing_lessons(body: GenerateWritingLessonsRequest):
    """
    For each unit number:
      1. Fetches all existing lessons whose title starts with 'unit{num}_'.
      2. Collects all articles, questions, and answers from those lessons as LLM context.
      3. Determines the CEFR level (from cefr_mapping if provided, else from existing DB lessons).
      4. Calls the LLM to generate writing prompts calibrated to that CEFR level.
      5. Inserts a new Lesson (type='writing'), Questions (type='writing', has_answer=0),
         and a blank Answer row for each question.
    Skips units that already have a writing lesson unless force=True.
    """
    effective_api_key = body.openai_api_key or os.getenv("OPENAI_API_KEY")
    if not effective_api_key:
        raise HTTPException(status_code=400, detail="OpenAI API key is required.")

    openai_client = OpenAI(api_key=effective_api_key)

    try:
        conn = connect_to_db(body.db)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Could not connect to database: {e}")

    results = []
    total_succeeded = total_failed = total_skipped = 0

    try:
        for unit_num in body.units:
            log: dict = {
                "unit": unit_num,
                "status": None,
                "cefr_level": None,
                "lesson_id": None,
                "questions_inserted": 0,
                "error": None,
            }

            try:
                with conn.cursor() as cur:
                    # 1. Skip if a writing lesson already exists for this unit
                    if not body.force:
                        cur.execute(
                            "SELECT lesson_id FROM Lesson WHERE title LIKE %s AND type = 'writing' LIMIT 1",
                            (f"unit{unit_num}\\_%",),
                        )
                        if cur.fetchone():
                            log["status"] = "skipped_already_exists"
                            total_skipped += 1
                            results.append(log)
                            continue

                    # 2. Fetch all existing lessons for this unit
                    cur.execute(
                        "SELECT lesson_id, title, cefr_level FROM Lesson "
                        "WHERE title LIKE %s AND type != 'writing'",
                        (f"unit{unit_num}\\_%",),
                    )
                    lessons = cur.fetchall()

                if not lessons:
                    log["status"] = "skipped_no_lessons"
                    total_skipped += 1
                    results.append(log)
                    continue

                # 3. Determine CEFR level
                if body.cefr_mapping:
                    cefr = resolve_cefr(f"unit{unit_num}_", body.cefr_mapping, body.cefr_level)
                else:
                    cefr = lessons[0]["cefr_level"] or body.cefr_level
                log["cefr_level"] = cefr

                # 4. Build context from all lessons in the unit
                context_parts: list[str] = []
                with conn.cursor() as cur:
                    for lesson in lessons:
                        lid = lesson["lesson_id"]
                        section_parts = [f"=== Lesson: {lesson['title']} ==="]

                        # Articles
                        cur.execute(
                            "SELECT sequence_id, content FROM Article "
                            "WHERE lesson_id = %s ORDER BY sequence_id",
                            (lid,),
                        )
                        for art in cur.fetchall():
                            section_parts.append(f"[Article {art['sequence_id']}]\n{art['content']}")

                        # Questions + Answers
                        cur.execute(
                            "SELECT question_id, sequence_id, question_text FROM Question "
                            "WHERE lesson_id = %s ORDER BY sequence_id",
                            (lid,),
                        )
                        for q in cur.fetchall():
                            section_parts.append(f"Q{q['sequence_id']}: {q['question_text']}")
                            cur.execute(
                                "SELECT answer_text, is_correct FROM Answer "
                                "WHERE question_id = %s ORDER BY answer_id",
                                (q["question_id"],),
                            )
                            for ans in cur.fetchall():
                                marker = "[correct]" if ans["is_correct"] else "[wrong]"
                                section_parts.append(f"  {marker} {ans['answer_text']}")

                        context_parts.append("\n".join(section_parts))

                context = "\n\n".join(context_parts)

                # 5. Generate writing prompts via LLM
                messages = _build_writing_messages(unit_num, cefr, context, body.num_questions)
                resp = openai_client.chat.completions.create(
                    model=body.model,
                    messages=messages,
                    temperature=body.temperature,
                )
                raw = resp.choices[0].message.content or ""
                writing_items = _parse_mcq_json_response(raw)
                _validate_writing_list(writing_items, body.num_questions)

                # 6. Insert writing lesson, questions, and blank answers
                lesson_title = f"unit{unit_num}_writing_{cefr.lower()}"
                with conn.cursor() as cur:
                    cur.execute(
                        "INSERT INTO Lesson (title, type, cefr_level, has_question) "
                        "VALUES (%s, 'writing', %s, 1)",
                        (lesson_title, cefr),
                    )
                    new_lesson_id = cur.lastrowid
                    log["lesson_id"] = new_lesson_id

                    for seq, item in enumerate(writing_items, start=1):
                        cur.execute(
                            "INSERT INTO Question (lesson_id, sequence_id, question_text, type, has_answer) "
                            "VALUES (%s, %s, %s, 'short_answer', 0)",
                            (new_lesson_id, seq, item["question"]),
                        )
                        question_id = cur.lastrowid
                        cur.execute(
                            "INSERT INTO Answer (lesson_id, question_id, answer_text, is_correct) "
                            "VALUES (%s, %s, '', 0)",
                            (new_lesson_id, question_id),
                        )
                        log["questions_inserted"] += 1

                conn.commit()
                log["status"] = "success"
                total_succeeded += 1

            except Exception as e:
                conn.rollback()
                log["status"] = "failed"
                log["error"] = str(e)
                total_failed += 1

            results.append(log)

    finally:
        conn.close()

    all_ok = total_failed == 0
    return JSONResponse(
        content={
            "status": "ok" if all_ok else "partial",
            "summary": {
                "units_requested": len(body.units),
                "succeeded": total_succeeded,
                "skipped": total_skipped,
                "failed": total_failed,
            },
            "results": results,
        },
        status_code=200 if all_ok else 207,
    )


@app.post("/backfill-answer-text")
def backfill_answer_text(body: BackfillAnswerTextRequest):
    """
    For each JSON file in a folder (or a single file), extract the lesson_id from
    the filename prefix (e.g. '3251_h_unit16_...json' → 3251), then for every
    question in questions_and_answers find the matching Question row by lesson_id +
    question_text, fetch its Answer rows ordered by insertion id, match them
    positionally to the JSON answers, verify is_correct alignment, and UPDATE
    answer_text.
    """
    if not body.folder and not body.filename:
        raise HTTPException(status_code=400, detail="Provide at least one of 'folder' or 'filename'")

    json_files: list[Path] = []

    if body.folder:
        folder = Path(body.folder)
        if not folder.is_dir():
            raise HTTPException(status_code=400, detail=f"Folder not found: {body.folder}")
        json_files.extend(sorted(folder.glob("*.json")))

    if body.filename:
        p = Path(body.filename)
        if not p.is_file():
            raise HTTPException(status_code=400, detail=f"File not found: {body.filename}")
        if p not in json_files:
            json_files.append(p)

    if not json_files:
        raise HTTPException(status_code=400, detail="No JSON files found")

    if body.limit is not None:
        json_files = json_files[: body.limit]

    try:
        conn = connect_to_db(body.db)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Could not connect to database: {e}")

    files_processed = []
    files_failed = []

    try:
        for json_file in json_files:
            file_log = {
                "file": json_file.name,
                "status": None,
                "lesson_id": None,
                "answers_updated": 0,
                "errors": [],
            }

            # Extract lesson_id from filename prefix (digits before first '_')
            m = re.match(r"^(\d+)_", json_file.name)
            if not m:
                file_log["status"] = "failed"
                file_log["errors"].append("Could not extract lesson_id from filename")
                files_failed.append(file_log)
                continue
            lesson_id = int(m.group(1))
            file_log["lesson_id"] = lesson_id

            # Parse JSON
            try:
                data = json.loads(json_file.read_text(encoding="utf-8-sig"))
            except Exception as e:
                file_log["status"] = "failed"
                file_log["errors"].append(f"JSON parse error: {e}")
                files_failed.append(file_log)
                continue

            try:
                with conn.cursor() as cur:
                    for qa in data.get("questions_and_answers", []):
                        question_text = qa.get("question", "")
                        json_answers = qa.get("answers", [])
                        if not json_answers:
                            continue

                        # Find the Question row
                        cur.execute(
                            "SELECT question_id FROM Question WHERE lesson_id = %s AND question_text = %s LIMIT 1",
                            (lesson_id, question_text),
                        )
                        q_row = cur.fetchone()
                        if not q_row:
                            file_log["errors"].append(
                                f"Question not found: lesson_id={lesson_id} text={question_text!r}"
                            )
                            continue
                        question_id = q_row["question_id"]

                        # Fetch Answer rows ordered by answer_id (insertion order)
                        cur.execute(
                            "SELECT answer_id, is_correct FROM Answer WHERE question_id = %s ORDER BY answer_id ASC",
                            (question_id,),
                        )
                        db_answers = cur.fetchall()

                        if len(db_answers) != len(json_answers):
                            file_log["errors"].append(
                                f"question_id={question_id}: DB has {len(db_answers)} answers "
                                f"but JSON has {len(json_answers)}"
                            )
                            continue

                        # Match by is_correct: correct DB row → correct JSON text;
                        # incorrect DB rows paired positionally with incorrect JSON answers
                        db_correct = [a for a in db_answers if a["is_correct"]]
                        db_incorrect = [a for a in db_answers if not a["is_correct"]]
                        json_correct = [a for a in json_answers if a.get("is_correct")]
                        json_incorrect = [a for a in json_answers if not a.get("is_correct")]

                        if len(db_correct) != len(json_correct):
                            file_log["errors"].append(
                                f"question_id={question_id}: DB has {len(db_correct)} correct answer(s) "
                                f"but JSON has {len(json_correct)}"
                            )
                            continue

                        for db_ans, json_ans in list(zip(db_correct, json_correct)) + list(zip(db_incorrect, json_incorrect)):
                            cur.execute(
                                "UPDATE Answer SET answer_text = %s WHERE answer_id = %s",
                                (json_ans.get("text", ""), db_ans["answer_id"]),
                            )
                            file_log["answers_updated"] += 1

                conn.commit()
                file_log["status"] = "success"
                files_processed.append(file_log)

            except Exception as e:
                conn.rollback()
                file_log["status"] = "failed"
                file_log["errors"].append(str(e))
                files_failed.append(file_log)

    finally:
        conn.close()

    all_ok = len(files_failed) == 0
    return JSONResponse(
        content={
            "status": "ok" if all_ok else "partial",
            "summary": {
                "files_found": len(json_files),
                "files_succeeded": len(files_processed),
                "files_failed": len(files_failed),
                "answers_updated": sum(f["answers_updated"] for f in files_processed),
            },
            "files": files_processed + files_failed,
        },
        status_code=200 if all_ok else 207,
    )


lambda_handler = Mangum(app)
