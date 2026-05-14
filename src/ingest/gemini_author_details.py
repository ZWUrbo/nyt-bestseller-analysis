from __future__ import annotations

import json
from typing import Any, Optional

from src.ingest.gemini import extract_generated_text
from src.ingest.repo import GeminiAuthorDetailsInputRow, GeminiAuthorDetailsRow

PROMPT_TEMPLATE = """You are extracting structured data about an author. Use only verifiable, widely known facts. Do not infer or guess.

Input:
Author: {Author}
Book: {Book}

Rules:
- If the author input contains multiple people, return details for the first named author only.
- Use Google Search grounding to verify public facts before returning values.
- Only return a value if there is clear, high-confidence evidence.
- If information is unknown, disputed, private, or not publicly confirmed, return null.
- Do NOT infer sensitive attributes (BIPOC, LGBTQ, ethnicity) without explicit public confirmation from reliable public sources.
- Birth year must come from reliable public sources.
- Nationality must be a recognized country name.
- Ethnicity must only be included if explicitly and publicly identified by the author or reliable biographical sources.
- Gender must be:
  1 = Male
  2 = Female
  3 = Non-Binary
- Boolean fields must be true, false, or null (not strings).
- Birth year must be a 4-digit integer or null.
- Ethnicity must be a short descriptive string or null.

Output:
Return exactly one valid JSON object and nothing else.
Do not return markdown fences, citations, explanations, tables, arrays, tool code, or multiple JSON objects.
Use this exact schema:

{
  "birth_year": number | null,
  "nationality": string | null,
  "ethnicity": string | null,
  "gender": 1 | 2 | 3 | null,
  "bipoc": boolean | null,
  "lgbtq": boolean | null
}"""

GOOGLE_SEARCH_TOOLS: list[dict[str, Any]] = [
    {
        "google_search": {},
    }
]


def build_batch_request_line(row: GeminiAuthorDetailsInputRow) -> dict[str, Any]:
    prompt = PROMPT_TEMPLATE.replace("{Author}", row.author).replace("{Book}", row.book)
    return {
        "key": str(row.nyt_author_id),
        "request": {
            "contents": [
                {
                    "role": "user",
                    "parts": [
                        {"text": prompt},
                    ],
                }
            ],
            "generationConfig": {
                "temperature": 0.0,
                "candidateCount": 1,
            },
            "tools": GOOGLE_SEARCH_TOOLS,
        },
    }


def parse_batch_result_line(raw_line: str) -> GeminiAuthorDetailsRow:
    parsed = json.loads(raw_line)
    nyt_author_id, key_error = _extract_nyt_author_id(parsed)
    response_payload = parsed.get("response")
    error_payload = parsed.get("error")

    if isinstance(error_payload, dict):
        error_message = error_payload.get("message") or json.dumps(error_payload, ensure_ascii=False)
        last_error = ",".join(
            [x for x in [key_error, f"batch_error:{clean_whitespace(str(error_message))[:300]}"] if x]
        )
        return GeminiAuthorDetailsRow(
            nyt_author_id=nyt_author_id,
            birth_year=None,
            nationality=None,
            ethnicity=None,
            gender=None,
            is_bipoc=None,
            is_lgbtq=None,
            raw_response=json.dumps(parsed, ensure_ascii=False),
            last_error=last_error,
        )

    if not isinstance(response_payload, dict):
        last_error = ",".join([x for x in [key_error, "batch_response_missing"] if x])
        return GeminiAuthorDetailsRow(
            nyt_author_id=nyt_author_id,
            birth_year=None,
            nationality=None,
            ethnicity=None,
            gender=None,
            is_bipoc=None,
            is_lgbtq=None,
            raw_response=json.dumps(parsed, ensure_ascii=False),
            last_error=last_error,
        )

    raw_text = extract_generated_text(response_payload)
    if not raw_text:
        last_error = ",".join([x for x in [key_error, "batch_text_missing"] if x])
        return GeminiAuthorDetailsRow(
            nyt_author_id=nyt_author_id,
            birth_year=None,
            nationality=None,
            ethnicity=None,
            gender=None,
            is_bipoc=None,
            is_lgbtq=None,
            raw_response=json.dumps(parsed, ensure_ascii=False),
            last_error=last_error,
        )

    values, format_error = parse_author_details_response(raw_text)
    last_error = ",".join([x for x in [key_error, format_error] if x]) or None
    return GeminiAuthorDetailsRow(
        nyt_author_id=nyt_author_id,
        birth_year=values["birth_year"],
        nationality=values["nationality"],
        ethnicity=values["ethnicity"],
        gender=values["gender"],
        is_bipoc=values["bipoc"],
        is_lgbtq=values["lgbtq"],
        raw_response=raw_text,
        last_error=last_error,
    )


def parse_author_details_response(raw_text: str) -> tuple[dict[str, Any], Optional[str]]:
    empty_values = {
        "birth_year": None,
        "nationality": None,
        "ethnicity": None,
        "gender": None,
        "bipoc": None,
        "lgbtq": None,
    }
    try:
        parsed = json.loads(_extract_json_object_text(raw_text))
    except json.JSONDecodeError:
        fallback_values = _parse_table_response(raw_text, empty_values)
        if fallback_values is not None:
            return fallback_values, None
        if _is_null_explanation(raw_text):
            return empty_values, None
        return empty_values, "structured_json_invalid"

    if not isinstance(parsed, dict):
        return empty_values, "structured_json_not_object"

    errors: list[str] = []
    values = dict(empty_values)
    expected_keys = set(empty_values)
    extra_keys = set(parsed) - expected_keys
    if extra_keys:
        errors.append(f"unexpected_keys:{','.join(sorted(extra_keys))}")

    values["birth_year"] = _parse_birth_year(parsed.get("birth_year"), errors)
    values["nationality"] = _parse_nullable_string(parsed.get("nationality"), "nationality", errors)
    values["ethnicity"] = _parse_nullable_string(parsed.get("ethnicity"), "ethnicity", errors)
    values["gender"] = _parse_gender(parsed.get("gender"), errors)
    values["bipoc"] = _parse_nullable_bool(parsed.get("bipoc"), "bipoc", errors)
    values["lgbtq"] = _parse_nullable_bool(parsed.get("lgbtq"), "lgbtq", errors)

    return values, ",".join(errors) if errors else None


def _extract_json_object_text(raw_text: str) -> str:
    cleaned = raw_text.strip()
    if cleaned.startswith("```"):
        lines = cleaned.splitlines()
        if lines:
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        cleaned = "\n".join(lines).strip()

    decoder = json.JSONDecoder()
    for start in _json_object_start_indexes(cleaned):
        try:
            parsed, end = decoder.raw_decode(cleaned[start:])
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
            return cleaned[start : start + end]

    return cleaned


def _json_object_start_indexes(value: str) -> list[int]:
    indexes: list[int] = []
    if value.startswith("{"):
        indexes.append(0)
    indexes.extend(i for i, char in enumerate(value) if char == "{" and i not in indexes)
    return indexes


def _parse_table_response(raw_text: str, empty_values: dict[str, Any]) -> dict[str, Any] | None:
    raw_values: dict[str, str] = {}
    for line in raw_text.splitlines():
        cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
        if len(cells) != 2:
            continue
        key, value = cells
        if key in empty_values:
            raw_values[key] = value

    if not raw_values:
        return None

    errors: list[str] = []
    values = dict(empty_values)
    values["birth_year"] = _parse_birth_year(_parse_literal(raw_values.get("birth_year")), errors)
    values["nationality"] = _parse_nullable_string(
        _parse_literal(raw_values.get("nationality")), "nationality", errors
    )
    values["ethnicity"] = _parse_nullable_string(_parse_literal(raw_values.get("ethnicity")), "ethnicity", errors)
    values["gender"] = _parse_gender(_parse_literal(raw_values.get("gender")), errors)
    values["bipoc"] = _parse_nullable_bool(_parse_literal(raw_values.get("bipoc")), "bipoc", errors)
    values["lgbtq"] = _parse_nullable_bool(_parse_literal(raw_values.get("lgbtq")), "lgbtq", errors)
    return None if errors else values


def _parse_literal(value: str | None) -> Any:
    if value is None:
        return None
    cleaned = clean_whitespace(value)
    if cleaned.lower() == "null" or cleaned == "":
        return None
    if cleaned.lower() == "true":
        return True
    if cleaned.lower() == "false":
        return False
    try:
        return int(cleaned)
    except ValueError:
        return cleaned


def _is_null_explanation(raw_text: str) -> bool:
    lowered = raw_text.lower()
    return (
        "cannot provide" in lowered
        or "not readily available" in lowered
        or "no public information" in lowered
    )


def clean_whitespace(value: str) -> str:
    return " ".join(value.split()).strip()


def _extract_result_key(parsed: dict[str, Any]) -> str:
    key = parsed.get("key")
    if isinstance(key, str) and key.strip():
        return key.strip()
    metadata = parsed.get("metadata")
    if isinstance(metadata, dict):
        metadata_key = metadata.get("key")
        if isinstance(metadata_key, str) and metadata_key.strip():
            return metadata_key.strip()
    return ""


def _extract_nyt_author_id(parsed: dict[str, Any]) -> tuple[int, Optional[str]]:
    raw_key = _extract_result_key(parsed)
    if not raw_key:
        return 0, "nyt_author_id_missing"
    try:
        nyt_author_id = int(raw_key)
    except ValueError:
        return 0, "nyt_author_id_invalid"
    if nyt_author_id <= 0:
        return 0, "nyt_author_id_invalid"
    return nyt_author_id, None


def _parse_birth_year(value: Any, errors: list[str]) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        errors.append("birth_year_invalid")
        return None
    if value < 1000 or value > 9999:
        errors.append("birth_year_invalid")
        return None
    return value


def _parse_nullable_string(value: Any, field_name: str, errors: list[str]) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        errors.append(f"{field_name}_invalid")
        return None
    cleaned = clean_whitespace(value)
    return cleaned or None


def _parse_gender(value: Any, errors: list[str]) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or value not in {1, 2, 3}:
        errors.append("gender_invalid")
        return None
    return int(value)


def _parse_nullable_bool(value: Any, field_name: str, errors: list[str]) -> bool | None:
    if value is None:
        return None
    if not isinstance(value, bool):
        errors.append(f"{field_name}_invalid")
        return None
    return value
