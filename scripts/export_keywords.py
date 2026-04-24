from __future__ import annotations

import argparse
import csv
import json
import math
import re
import unicodedata
from collections import Counter, defaultdict
from pathlib import Path


WHITESPACE_RE = re.compile(r"\s+")
NON_ALNUM_RE = re.compile(r"[^a-z0-9'&\- ]+")
PAREN_CONTENT_RE = re.compile(r"\([^)]*\)")
UUID_SUFFIX_RE = re.compile(r"[-_](?:[0-9a-f]{8,}|[0-9]{6,})(?:-[0-9a-f]{4,})*$")
LEADING_GENRE_PREFIX_RE = re.compile(r"^genre\s+")
ONLY_CODE_RE = re.compile(r"^(?:\d{4}|\d{10,}|[0-9a-f]{8,})$")
YEAR_OR_CODE_RE = re.compile(r"^(?:\d{1,4}(?:-\d{1,4})?|\d[\d .:/-]*[a-z]?)$")
TOKEN_RE = re.compile(r"[a-z0-9']+")
SENTENCE_SPLIT_RE = re.compile(r"[.!?;:\n]+")

TOP_N_KEYWORDS = 12
TITLE_EXCLUSION_TERMS = {"book", "novel", "memoir", "story", "guide", "series"}
STRUCTURED_EXCLUDED_TERMS = {
    "adult",
    "american fiction",
    "american literature",
    "audiobook",
    "books and reading",
    "fiction",
    "general",
    "history",
    "juvenile literature",
    "large print books",
    "literature",
    "new york times bestseller",
    "new york times reviewed",
    "nonfiction",
    "social aspects",
    "treatment",
    "young adult",
}
HARDCOVER_GENERIC_TERMS = {
    "ability",
    "adult",
    "amazing",
    "audio",
    "audiobook",
    "audible",
    "books and reading",
    "contemp",
    "ebook",
    "fiction",
    "from audible",
    "general",
    "history",
    "kindle unlimited",
    "library",
    "libby",
    "nonfiction",
    "women",
}
STOPWORDS = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "been",
    "but",
    "by",
    "for",
    "from",
    "had",
    "has",
    "have",
    "he",
    "her",
    "hers",
    "him",
    "his",
    "i",
    "if",
    "in",
    "into",
    "is",
    "it",
    "its",
    "me",
    "more",
    "most",
    "my",
    "of",
    "on",
    "or",
    "our",
    "out",
    "she",
    "so",
    "than",
    "that",
    "the",
    "their",
    "them",
    "they",
    "this",
    "to",
    "too",
    "was",
    "we",
    "were",
    "what",
    "when",
    "which",
    "who",
    "why",
    "with",
    "would",
    "you",
    "your",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export Tableau-friendly keyword files from the pipeline output CSV."
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=Path("/Users/zacurbiztondo/Desktop/isbn13_text_enrichment_pipeline_output.csv"),
        help="Path to the pipeline CSV containing enrichment columns.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/processed/features/isbn13_keywords.csv"),
        help="Compact output CSV with isbn13 and JSON-encoded keywords.",
    )
    parser.add_argument(
        "--tableau-output",
        type=Path,
        default=Path("data/processed/features/isbn13_keyword_tableau.csv"),
        help="Long output CSV with one row per isbn13/keyword pair.",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=TOP_N_KEYWORDS,
        help="Maximum number of keywords per isbn13 row.",
    )
    return parser.parse_args()


def normalize_text(value: str) -> str:
    ascii_value = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")
    return WHITESPACE_RE.sub(" ", ascii_value.strip())


def clean_phrase(value: str) -> str | None:
    term = normalize_text(value).lower()
    if not term:
        return None
    term = PAREN_CONTENT_RE.sub("", term)
    term = UUID_SUFFIX_RE.sub("", term)
    term = term.replace("_", " ").replace("/", " ").replace("-", " ")
    term = LEADING_GENRE_PREFIX_RE.sub("", term)
    term = NON_ALNUM_RE.sub(" ", term)
    term = WHITESPACE_RE.sub(" ", term).strip(" -'")
    if not term:
        return None
    if ONLY_CODE_RE.fullmatch(term) or YEAR_OR_CODE_RE.fullmatch(term):
        return None
    if any(char.isdigit() for char in term):
        return None
    words = term.split()
    if not words or len(words) > 6:
        return None
    if len(words) == 1 and len(words[0]) <= 2:
        return None
    if words[-1] in {"and", "for", "in", "of", "the", "to"}:
        return None
    return term


def parse_json_list(raw_value: str | None) -> list[str]:
    if not raw_value or not raw_value.strip():
        return []
    try:
        parsed = json.loads(raw_value)
    except json.JSONDecodeError:
        return []
    return [item for item in parsed if isinstance(item, str)]


def parse_hardcover_tags(row: dict[str, str]) -> dict[str, list[dict[str, object]]]:
    raw_value = row.get("hardcover_tags_parsed") or row.get("cached_tags") or ""
    if not raw_value.strip():
        return {}
    try:
        parsed = json.loads(raw_value)
    except json.JSONDecodeError:
        return {}
    if not isinstance(parsed, dict):
        return {}
    output: dict[str, list[dict[str, object]]] = {}
    for category, items in parsed.items():
        if isinstance(category, str) and isinstance(items, list):
            output[category] = [item for item in items if isinstance(item, dict)]
    return output


def split_pipe_list(raw_value: str | None) -> list[str]:
    if not raw_value:
        return []
    return [part.strip() for part in raw_value.split("|") if part and part.strip()]


def build_final_text(row: dict[str, str]) -> str:
    parts: list[str] = []
    title = (row.get("title") or "").strip()
    author = (row.get("author") or "").strip()
    description = (
        (row.get("desc_hardcover") or "").strip()
        or (row.get("desc_openlibrary") or "").strip()
        or (row.get("desc_nyt") or "").strip()
    )
    subjects = split_pipe_list(row.get("subjects"))
    subject_places = split_pipe_list(row.get("subject_places"))

    if title:
        parts.append(f"{title}.")
    if author:
        parts.append(f"by {author}.")
    if description:
        parts.append(description)
    if subjects:
        parts.append("Subjects: " + "; ".join(subjects))
    if subject_places:
        parts.append("Places: " + "; ".join(subject_places))
    return " ".join(parts).strip()


def narrative_text_only(text: str) -> str:
    return re.split(r"\b(?:Subjects|Places):\s*", text, maxsplit=1)[0].strip()


def add_candidate(
    scores: defaultdict[str, float],
    keyword: str | None,
    score: float,
    seen_order: dict[str, int],
) -> None:
    if not keyword:
        return
    if keyword in STRUCTURED_EXCLUDED_TERMS:
        return
    if keyword in HARDCOVER_GENERIC_TERMS:
        return
    if keyword not in seen_order:
        seen_order[keyword] = len(seen_order)
    scores[keyword] += score


def add_openlibrary_terms(
    row: dict[str, str],
    scores: defaultdict[str, float],
    seen_order: dict[str, int],
) -> None:
    subject_terms = parse_json_list(row.get("openlibrary_subjects_clean"))
    if not subject_terms:
        subject_terms = split_pipe_list(row.get("subjects"))
    place_terms = parse_json_list(row.get("openlibrary_subject_places_clean"))
    if not place_terms:
        place_terms = split_pipe_list(row.get("subject_places"))

    for index, term in enumerate(subject_terms):
        add_candidate(scores, clean_phrase(term), max(6.0 - (index * 0.22), 2.5), seen_order)
    for index, term in enumerate(place_terms):
        add_candidate(scores, clean_phrase(term), max(2.5 - (index * 0.2), 1.5), seen_order)


def add_hardcover_terms(
    row: dict[str, str],
    scores: defaultdict[str, float],
    seen_order: dict[str, int],
) -> None:
    tags_by_category = parse_hardcover_tags(row)
    category_weights = {
        "Content Warning": 5.0,
        "Genre": 5.5,
        "Mood": 4.5,
        "Tag": 4.0,
    }
    for category_name, items in tags_by_category.items():
        category_weight = category_weights.get(category_name, 3.0)
        for item in items:
            raw_tag = item.get("tag") or item.get("tagSlug")
            if not isinstance(raw_tag, str):
                continue
            cleaned = clean_phrase(raw_tag)
            count = item.get("count")
            count_bonus = 0.0
            if isinstance(count, int) and count > 0:
                count_bonus = min(math.log1p(count), 2.0)
            add_candidate(scores, cleaned, category_weight + count_bonus, seen_order)


def extract_text_phrases(text: str) -> Counter[str]:
    original = normalize_text(text)
    phrase_counts: Counter[str] = Counter()

    for sentence in SENTENCE_SPLIT_RE.split(original):
        tokens = [token.lower() for token in TOKEN_RE.findall(sentence)]
        if not tokens:
            continue

        content_tokens = [token for token in tokens if token not in STOPWORDS and len(token) > 2]
        for token in content_tokens:
            phrase_counts[token] += 1

        for n in range(2, 5):
            for index in range(len(tokens) - n + 1):
                phrase_tokens = tokens[index : index + n]
                if phrase_tokens[0] in STOPWORDS or phrase_tokens[-1] in STOPWORDS:
                    continue
                if sum(token in STOPWORDS for token in phrase_tokens) > 1:
                    continue
                if all(token in STOPWORDS for token in phrase_tokens):
                    continue
                phrase = clean_phrase(" ".join(phrase_tokens))
                if not phrase:
                    continue
                phrase_counts[phrase] += 1

    return phrase_counts


def add_text_terms(
    row: dict[str, str],
    scores: defaultdict[str, float],
    seen_order: dict[str, int],
) -> None:
    final_text = narrative_text_only(build_final_text(row))
    phrase_counts = extract_text_phrases(final_text)
    title = clean_phrase(row.get("title") or "")
    author = clean_phrase(row.get("author") or "")

    if author:
        add_candidate(scores, author, 5.5, seen_order)
    if title:
        add_candidate(scores, title, 4.0, seen_order)
        title_words = title.split()
        for n in range(min(4, len(title_words)), 0, -1):
            phrase = " ".join(title_words[:n])
            if phrase not in TITLE_EXCLUSION_TERMS:
                add_candidate(scores, clean_phrase(phrase), 1.2 + (0.3 * n), seen_order)

    for phrase, count in phrase_counts.most_common(80):
        if phrase in STOPWORDS:
            continue
        if len(phrase.split()) == 1 and count < 2:
            continue
        if phrase in STRUCTURED_EXCLUDED_TERMS:
            continue
        bonus = 0.0
        if title and phrase in title:
            bonus += 1.5
        if author and phrase == author:
            bonus += 2.0
        add_candidate(scores, phrase, min(count * 0.9, 4.0) + bonus, seen_order)


def rank_keywords(row: dict[str, str], top_n: int) -> list[str]:
    scores: defaultdict[str, float] = defaultdict(float)
    seen_order: dict[str, int] = {}

    add_openlibrary_terms(row, scores, seen_order)
    add_hardcover_terms(row, scores, seen_order)
    add_text_terms(row, scores, seen_order)

    ranked = sorted(
        scores.items(),
        key=lambda item: (-item[1], len(item[0]), seen_order[item[0]]),
    )

    keywords: list[str] = []
    for keyword, _score in ranked:
        if keyword in keywords:
            continue
        if any(keyword != existing and keyword in existing for existing in keywords):
            continue
        keywords.append(keyword)
        if len(keywords) >= top_n:
            break
    return keywords


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def export_keywords(input_path: Path, output_path: Path, tableau_output_path: Path, top_n: int) -> tuple[int, int]:
    ensure_parent(output_path)
    ensure_parent(tableau_output_path)

    row_count = 0
    keyword_count = 0

    with (
        input_path.open(newline="", encoding="utf-8-sig") as infile,
        output_path.open("w", newline="", encoding="utf-8") as compact_outfile,
        tableau_output_path.open("w", newline="", encoding="utf-8") as tableau_outfile,
    ):
        reader = csv.DictReader(infile)
        compact_writer = csv.DictWriter(compact_outfile, fieldnames=["isbn13", "keywords"])
        tableau_writer = csv.DictWriter(tableau_outfile, fieldnames=["isbn13", "keyword"])
        compact_writer.writeheader()
        tableau_writer.writeheader()

        for row in reader:
            isbn13 = (row.get("isbn13") or "").strip()
            if not isbn13:
                continue

            keywords = rank_keywords(row, top_n=max(top_n, 1))
            compact_writer.writerow({"isbn13": isbn13, "keywords": json.dumps(keywords, ensure_ascii=False)})
            for keyword in keywords:
                tableau_writer.writerow({"isbn13": isbn13, "keyword": keyword})

            row_count += 1
            keyword_count += len(keywords)

    return row_count, keyword_count


def main() -> None:
    args = parse_args()
    row_count, keyword_count = export_keywords(
        input_path=args.input,
        output_path=args.output,
        tableau_output_path=args.tableau_output,
        top_n=args.top_n,
    )
    print(
        f"Exported keywords for {row_count} isbn13 rows to {args.output} "
        f"and {args.tableau_output} ({keyword_count} keyword rows)."
    )


if __name__ == "__main__":
    main()
