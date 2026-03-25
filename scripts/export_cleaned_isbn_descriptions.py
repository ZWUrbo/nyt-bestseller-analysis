from __future__ import annotations

import argparse
import html
import re
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import settings
from src.utils.io import connect_sqlite

HTML_TAG_RE = re.compile(r"<[^>]+>")
URL_RE = re.compile(r"https?://\S+|www\.\S+")
ISBN_RE = re.compile(r"\b97[89]\d{10}\b")
WEIRD_TOKEN_RE = re.compile(r"[_~`^*#=+|\\/]+")
NON_TEXT_RE = re.compile(r"[^a-z0-9.!?\-\s']")
WHITESPACE_RE = re.compile(r"\s+")
SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+")
LEADING_BLURB_PATTERNS = [
    re.compile(r"^\s*new york times bestselling author\b[^.?!:]*[.?!:]\s*", re.IGNORECASE),
    re.compile(r"^\s*from (?:the )?new york times bestselling author\b[^.?!:]*[.?!:]\s*", re.IGNORECASE),
    re.compile(r"^\s*from the author of\b[^.?!:]*[.?!:]\s*", re.IGNORECASE),
    re.compile(r"^\s*usa today bestselling author\b[^.?!:]*[.?!:]\s*", re.IGNORECASE),
    re.compile(r"^\s*instant new york times bestseller\b[^.?!:]*[.?!:]\s*", re.IGNORECASE),
    re.compile(r"^\s*#?\d+\s*new york times bestseller\b[^.?!:]*[.?!:]\s*", re.IGNORECASE),
]
PROMO_MARKERS = (
    "bestseller",
    "bestselling",
    "award-winning",
    "award winner",
    "anticipated book",
    "best book",
    "must-read",
    "must read",
    "instant ",
    "acclaimed",
    "praise for",
    "praised by",
    "author of",
    "publisher weekly",
    "oprah daily",
    "kirkus",
    "bookpage",
    "goodreads",
    "washington post",
    "usa today",
    "library journal",
)
NOISE_LEMMAS = {
    "author",
    "bestseller",
    "bestselling",
    "acclaim",
    "acclaimed",
    "award",
    "winner",
    "instant",
    "anticipate",
    "review",
    "goodread",
    "oprah",
    "daily",
    "kirkus",
    "bookpage",
    "vogue",
    "esquire",
    "lithub",
    "npr",
    "journal",
    "publisher",
    "weekly",
    "today",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export one NLP-cleaned, concatenated description per distinct isbn13 from NYT and enrichment tables."
    )
    parser.add_argument(
        "--db-path",
        type=Path,
        default=settings.db_path,
        help="Path to the SQLite database.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=settings.data_dir / "processed" / "features" / "isbn13_cleaned_descriptions.csv",
        help="Destination CSV path.",
    )
    parser.add_argument(
        "--min-words",
        type=int,
        default=30,
        help="Drop descriptions shorter than this many cleaned tokens.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=64,
        help="spaCy batch size for lemmatization.",
    )
    return parser.parse_args()


def load_source_frame(db_path: Path) -> pd.DataFrame:
    query = """
    WITH nyt_grouped AS (
        SELECT
            isbn13,
            GROUP_CONCAT(description, ' ') AS nyt_description
        FROM (
            SELECT DISTINCT
                isbn13,
                TRIM(description) AS description
            FROM nyt_entries
            WHERE isbn13 IS NOT NULL
              AND TRIM(isbn13) <> ''
              AND description IS NOT NULL
              AND TRIM(description) <> ''
        )
        GROUP BY isbn13
    )
    SELECT
        n.isbn13,
        COALESCE(y.nyt_description, '') AS nyt_description,
        COALESCE(o.description, '') AS openlibrary_description,
        COALESCE(h.description, '') AS hardcover_description
    FROM (
        SELECT DISTINCT isbn13
        FROM nyt_entries
        WHERE isbn13 IS NOT NULL
          AND TRIM(isbn13) <> ''
    ) n
    LEFT JOIN nyt_grouped y
        ON y.isbn13 = n.isbn13
    LEFT JOIN openlibrary_enrichment o
        ON o.isbn13 = n.isbn13
    LEFT JOIN hardcover_enrichment h
        ON h.isbn13 = n.isbn13
    ORDER BY n.isbn13
    """
    conn = connect_sqlite(db_path)
    try:
        return pd.read_sql_query(query, conn)
    finally:
        conn.close()


def strip_leading_blurbs(text: str) -> str:
    cleaned = text
    while True:
        updated = cleaned
        for pattern in LEADING_BLURB_PATTERNS:
            updated = pattern.sub("", updated, count=1)
        if updated == cleaned:
            return cleaned
        cleaned = updated.lstrip()


def strip_promotional_lead_sentences(text: str) -> str:
    sentences = SENTENCE_SPLIT_RE.split(text.strip())
    kept: list[str] = []
    dropping = True

    for sentence in sentences:
        normalized = WHITESPACE_RE.sub(" ", sentence).strip().lower()
        if not normalized:
            continue
        has_marker = any(marker in normalized for marker in PROMO_MARKERS)
        if dropping and has_marker:
            continue
        dropping = False
        kept.append(sentence.strip())

    return " ".join(kept).strip()


def normalize_raw_text(raw_text: str) -> str:
    text = html.unescape(raw_text or "").lower()
    text = HTML_TAG_RE.sub(" ", text)
    text = URL_RE.sub(" ", text)
    text = ISBN_RE.sub(" ", text)
    text = WEIRD_TOKEN_RE.sub(" ", text)
    text = text.replace("&nbsp;", " ")
    text = strip_leading_blurbs(text)
    text = strip_promotional_lead_sentences(text)
    text = NON_TEXT_RE.sub(" ", text)
    return WHITESPACE_RE.sub(" ", text).strip()


def build_stopwords() -> set[str]:
    try:
        import spacy
    except ImportError as exc:
        raise RuntimeError(
            "spaCy is required for preprocessing. Install it in the active environment."
        ) from exc

    try:
        from nltk.corpus import stopwords
    except ImportError as exc:
        raise RuntimeError(
            "NLTK is required for preprocessing. Install it in the active environment."
        ) from exc

    try:
        nltk_stopwords = set(stopwords.words("english"))
    except LookupError as exc:
        raise RuntimeError(
            "NLTK stopwords corpus is missing. Run the NLTK downloader for 'stopwords'."
        ) from exc

    return set(spacy.lang.en.stop_words.STOP_WORDS) | nltk_stopwords


def load_spacy_model():
    try:
        import spacy
    except ImportError as exc:
        raise RuntimeError(
            "spaCy is required for preprocessing. Install it in the active environment."
        ) from exc

    try:
        return spacy.load("en_core_web_sm", disable=["parser", "ner"])
    except OSError as exc:
        raise RuntimeError(
            "spaCy model 'en_core_web_sm' is missing. Install it in the active environment."
        ) from exc


def clean_descriptions(texts: list[str], min_words: int, batch_size: int) -> list[str]:
    nlp = load_spacy_model()
    stopwords = build_stopwords()
    cleaned_texts: list[str] = []

    for doc in nlp.pipe(texts, batch_size=batch_size):
        lemmas: list[str] = []
        for token in doc:
            if token.is_space or token.is_punct:
                continue
            if token.like_num:
                continue
            lemma = token.lemma_.strip().lower()
            if not lemma or lemma == "-pron-":
                continue
            if len(lemma) <= 2:
                continue
            if lemma in stopwords:
                continue
            if lemma in NOISE_LEMMAS:
                continue
            if not lemma.isalpha():
                continue
            lemmas.append(lemma)

        cleaned_texts.append(" ".join(lemmas) if len(lemmas) >= min_words else "")

    return cleaned_texts


def build_description_frame(source_frame: pd.DataFrame, min_words: int, batch_size: int) -> pd.DataFrame:
    combined = (
        source_frame["nyt_description"].fillna("")
        + " "
        + source_frame["openlibrary_description"].fillna("")
        + " "
        + source_frame["hardcover_description"].fillna("")
    )
    normalized_texts = [normalize_raw_text(value) for value in combined]
    cleaned_texts = clean_descriptions(
        texts=normalized_texts,
        min_words=min_words,
        batch_size=batch_size,
    )

    frame = pd.DataFrame(
        {
            "isbn13": source_frame["isbn13"],
            "description": cleaned_texts,
        }
    )
    return frame.sort_values("isbn13").reset_index(drop=True)


def main() -> None:
    args = parse_args()
    settings.ensure_dirs()

    source_frame = load_source_frame(args.db_path)
    description_frame = build_description_frame(
        source_frame=source_frame,
        min_words=max(args.min_words, 1),
        batch_size=max(args.batch_size, 1),
    )

    args.output.parent.mkdir(parents=True, exist_ok=True)
    description_frame.to_csv(args.output, index=False)

    non_empty_rows = int(description_frame["description"].astype(bool).sum())
    print(
        f"Exported cleaned descriptions to {args.output} "
        f"(rows={len(description_frame)}, non_empty_descriptions={non_empty_rows})."
    )


if __name__ == "__main__":
    main()
