# NYT Bestseller Analysis

A reproducible Python data pipeline for collecting, enriching, and exporting New York Times bestseller data for literary analysis, exploratory research, and analytics storytelling.

This project uses the New York Times bestseller lists as a starting point, then enriches each title with metadata from Open Library and Hardcover, plus AI-assisted summaries and content tags from Gemini. The result is a local SQLite database and a set of analysis-ready CSV outputs that make it easier to study the subjects, themes, signals, and patterns present in widely read books.

## Project Motivation

I started this project because of my curiosity about people and my love of reading. I wanted to better understand the subjects, themes, and ideas people are consuming through books because I believe books, as a form of media, meaningfully shape how we see the world.

Using the New York Times bestseller lists as a proxy for widely consumed books, I built this project to enrich those titles with descriptive metadata, readership signals, and AI-assisted summaries. My goal is to use that data to provide an overview of what kinds of content are being consumed by people at scale to possibly obtain just a fraction better understanding of how people think.

## What This Project Does

- Pulls weekly New York Times bestseller entries for a configurable date range
- Enriches titles by `isbn13` with subjects, places, descriptions, and edition metadata from Open Library
- Adds Hardcover metadata such as ratings, readership signals, and tag groupings
- Enriches author records with additional Hardcover profile fields
- Generates AI-assisted summaries and seed content tags using Gemini
- Exports clean CSV datasets for notebooks, SQL, BI tools, and Tableau-style analysis

## Why It’s Useful

This repository sits at the intersection of data engineering, analytics, and cultural research. It is designed for questions like:

- What genres, moods, and themes appear most often in bestselling books?
- How does the language around popular books shift over time?
- Who are the people writing these highly consumed books?
- Which subject areas or content patterns show up repeatedly across widely consumed titles?
- What structured features can be extracted from book metadata for dashboards or downstream modeling?

## Data Sources

- `New York Times Books API`
  Weekly bestseller list entries and descriptive list data
- `Open Library`
  Subjects, subject places, descriptions, and bibliographic metadata
- `Hardcover API`
  Book enrichment, readership signals, ratings, and tag groupings
- `Gemini`
  AI-assisted summaries and seed content tags

## Pipeline Overview

The main orchestration entry point is:

```bash
python scripts/run_pipeline.py
```

Pipeline stages:

1. `scripts/fetch_nyt.py`
   Ingests weekly bestseller entries from the New York Times.
2. `scripts/fetch_openlibrary.py`
   Enriches books with Open Library descriptions, subjects, and places.
3. `scripts/fetch_hardcover.py`
   Adds Hardcover metadata, tag collections, and readership-related fields.
4. `scripts/fetch_hardcover_authors.py`
   Enriches related authors with additional author-level data.
5. `scripts/fetch_gemini_summaries.py`
   Generates AI-assisted summaries and seed content tags.

After ingestion, the project provides export scripts for flat analytical outputs:

- `scripts/export_tables.py`
- `scripts/export_keywords.py`
- `scripts/export_gemini_content_tags.py`

## Outputs

The project creates a local SQLite database at `data/interim/books.db` and writes processed files under `data/processed/`.

Key outputs include:

- Core table exports in `data/processed/exports/`
- Keyword feature files in `data/processed/features/`
- Tableau-friendly long-format keyword and content-tag files
- Exported Gemini summary datasets for downstream text analysis

Primary database tables:

- `nyt_entries`
- `openlibrary_enrichment`
- `hardcover_enrichment`
- `hardcover_authors`
- `gemini_content_summaries`

## Repository Structure

```text
literary-analysis/
├── data/
│   ├── raw/
│   ├── interim/
│   └── processed/
├── notebooks/
├── prompts/
├── scripts/
├── src/
│   ├── ingest/
│   └── utils/
├── README.md
├── pyproject.toml
└── requirements.txt
```

## Tech Stack

- `Python 3.10+`
- `SQLite`
- `pandas`
- `requests`
- `requests-cache`
- `pydantic`
- `python-dotenv`
- `tenacity`
- `tqdm`

## Setup

### 1. Clone the repository

```bash
git clone <your-repo-url>
cd literary-analysis
```

### 2. Create and activate a virtual environment

```bash
python -m venv .venv
source .venv/bin/activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure environment variables

Create a `.env` file in the project root:

```env
NYT_API_KEY=your_nyt_api_key
HARDCOVER_API_TOKEN=your_hardcover_api_token
GEMINI_API_KEY=your_gemini_api_key
CONTACT_EMAIL=your_email@example.com

START_YEAR=2021
END_YEAR=2024
NYT_RPS=2.0
OPENLIBRARY_RPS=5.0
HARDCOVER_RPS=0.8
```

Optional environment variables:

- `HARDCOVER_API_URL`
- `GEMINI_API_URL`
- `GEMINI_MODEL`
- `HTTP_CACHE_PATH`
- `HTTP_CACHE_EXPIRE_SECONDS`

## Usage

### Run the full ingestion pipeline

```bash
python scripts/run_pipeline.py
```

### Run for a specific date range

```bash
python scripts/run_pipeline.py --start 2023-01-01 --end 2023-12-31
```

### Limit enrichment volume while testing

```bash
python scripts/run_pipeline.py --limit 100
```

### Reprocess existing records

```bash
python scripts/run_pipeline.py --refresh-all
```

### Skip selected stages

```bash
python scripts/run_pipeline.py --skip-gemini
python scripts/run_pipeline.py --skip-openlibrary --skip-hardcover
python scripts/run_pipeline.py --skip-hardcover-authors
```

### Export analytical datasets

```bash
python scripts/export_tables.py
python scripts/export_keywords.py
python scripts/export_gemini_content_tags.py
```

## Notes

- The pipeline is designed to be incremental, with enrichment keyed primarily by `isbn13`.
- Request caching is used to reduce redundant API calls and make repeated runs cheaper and faster.
- Some stages require authenticated third-party APIs, so full end-to-end execution depends on valid credentials.

## Future Directions

Potential next steps for the project include:

- richer temporal analysis across bestseller periods
- deeper author-level and demographic exploration
- topic clustering or embedding-based comparisons across titles
- dashboarding layers for interactive exploration
- stronger validation and automated test coverage

## Closing Thought

This project is technical by design, but it is also personal. It reflects an interest in books not just as products, but as signals of culture, attention, and shared imagination.
