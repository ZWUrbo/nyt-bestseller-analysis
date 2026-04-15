from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path
from typing import Sequence


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Run the ingestion pipeline in order: NYT extraction, Open Library enrichment, Hardcover enrichment, then Hardcover author enrichment."
    )
    p.add_argument("--start", type=str, default=None, help="NYT start date (YYYY-MM-DD).")
    p.add_argument("--end", type=str, default=None, help="NYT end date (YYYY-MM-DD).")
    p.add_argument(
        "--limit",
        type=int,
        default=1000,
        help="Open Library max ISBN13 values to process.",
    )
    p.add_argument(
        "--refresh-all",
        action="store_true",
        help="Open Library: reprocess all NYT ISBN13 values (not only missing rows).",
    )
    p.add_argument(
        "--batch-size",
        type=int,
        default=200,
        help="Open Library upsert batch size.",
    )
    p.add_argument(
        "--skip-nyt",
        action="store_true",
        help="Skip NYT extraction step.",
    )
    p.add_argument(
        "--skip-openlibrary",
        action="store_true",
        help="Skip Open Library enrichment step.",
    )
    p.add_argument(
        "--skip-hardcover",
        action="store_true",
        help="Skip Hardcover enrichment step.",
    )
    p.add_argument(
        "--skip-hardcover-authors",
        action="store_true",
        help="Skip Hardcover author enrichment step.",
    )
    p.add_argument(
        "--skip-gemini",
        action="store_true",
        help="Skip Gemini content summary enrichment step.",
    )
    return p.parse_args()


def run_step(cmd: Sequence[str], cwd: Path, step_name: str) -> None:
    print(f"[pipeline] Running {step_name}: {' '.join(cmd)}")
    subprocess.run(cmd, cwd=str(cwd), check=True)


def main() -> None:
    args = parse_args()
    project_root = Path(__file__).resolve().parents[1]

    nyt_cmd = [sys.executable, "scripts/fetch_nyt.py"]
    if args.start:
        nyt_cmd.extend(["--start", args.start])
    if args.end:
        nyt_cmd.extend(["--end", args.end])

    openlibrary_cmd = [sys.executable, "scripts/fetch_openlibrary.py"]
    openlibrary_cmd.extend(["--limit", str(args.limit)])
    if args.refresh_all:
        openlibrary_cmd.append("--refresh-all")
    openlibrary_cmd.extend(["--batch-size", str(args.batch_size)])

    hardcover_cmd = [sys.executable, "scripts/fetch_hardcover.py"]
    hardcover_cmd.extend(["--limit", str(args.limit)])
    if args.refresh_all:
        hardcover_cmd.append("--refresh-all")
    hardcover_cmd.extend(["--batch-size", str(args.batch_size)])

    hardcover_authors_cmd = [sys.executable, "scripts/fetch_hardcover_authors.py"]
    hardcover_authors_cmd.extend(["--limit", str(args.limit)])
    if args.refresh_all:
        hardcover_authors_cmd.append("--refresh-all")
    hardcover_authors_cmd.extend(["--batch-size", str(args.batch_size)])

    gemini_cmd = [sys.executable, "scripts/fetch_gemini_summaries.py"]
    gemini_cmd.extend(["--limit", str(args.limit)])
    if args.refresh_all:
        gemini_cmd.append("--refresh-all")

    try:
        if not args.skip_nyt:
            run_step(nyt_cmd, project_root, "NYT extraction")
        if not args.skip_openlibrary:
            run_step(openlibrary_cmd, project_root, "Open Library enrichment")
        if not args.skip_hardcover:
            run_step(hardcover_cmd, project_root, "Hardcover enrichment")
        if not args.skip_hardcover_authors:
            run_step(hardcover_authors_cmd, project_root, "Hardcover author enrichment")
        if not args.skip_gemini:
            run_step(gemini_cmd, project_root, "Gemini content summary enrichment")
    except subprocess.CalledProcessError as exc:
        print(f"[pipeline] Failed during step: exit_code={exc.returncode}")
        raise SystemExit(exc.returncode) from exc

    print("[pipeline] Completed.")


if __name__ == "__main__":
    main()
