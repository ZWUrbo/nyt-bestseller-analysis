from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.config import settings
from src.ingest.gemini import (
    TERMINAL_BATCH_STATES,
    GeminiClient,
    GeminiConfig,
    parse_batch_result_line,
)
from src.ingest.repo import GeminiContentSummaryRow, Repo
from src.utils.io import connect_sqlite
from src.utils.logging import get_logger

logger = get_logger("fetch_gemini_summaries")

SUCCEEDED_BATCH_STATES = {
    "BATCH_STATE_SUCCEEDED",
}


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument(
        "--limit",
        type=int,
        default=1000,
        help="Max number of ISBN13 values to include in a new Gemini batch.",
    )
    p.add_argument(
        "--refresh-all",
        action="store_true",
        help="Reprocess all distinct NYT ISBN13 values, not only missing Gemini summary rows.",
    )
    p.add_argument(
        "--jsonl-path",
        type=Path,
        default=None,
        help="Optional explicit path for the generated JSONL batch input file.",
    )
    p.add_argument(
        "--manifest-path",
        type=Path,
        default=None,
        help="Optional batch manifest path. If omitted for a new run, one is created under data/interim/gemini.",
    )
    p.add_argument(
        "--batch-name",
        type=str,
        default=None,
        help="Existing Gemini batch job name to poll/import instead of creating a new one.",
    )
    p.add_argument(
        "--wait",
        action="store_true",
        help="Poll the batch job until it reaches a terminal state and import results if it succeeds.",
    )
    p.add_argument(
        "--poll-interval-seconds",
        type=int,
        default=30,
        help="Polling interval when --wait is enabled.",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    settings.ensure_dirs()
    args.poll_interval_seconds = max(args.poll_interval_seconds, 5)

    if not settings.gemini_api_key:
        raise SystemExit("GEMINI_API_KEY is required for Gemini content summaries.")

    gemini_dir = settings.data_dir / "interim" / "gemini"
    gemini_dir.mkdir(parents=True, exist_ok=True)

    gemini = GeminiClient(
        GeminiConfig(
            api_url=settings.gemini_api_url,
            api_key=settings.gemini_api_key,
            model=settings.gemini_model,
        )
    )

    if args.batch_name:
        manifest_path = resolve_manifest_path(args.manifest_path, gemini_dir, create_new=False)
        manifest = load_manifest(manifest_path) if manifest_path else {}
        manifest["batch_name"] = args.batch_name
        if manifest_path:
            write_manifest(manifest_path, manifest)
        process_existing_batch(gemini, manifest, manifest_path, gemini_dir, wait=args.wait, poll_interval_seconds=args.poll_interval_seconds)
        return

    conn = connect_sqlite(settings.db_path)
    repo = Repo(conn)
    repo.init_schema()

    summary_inputs = repo.list_gemini_summary_inputs(
        limit=args.limit,
        missing_only=not args.refresh_all,
    )
    conn.close()

    if not summary_inputs:
        logger.info("No ISBN13 rows to process.")
        return

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    jsonl_path = (args.jsonl_path or (gemini_dir / f"gemini_batch_requests_{timestamp}.jsonl")).resolve()
    manifest_path = resolve_manifest_path(args.manifest_path, gemini_dir, create_new=True, timestamp=timestamp)

    write_jsonl_input(jsonl_path, gemini, summary_inputs)
    logger.info("Wrote Gemini batch input JSONL: %s (requests=%s)", jsonl_path, len(summary_inputs))

    upload_payload = gemini.upload_jsonl_file(jsonl_path, display_name=jsonl_path.stem)
    input_file_name = ((upload_payload.get("file") or {}).get("name") or "").strip()
    if not input_file_name:
        raise SystemExit("Gemini file upload did not return a file name.")

    batch_payload = gemini.create_batch_job(
        input_file_name=input_file_name,
        display_name=f"gemini-content-summaries-{timestamp}",
    )
    batch_name = (batch_payload.get("name") or "").strip()
    if not batch_name:
        raise SystemExit("Gemini batch creation did not return a batch name.")

    manifest = {
        "created_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "model": settings.gemini_model,
        "batch_name": batch_name,
        "input_file_name": input_file_name,
        "input_jsonl_path": str(jsonl_path),
        "request_count": len(summary_inputs),
    }
    write_manifest(manifest_path, manifest)

    logger.info("Created Gemini batch job: %s", batch_name)
    logger.info("Saved Gemini batch manifest: %s", manifest_path)

    process_existing_batch(
        gemini,
        manifest,
        manifest_path,
        gemini_dir,
        wait=args.wait,
        poll_interval_seconds=args.poll_interval_seconds,
    )


def process_existing_batch(
    gemini: GeminiClient,
    manifest: dict[str, Any],
    manifest_path: Path | None,
    gemini_dir: Path,
    wait: bool,
    poll_interval_seconds: int,
) -> None:
    batch_name = str(manifest.get("batch_name") or "").strip()
    if not batch_name:
        raise SystemExit("A batch name is required to inspect or import an existing Gemini batch.")

    batch_payload = gemini.get_batch_job(batch_name)
    batch_state = extract_batch_state(batch_payload)
    logger.info("Gemini batch state: %s (%s)", batch_state, batch_name)

    while wait and batch_state not in TERMINAL_BATCH_STATES:
        time.sleep(poll_interval_seconds)
        batch_payload = gemini.get_batch_job(batch_name)
        batch_state = extract_batch_state(batch_payload)
        logger.info("Gemini batch state: %s (%s)", batch_state, batch_name)

    manifest["last_seen_state"] = batch_state
    manifest["last_checked_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")

    result_file_name = extract_result_file_name(batch_payload)
    if result_file_name:
        manifest["result_file_name"] = result_file_name

    if manifest_path:
        write_manifest(manifest_path, manifest)

    if batch_state not in SUCCEEDED_BATCH_STATES:
        if batch_state in TERMINAL_BATCH_STATES:
            batch_error = batch_payload.get("error")
            logger.info("Gemini batch ended in state %s | error=%s", batch_state, batch_error)
        else:
            logger.info("Batch is still running. Re-run this script with --batch-name %s --wait to import later.", batch_name)
        return

    if not result_file_name:
        raise SystemExit("Gemini batch succeeded but no result file name was returned.")

    result_path = gemini_dir / f"{sanitize_name(batch_name)}_results.jsonl"
    result_bytes = gemini.download_file_bytes(result_file_name)
    result_path.write_bytes(result_bytes)
    logger.info("Downloaded Gemini batch results: %s", result_path)

    manifest["result_jsonl_path"] = str(result_path.resolve())
    if manifest_path:
        write_manifest(manifest_path, manifest)

    import_result_jsonl(result_path)


def write_jsonl_input(jsonl_path: Path, gemini: GeminiClient, summary_inputs: list[Any]) -> None:
    jsonl_path.parent.mkdir(parents=True, exist_ok=True)
    with jsonl_path.open("w", encoding="utf-8") as fh:
        for row in summary_inputs:
            fh.write(json.dumps(gemini.build_batch_request_line(row), ensure_ascii=False) + "\n")


def import_result_jsonl(result_path: Path) -> None:
    raw_lines = [line for line in result_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    if not raw_lines:
        logger.info("Gemini result JSONL was empty: %s", result_path)
        return

    parsed_lines = [json.loads(line) for line in raw_lines]

    conn = connect_sqlite(settings.db_path)
    repo = Repo(conn)
    repo.init_schema()

    rows: list[GeminiContentSummaryRow] = []
    failures = 0
    for line, parsed in zip(raw_lines, parsed_lines):
        isbn13 = extract_line_key(parsed)
        if not isbn13:
            failures += 1
            logger.info("Skipping Gemini result line without key: %s", line[:200])
            continue
        summary_row = parse_batch_result_line(line)
        if summary_row.last_error:
            failures += 1
        rows.append(summary_row)

    upserts = repo.upsert_gemini_content_summaries(rows)
    conn.close()

    logger.info(
        "Imported Gemini batch results: rows=%s upserts=%s failures=%s",
        len(rows),
        upserts,
        failures,
    )


def extract_line_key(parsed: dict[str, Any]) -> str:
    key = parsed.get("key")
    if isinstance(key, str) and key.strip():
        return key.strip()
    metadata = parsed.get("metadata")
    if isinstance(metadata, dict):
        metadata_key = metadata.get("key")
        if isinstance(metadata_key, str) and metadata_key.strip():
            return metadata_key.strip()
    return ""


def extract_batch_state(batch_payload: dict[str, Any]) -> str:
    for candidate in (
        batch_payload.get("state"),
        batch_payload.get("metadata"),
        batch_payload.get("response"),
    ):
        if isinstance(candidate, str):
            return candidate.strip()
        if isinstance(candidate, dict):
            for key in ("state", "name"):
                state_name = candidate.get(key)
                if isinstance(state_name, str):
                    return state_name.strip()
    return "BATCH_STATE_UNSPECIFIED"


def extract_result_file_name(batch_payload: dict[str, Any]) -> str | None:
    for candidate in (
        batch_payload.get("dest"),
        batch_payload.get("output"),
        batch_payload.get("metadata"),
        batch_payload.get("response"),
    ):
        if not isinstance(candidate, dict):
            continue

        file_name = candidate.get("fileName") or candidate.get("file_name")
        if isinstance(file_name, str) and file_name.strip():
            return file_name.strip()

        output = candidate.get("output")
        if isinstance(output, dict):
            nested_file_name = output.get("responsesFile") or output.get("responses_file")
            if isinstance(nested_file_name, str) and nested_file_name.strip():
                return nested_file_name.strip()

        responses_file = candidate.get("responsesFile") or candidate.get("responses_file")
        if isinstance(responses_file, str) and responses_file.strip():
            return responses_file.strip()

    return None


def resolve_manifest_path(
    manifest_path: Path | None,
    gemini_dir: Path,
    create_new: bool,
    timestamp: str | None = None,
) -> Path | None:
    if manifest_path is not None:
        return manifest_path.resolve()
    if not create_new:
        return None
    if not timestamp:
        raise ValueError("timestamp is required when creating a new manifest path")
    return (gemini_dir / f"gemini_batch_manifest_{timestamp}.json").resolve()


def load_manifest(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise SystemExit(f"Manifest file does not exist: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def write_manifest(path: Path, manifest: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")


def sanitize_name(value: str) -> str:
    return value.replace("/", "_").replace(":", "_")


if __name__ == "__main__":
    main()
