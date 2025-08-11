#!/usr/bin/env python3
"""
Seed ticket_details.json with skeleton records using multithreading.

Behavior:
- Read Ticket IDs from a CSV (default: 'Sample Data from Design OCL - Sheet1.csv')
- Process up to N ticket IDs (default: 20) in parallel (default workers: 20)
- Append records to migration/ticket_details.json (JSON array)
- Record schema:
  {
    "Ticket_ID": <int>,
    "Created_when": "",
    "Created_by": "",
    "Ticket Attachments": null,
    "Conversations": null,
    "Conversation Attachments": null
  }

Notes:
- The three fields (Ticket Attachments, Conversations, Conversation Attachments) are initialized as null
- Created_when and Created_by are empty strings for now
"""

import argparse
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import csv
from typing import Any, Dict, List, Set, Optional, Tuple
import os

import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter, Retry


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Seed migration/ticket_details.json with skeleton records using multithreading.")
    parser.add_argument('--input', type=str, default='Sample Data from Design OCL - Sheet1.csv', help='Path to input CSV containing Ticket ID column')
    parser.add_argument('--id-column', type=str, default='Ticket ID', help='Column name containing ticket IDs')
    parser.add_argument('--output', type=str, default='migration/ticket_details.json', help='Output JSON file (array)')
    parser.add_argument('--limit', type=int, default=20, help='Number of tickets to process')
    parser.add_argument('--workers', type=int, default=20, help='Number of concurrent worker threads')
    return parser.parse_args()


def create_requests_session() -> requests.Session:
    session = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=1.0,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"]
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


def read_ticket_ids(input_csv: Path, id_column: str) -> List[int]:
    with input_csv.open('r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        if id_column not in (reader.fieldnames or []):
            raise ValueError(f"Input CSV must contain column: {id_column}")
        ids: List[int] = []
        for row in reader:
            raw = (row.get(id_column) or '').strip()
            if not raw:
                continue
            try:
                ids.append(int(raw))
            except ValueError:
                # Skip non-integer
                continue
    return ids


def fetch_ticket_fields(session: requests.Session, domain: str, api_key: str, ticket_id: int) -> Tuple[str, str]:
    """Return (created_when_iso, created_by_identifier). Fallbacks to empty strings."""
    url = f"https://{domain}/api/v2/tickets/{ticket_id}"
    try:
        resp = session.get(url, auth=(api_key, 'X'), timeout=30)
    except requests.RequestException:
        return ("", "")

    if resp.status_code != 200:
        return ("", "")

    try:
        data = resp.json()
    except ValueError:
        return ("", "")

    created_when = data.get('created_at') or ""
    # Use requester_id as the author identifier of the ticket
    requester_id = data.get('requester_id')
    created_by = str(requester_id) if requester_id is not None else ""
    return (created_when, created_by)


def build_record(ticket_id: int, created_when: str, created_by: str) -> Dict[str, Any]:
    return {
        "Ticket_ID": ticket_id,
        "Created_when": created_when,
        "Created_by": created_by,
        "Ticket Attachments": None,
        "Conversations": None,
        "Conversation Attachments": None,
    }


def load_existing(output_path: Path) -> List[Dict[str, Any]]:
    if not output_path.exists():
        return []
    try:
        data = json.loads(output_path.read_text(encoding='utf-8') or '[]')
        if isinstance(data, list):
            return data
    except Exception:
        pass
    # Fallback to empty array if corrupted/unexpected
    return []


def write_json_atomic(output_path: Path, data: List[Dict[str, Any]]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = output_path.with_suffix(output_path.suffix + '.tmp')
    tmp_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding='utf-8')
    tmp_path.replace(output_path)


def main() -> int:
    args = parse_args()
    input_csv = Path(args.input)
    output_path = Path(args.output)

    if not input_csv.exists():
        print(f"Input CSV not found: {input_csv}")
        return 1

    load_dotenv()
    domain = os.getenv('FRESHDESK_DOMAIN')
    api_key = os.getenv('FRESHDESK_API_KEY')
    if not domain or not api_key:
        print("FRESHDESK_DOMAIN and FRESHDESK_API_KEY must be set")
        return 1

    ticket_ids = read_ticket_ids(input_csv, args.id_column)
    if not ticket_ids:
        print("No ticket IDs found.")
        return 0

    ticket_ids = ticket_ids[: max(0, args.limit)]

    # Fetch ticket fields concurrently
    session = create_requests_session()
    records: List[Dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as executor:
        future_to_id = {
            executor.submit(fetch_ticket_fields, session, domain, api_key, tid): tid
            for tid in ticket_ids
        }
        for future in as_completed(future_to_id):
            tid = future_to_id[future]
            created_when, created_by = ("", "")
            try:
                created_when, created_by = future.result()
            except Exception:
                pass
            records.append(build_record(tid, created_when, created_by))

    # Merge with existing, de-duplicate by Ticket_ID (existing wins)
    existing = load_existing(output_path)
    seen: Set[int] = set()
    merged: List[Dict[str, Any]] = []
    for rec in existing:
        try:
            tid = int(rec.get("Ticket_ID"))
        except Exception:
            continue
        if tid in seen:
            continue
        seen.add(tid)
        merged.append(rec)

    for rec in records:
        tid = rec["Ticket_ID"]
        if tid in seen:
            continue
        seen.add(tid)
        merged.append(rec)

    write_json_atomic(output_path, merged)
    print(f"Seeded {len(records)} records. Total records now: {len(merged)}. Output: {output_path}")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

