#!/usr/bin/env python3
"""
Attachments Consumer

Runs independently with N workers. Continuously claims records where
`Ticket Attachments` is null in `migration/ticket_details.json`, marks them
as in-progress ("I"), downloads ticket-level attachments from
`ticket_attachments/ticket_{id}_attachments.json`, and finally updates the
status to either a mapping {id: timestamp_when_download_completed} or "NA" if none.

It also provides a finalize pass to resolve lingering "I" into mapping/NA.
"""

from __future__ import annotations

import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Dict, List

from download_attachments import download_file, sanitize_filename
from extract_ticket_details import get_complete_ticket_data
from migration_store import (
    claim_next_null_attachments,
    set_attachments_status,
    finalize_in_progress,
)


OUTPUT_DIRS = {
    'complete_ticket_data': Path('complete_ticket_data'),
    'attachments': Path('attachments'),
}


CONFIG = {
    'workers': 10,
    'max_wait_for_json_seconds': 10,
}


def evaluate_status(ticket_id: int) -> Any:
    att_file = OUTPUT_DIRS['ticket_attachments'] / f"ticket_{ticket_id}_attachments.json"
    if att_file.exists():
        try:
            data = json.loads(att_file.read_text(encoding='utf-8') or '[]')
            if isinstance(data, list) and len(data) > 0:
                now_iso = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
                return {str(att.get('id')): now_iso for att in data if att.get('id')}
        except Exception:
            pass
    return "NA"


def process_one(ticket_id: int) -> None:
    logging.info(f"[attachments] Claimed ticket {ticket_id} -> I")
    # Read from consolidated complete ticket data file
    complete_file = OUTPUT_DIRS['complete_ticket_data'] / f"ticket_{ticket_id}_complete.json"

    waited = 0.0
    while not complete_file.exists() and waited < CONFIG['max_wait_for_json_seconds']:
        time.sleep(0.5)
        waited += 0.5
    if not complete_file.exists():
        set_attachments_status(ticket_id, "NA")
        logging.info(f"[attachments] Finalized ticket {ticket_id} -> NA (complete file not found after {waited}s)")
        return

    try:
        complete_data = json.loads(complete_file.read_text(encoding='utf-8') or '{}')
        # Extract ticket-level attachments (not conversation attachments)
        all_attachments = complete_data.get('attachments', [])
        raw_list = [att for att in all_attachments if att.get('type') == 'ticket_attachment']
    except Exception:
        set_attachments_status(ticket_id, "NA")
        logging.info(f"[attachments] Finalized ticket {ticket_id} -> NA (malformed JSON)")
        return

    if not isinstance(raw_list, list) or len(raw_list) == 0:
        set_attachments_status(ticket_id, "NA")
        logging.info(f"[attachments] Finalized ticket {ticket_id} -> NA (no ticket attachments)")
        return

    ticket_dir = Path('attachments') / str(ticket_id)
    ticket_dir.mkdir(parents=True, exist_ok=True)

    def attempt_downloads(attachments_list: List[Dict[str, Any]]) -> Dict[str, str]:
        mapping_local: Dict[str, str] = {}
        for att in attachments_list:
            att_id = att.get('id')
            name = att.get('name')
            url = att.get('url') or att.get('attachment_url')
            if not (att_id and name and url):
                continue
            safe_name = sanitize_filename(str(name))
            dest = ticket_dir / safe_name
            if dest.exists():
                mapping_local[str(att_id)] = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
                continue
            ok = download_file(url, str(dest))
            if ok:
                mapping_local[str(att_id)] = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
        return mapping_local

    mapping: Dict[str, str] = attempt_downloads(raw_list)

    # If nothing succeeded, Freshdesk pre-signed URLs may have expired. Refresh once.
    if not mapping:
        try:
            latest = get_complete_ticket_data(ticket_id) or {}
            all_refreshed = latest.get('attachments') or []
            refreshed = [att for att in all_refreshed if att.get('type') == 'ticket_attachment']
            # Persist refreshed JSON for traceability (update complete file)
            try:
                new_file = OUTPUT_DIRS['complete_ticket_data'] / f"ticket_{ticket_id}_complete.json"
                new_file.write_text(json.dumps(latest, ensure_ascii=False, indent=2), encoding='utf-8')
            except Exception:
                pass
            mapping = attempt_downloads(refreshed)
        except Exception as exc:
            logging.warning(f"[attachments] refresh failed for {ticket_id}: {exc}")

    if mapping:
        set_attachments_status(ticket_id, mapping)
        logging.info(f"[attachments] Finalized ticket {ticket_id} -> mapping({len(mapping)})")
    else:
        set_attachments_status(ticket_id, "NA")
        logging.info(f"[attachments] Finalized ticket {ticket_id} -> NA (no successful downloads)")


def worker_loop(stop_flag: List[bool]) -> None:
    consecutive_empty_checks = 0
    max_empty_checks = 60  # Stop after 60 consecutive empty checks (30 seconds with 0.5s sleep)
    
    while not stop_flag[0]:
        tid = claim_next_null_attachments()
        if tid == -1:
            consecutive_empty_checks += 1
            if consecutive_empty_checks >= max_empty_checks:
                logging.info(f"[attachments] Worker stopping after {consecutive_empty_checks} consecutive empty checks")
                break
            time.sleep(0.5)
            continue
        
        # Reset counter when work is found
        consecutive_empty_checks = 0
        
        try:
            process_one(tid)
        except Exception as exc:
            logging.exception(f"[attachments] Error processing {tid}: {exc}")
            set_attachments_status(tid, "NA")


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s [%(threadName)s] %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('attachments_consumer.log', mode='a', encoding='utf-8')
        ],
        force=True,
    )

    stop_flag = [False]
    with ThreadPoolExecutor(max_workers=max(1, CONFIG['workers'])) as executor:
        futures = [executor.submit(worker_loop, stop_flag) for _ in range(CONFIG['workers'])]
        try:
            for fut in futures:
                fut.result()
        except KeyboardInterrupt:
            stop_flag[0] = True

    # Finalization pass
    updated = finalize_in_progress(evaluate_status)
    if updated:
        logging.info(f"[finalize] Updated {updated} lingering 'I' statuses")

    logging.info('Attachments Consumer done.')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

