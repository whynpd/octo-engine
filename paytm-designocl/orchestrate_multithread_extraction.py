#!/usr/bin/env python3
"""
Multithreaded orchestrator:
- Reads ticket IDs from CSV (default: 'Sample Data from Design OCL - Sheet1.csv')
- Processes 20 IDs concurrently (configurable)
- For each ticket ID, creates up to four JSON files (only when data exists):
  - ticket_details/ticket_{id}_details.json      (always)
  - ticket_attachments/ticket_{id}_attachments.json  (only if ticket attachments exist)
  - conversations/ticket_{id}_conversations.json     (only if conversations exist)
  - conversation_attachments/ticket_{id}_conversation_attachments.json (only if conversation attachments exist)
- Simultaneously reflects the ticket in migration/ticket_details.json with:
  {
    "Ticket_ID": <int>,
    "Created_when": <ISO string>,
    "Created_by": <requester_id string>,
    "Ticket Attachments": null,
    "Conversations": null,
    "Conversation Attachments": null
  }

Notes:
- Only the necessary per-ticket files are created.
- migration/ticket_details.json is a JSON array; updates are merged with de-dup by Ticket_ID.
"""

import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Tuple
import threading
import time
import logging

from extract_ticket_details import (
    read_ticket_ids_from_csv,
    get_ticket_details,
    fetch_conversations,
)
from download_attachments import download_file, sanitize_filename


OUTPUT_DIRS = {
    'ticket_details': Path('ticket_details'),
    'ticket_attachments': Path('ticket_attachments'),
    'conversations': Path('conversations'),
    'conversation_attachments': Path('conversation_attachments'),
}

CONFIG = {
    'input_csv': 'Sample Data from Design OCL - Sheet1.csv',
    'id_column': 'Ticket ID',
    'workers': 20,  # ticket details workers
    'attachments_workers': 10,  # background workers for ticket_attachments status
    'batch_size': 20,
    'limit': 0,  # 0 = all
    'migration_json': 'migration/ticket_details.json',
}


def ensure_dirs() -> None:
    for p in OUTPUT_DIRS.values():
        p.mkdir(exist_ok=True)
    Path('migration').mkdir(exist_ok=True)


def write_json_atomic(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + '.tmp')
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding='utf-8')
    tmp.replace(path)


def save_per_ticket_files(ticket_id: int, ticket_data: Dict[str, Any], conversations: List[Dict[str, Any]]) -> None:
    # 1) ticket details (always)
    details = {k: v for k, v in ticket_data.items() if k not in ['attachments', 'conversations']}
    details['ticket_id'] = ticket_id
    details_file = OUTPUT_DIRS['ticket_details'] / f"ticket_{ticket_id}_details.json"
    write_json_atomic(details_file, details)

    # 2) ticket attachments if exist
    ticket_attachments = ticket_data.get('attachments') or []
    if ticket_attachments:
        for att in ticket_attachments:
            att['ticket_id'] = ticket_id
        attachments_file = OUTPUT_DIRS['ticket_attachments'] / f"ticket_{ticket_id}_attachments.json"
        write_json_atomic(attachments_file, ticket_attachments)

    # 3) conversations if exist
    if conversations:
        conversations_data: List[Dict[str, Any]] = []
        for conv in conversations:
            conv_data = {k: v for k, v in conv.items() if k != 'attachments'}
            conv_data['ticket_id'] = ticket_id
            conversations_data.append(conv_data)
        conversations_file = OUTPUT_DIRS['conversations'] / f"ticket_{ticket_id}_conversations.json"
        write_json_atomic(conversations_file, conversations_data)

    # 4) conversation attachments if exist
    conversation_attachments: List[Dict[str, Any]] = []
    for conv in conversations or []:
        for att in conv.get('attachments', []) or []:
            att['conversation_id'] = conv.get('id')
            att['ticket_id'] = ticket_id
            att['user_id'] = conv.get('user_id')
            conversation_attachments.append(att)
    if conversation_attachments:
        conv_attachments_file = OUTPUT_DIRS['conversation_attachments'] / f"ticket_{ticket_id}_conversation_attachments.json"
        write_json_atomic(conv_attachments_file, conversation_attachments)


def make_migration_entry(ticket_id: int, ticket_data: Dict[str, Any]) -> Dict[str, Any]:
    created_when = ticket_data.get('created_at') or ""
    requester_id = ticket_data.get('requester_id')
    created_by = str(requester_id) if requester_id is not None else ""
    return {
        "Ticket_ID": ticket_id,
        "Created_when": created_when,
        "Created_by": created_by,
        "Ticket Attachments": None,
        "Conversations": None,
        "Conversation Attachments": None,
    }


def load_migration_json(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text(encoding='utf-8') or '[]')
        if isinstance(data, list):
            return data
    except Exception:
        pass
    return []


def merge_migration_entries(path: Path, new_entries: List[Dict[str, Any]], lock: threading.Lock) -> None:
    added_tids: List[int] = []
    with lock:
        existing = load_migration_json(path)
        seen = {int(rec.get('Ticket_ID')) for rec in existing if isinstance(rec.get('Ticket_ID'), (int,)) or str(rec.get('Ticket_ID')).isdigit()}
        for rec in new_entries:
            tid = rec['Ticket_ID']
            if tid not in seen:
                existing.append(rec)
                seen.add(tid)
                added_tids.append(tid)
        write_json_atomic(path, existing)
    # Log stage: attachments=null at creation time
    for tid in added_tids:
        logging.info(f"[migration] ticket {tid} -> attachments=null")


ATTACHMENTS_FIELD = "Ticket Attachments"


def claim_next_null_attachments(path: Path, lock: threading.Lock) -> int:
    """Find a record with Ticket Attachments == None, set to "I" and return its Ticket_ID. Return -1 if none."""
    with lock:
        records = load_migration_json(path)
        for rec in records:
            if rec.get(ATTACHMENTS_FIELD, None) is None:
                rec[ATTACHMENTS_FIELD] = "I"
                write_json_atomic(path, records)
                logging.info(f"[attachments] ticket {int(rec.get('Ticket_ID'))} -> I (claimed)")
                return int(rec.get("Ticket_ID"))
    return -1


def set_attachments_status(path: Path, lock: threading.Lock, ticket_id: int, value: Any) -> None:
    with lock:
        records = load_migration_json(path)
        for rec in records:
            if int(rec.get("Ticket_ID", -1)) == ticket_id:
                before = rec.get(ATTACHMENTS_FIELD)
                rec[ATTACHMENTS_FIELD] = value
                break
        write_json_atomic(path, records)
    # Log transition after write
    if isinstance(value, dict):
        sample = list(value.items())[:3]
        logging.info(f"[attachments] ticket {ticket_id}: I -> mapping({len(value)}) sample={sample}")
    else:
        logging.info(f"[attachments] ticket {ticket_id}: I -> {value}")


def attachments_worker_loop(mig_path: Path, stop_event: threading.Event, lock: threading.Lock) -> None:
    while True:
        if stop_event.is_set():
            # Final drain: if no more nulls, exit
            tid = claim_next_null_attachments(mig_path, lock)
            if tid == -1:
                return
            # else fall-through to process remaining

        tid = claim_next_null_attachments(mig_path, lock)
        if tid == -1:
            time.sleep(0.5)
            continue

        logging.info(f"[attachments] Claimed ticket {tid} -> I")
        # Download ticket-level attachments and then set final mapping/NA
        att_file = OUTPUT_DIRS['ticket_attachments'] / f"ticket_{tid}_attachments.json"
        # Wait for details writer to produce attachments JSON to avoid race; max ~30s
        max_wait_seconds = 30
        waited = 0
        while not att_file.exists() and waited < max_wait_seconds and not stop_event.is_set():
            time.sleep(0.5)
            waited += 0.5
        if not att_file.exists():
            # After waiting, still no JSON -> treat as NA
            set_attachments_status(mig_path, lock, tid, "NA")
            logging.info(f"[attachments] Finalized ticket {tid} -> NA (no ticket_attachments JSON after {waited}s)")
            continue
        try:
            raw_list = json.loads(att_file.read_text(encoding='utf-8') or '[]')
        except Exception:
            set_attachments_status(mig_path, lock, tid, "NA")
            logging.info(f"[attachments] Finalized ticket {tid} -> NA (malformed ticket_attachments JSON)")
            continue
        if not isinstance(raw_list, list) or len(raw_list) == 0:
            set_attachments_status(mig_path, lock, tid, "NA")
            logging.info(f"[attachments] Finalized ticket {tid} -> NA (no ticket attachments)")
            continue

        # Perform downloads synchronously per ticket; timestamp on each success
        ticket_dir = Path('attachments') / str(tid)
        ticket_dir.mkdir(parents=True, exist_ok=True)
        mapping: Dict[str, str] = {}
        for att in raw_list:
            att_id = att.get('id')
            name = att.get('name')
            url = att.get('url') or att.get('attachment_url')
            if not (att_id and name and url):
                continue
            safe_name = sanitize_filename(str(name))
            dest = ticket_dir / safe_name
            if dest.exists():
                # Already downloaded; mark with current time as seen
                mapping[str(att_id)] = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
                continue
            ok = download_file(url, str(dest))
            if ok:
                mapping[str(att_id)] = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
        if mapping:
            set_attachments_status(mig_path, lock, tid, mapping)
            logging.info(f"[attachments] Finalized ticket {tid} -> mapping({len(mapping)})")
        else:
            set_attachments_status(mig_path, lock, tid, "NA")
            logging.info(f"[attachments] Finalized ticket {tid} -> NA (no successful downloads)")


def evaluate_attachment_status(ticket_id: int) -> Any:
    """Return final status for attachments: mapping {id:timestamp} or "NA"."""
    att_file = OUTPUT_DIRS['ticket_attachments'] / f"ticket_{ticket_id}_attachments.json"
    if att_file.exists():
        try:
            data = json.loads(att_file.read_text(encoding='utf-8') or '[]')
            if isinstance(data, list) and len(data) > 0:
                now_iso = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
                return {str(att.get('id')): now_iso for att in data}
        except Exception:
            pass
    return "NA"


def finalize_in_progress_attachments(mig_path: Path, lock: threading.Lock) -> int:
    """Resolve any lingering 'I' statuses into final mapping/NA. Returns count updated."""
    with lock:
        records = load_migration_json(mig_path)
    updated = 0
    for rec in records:
        if rec.get(ATTACHMENTS_FIELD) == "I":
            tid = int(rec.get("Ticket_ID", -1))
            final_value = evaluate_attachment_status(tid)
            rec[ATTACHMENTS_FIELD] = final_value
            updated += 1
            if isinstance(final_value, dict):
                sample = list(final_value.items())[:3]
                logging.info(f"[finalize] ticket {tid} -> mapping({len(final_value)}) sample={sample}")
            else:
                logging.info(f"[finalize] ticket {tid} -> {final_value}")
    if updated:
        with lock:
            write_json_atomic(mig_path, records)
    return updated


def process_one(ticket_id: int) -> Tuple[int, Dict[str, Any]]:
    details = get_ticket_details(ticket_id)
    if not details:
        return (ticket_id, {})
    convs = fetch_conversations(ticket_id) or []
    save_per_ticket_files(ticket_id, details, convs)
    return (ticket_id, make_migration_entry(ticket_id, details))


def chunked(seq: List[int], size: int) -> List[List[int]]:
    return [seq[i:i+size] for i in range(0, len(seq), size)]


def main() -> int:
    # Log to console and file to make stage transitions easy to inspect
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s [%(threadName)s] %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('orchestrator.log', mode='a', encoding='utf-8')
        ],
        force=True,
    )
    ensure_dirs()

    ticket_ids = read_ticket_ids_from_csv(CONFIG['input_csv'])
    if CONFIG['limit'] and CONFIG['limit'] > 0:
        ticket_ids = ticket_ids[:CONFIG['limit']]
    if not ticket_ids:
        logging.error('No ticket IDs found in CSV')
        return 1

    mig_path = Path(CONFIG['migration_json'])
    write_lock = threading.Lock()
    stop_event = threading.Event()

    # Start attachments background workers
    attach_executor = ThreadPoolExecutor(max_workers=max(1, CONFIG['attachments_workers']))
    attach_futs = [attach_executor.submit(attachments_worker_loop, mig_path, stop_event, write_lock) for _ in range(CONFIG['attachments_workers'])]

    for batch_idx, batch in enumerate(chunked(ticket_ids, CONFIG['batch_size']), start=1):
        logging.info(f"Processing batch {batch_idx} ({len(batch)} tickets)...")
        results: List[Dict[str, Any]] = []
        with ThreadPoolExecutor(max_workers=max(1, CONFIG['workers'])) as exe:
            def _wrapped(tid: int):
                logging.info(f"[details] START ticket {tid}")
                result = process_one(tid)
                logging.info(f"[details] END   ticket {tid}")
                return result
            futs = [exe.submit(_wrapped, tid) for tid in batch]
            for fut in as_completed(futs):
                tid, entry = fut.result()
                if entry:
                    results.append(entry)
        if results:
            merge_migration_entries(mig_path, results, write_lock)
        logging.info(f"Batch {batch_idx} done. Added {len(results)} entries to {mig_path}")

    # Signal attachments workers to finish when no nulls remain
    stop_event.set()
    for fut in attach_futs:
        fut.result()
    attach_executor.shutdown(wait=True)

    # Final sweep to resolve any lingering 'I' statuses
    resolved = finalize_in_progress_attachments(mig_path, write_lock)
    if resolved:
        logging.info(f"Finalized {resolved} lingering 'I' attachment statuses.")

    logging.info('All done.')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

