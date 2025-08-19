#!/usr/bin/env python3
"""
Details Producer

Runs independently to extract ticket details and conversations, and writes per-ticket
JSON files conditionally. Also appends skeleton records to `migration/ticket_details.json`
with attachments/conversations fields set to null.

Configured via the CONFIG dictionary below (no CLI args).
"""

from __future__ import annotations

import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Tuple

from extract_ticket_details import (
    read_ticket_ids_from_csv,
    get_complete_ticket_data,
    save_complete_ticket_data,
)
from migration_store import merge_entries


OUTPUT_DIRS = {
    'ticket_details': Path('ticket_details'),
    'ticket_attachments': Path('ticket_attachments'),
    'conversations': Path('conversations'),
    'conversation_attachments': Path('conversation_attachments'),
    'complete_ticket_data': Path('complete_ticket_data'),  # New consolidated output
}


CONFIG = {
    'input_csv': 'Sample Data from Design OCL - Sheet1.csv',
    'workers': 20,
    'batch_size': 20,
    'limit': 0,  # 0 = all
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


def save_per_ticket_files(ticket_id: int, complete_data: Dict[str, Any]) -> None:
    """
    Save ticket data in both formats:
    1. Complete consolidated format (NEW)
    2. Separate files format (BACKWARD COMPATIBILITY)
    """
    # NEW: Save complete consolidated data
    save_complete_ticket_data(ticket_id, complete_data, OUTPUT_DIRS['complete_ticket_data'])
    
    # BACKWARD COMPATIBILITY: Extract and save separate files for existing consumers
    conversations = complete_data.get('conversations', [])
    
    # 1) ticket details (always) - exclude conversations for separate file
    details = {k: v for k, v in complete_data.items() if k not in ['conversations']}
    details['ticket_id'] = ticket_id
    details_file = OUTPUT_DIRS['ticket_details'] / f"ticket_{ticket_id}_details.json"
    write_json_atomic(details_file, details)

    # 2) ticket attachments if exist
    ticket_attachments = complete_data.get('attachments') or []
    if ticket_attachments:
        # Only save ticket-level attachments (exclude conversation attachments)
        ticket_only_attachments = [att for att in ticket_attachments if att.get('type') == 'ticket_attachment']
        if ticket_only_attachments:
            for att in ticket_only_attachments:
                att['ticket_id'] = ticket_id
            attachments_file = OUTPUT_DIRS['ticket_attachments'] / f"ticket_{ticket_id}_attachments.json"
            write_json_atomic(attachments_file, ticket_only_attachments)

    # 3) conversations if exist (without attachments for separate file)
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


def make_migration_entry(ticket_id: int, complete_data: Dict[str, Any]) -> Dict[str, Any]:
    created_when = complete_data.get('created_at') or ""
    requester_id = complete_data.get('requester_id')
    created_by = str(requester_id) if requester_id is not None else ""
    return {
        "Ticket_ID": ticket_id,
        "Created_when": created_when,
        "Created_by": created_by,
        "Ticket Attachments": None,
        "Conversations": None,
        "Conversation Attachments": None,
    }


def chunked(seq: List[int], size: int) -> List[List[int]]:
    return [seq[i:i+size] for i in range(0, len(seq), size)]


def process_one(ticket_id: int) -> Tuple[int, Dict[str, Any]]:
    # NEW: Use consolidated single API call
    complete_data = get_complete_ticket_data(ticket_id)
    if not complete_data:
        return (ticket_id, {})
    
    # Save in both formats (consolidated + backward compatibility)
    save_per_ticket_files(ticket_id, complete_data)
    
    return (ticket_id, make_migration_entry(ticket_id, complete_data))


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s [%(threadName)s] %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('details_producer.log', mode='a', encoding='utf-8')
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
            added = merge_entries(results)
            for tid in added:
                logging.info(f"[migration] ticket {tid} -> attachments=null")
        logging.info(f"Batch {batch_idx} done. Added {len(results)} entries to migration store")

    logging.info('Details Producer done.')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

