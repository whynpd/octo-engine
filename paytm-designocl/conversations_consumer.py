#!/usr/bin/env python3
"""
Conversations Consumer

Runs independently with N workers. Continuously claims records where
`Conversations` is null in `migration/ticket_details.json`, marks them
as in-progress ("I"), checks for conversation data from
`conversations/ticket_{id}_conversations.json`, and finally updates the
status to either a mapping {id: timestamp_when_processing_completed} or "NA" if none.

It also provides a finalize pass to resolve lingering "I" into mapping/NA.
"""

from __future__ import annotations

import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Dict, List

from migration_store import (
    claim_next_null_conversations,
    set_conversations_status,
    finalize_in_progress_conversations,
)


OUTPUT_DIRS = {
    'complete_ticket_data': Path('complete_ticket_data'),
}


CONFIG = {
    'workers': 10,
    'max_wait_for_json_seconds': 30,
}


def evaluate_conversation_status(ticket_id: int) -> Any:
    """Check if conversations JSON exists and has data, return mapping or NA."""
    conv_file = OUTPUT_DIRS['conversations'] / f"ticket_{ticket_id}_conversations.json"
    if conv_file.exists():
        try:
            data = json.loads(conv_file.read_text(encoding='utf-8') or '[]')
            if isinstance(data, list) and len(data) > 0:
                now_iso = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
                return {str(conv.get('id', 'unknown')): now_iso for conv in data if conv.get('id')}
        except Exception:
            pass
    return "NA"


def process_one_conversation(ticket_id: int) -> None:
    logging.info(f"[conversations] Claimed ticket {ticket_id} -> I")
    # Read from consolidated complete ticket data file
    complete_file = OUTPUT_DIRS['complete_ticket_data'] / f"ticket_{ticket_id}_complete.json"

    waited = 0.0
    while not complete_file.exists() and waited < CONFIG['max_wait_for_json_seconds']:
        time.sleep(0.5)
        waited += 0.5
    
    if not complete_file.exists():
        set_conversations_status(ticket_id, "NA")
        logging.info(f"[conversations] Finalized ticket {ticket_id} -> NA (complete file not found after {waited}s)")
        return

    try:
        complete_data = json.loads(complete_file.read_text(encoding='utf-8') or '{}')
        conversations = complete_data.get('conversations', [])
    except Exception:
        set_conversations_status(ticket_id, "NA")
        logging.info(f"[conversations] Finalized ticket {ticket_id} -> NA (malformed JSON)")
        return

    if not isinstance(conversations, list) or len(conversations) == 0:
        set_conversations_status(ticket_id, "NA")
        logging.info(f"[conversations] Finalized ticket {ticket_id} -> NA (no conversations)")
        return

    # Create mapping with current timestamp for each conversation ID
    now_iso = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
    mapping: Dict[str, str] = {}
    for conv in conversations:
        conv_id = conv.get('id')
        if conv_id:
            mapping[str(conv_id)] = now_iso

    if mapping:
        set_conversations_status(ticket_id, mapping)
        logging.info(f"[conversations] Finalized ticket {ticket_id} -> mapping({len(mapping)})")
    else:
        set_conversations_status(ticket_id, "NA")
        logging.info(f"[conversations] Finalized ticket {ticket_id} -> NA (no valid conversation IDs)")


def worker_loop(stop_flag: List[bool]) -> None:
    consecutive_empty_checks = 0
    max_empty_checks = 60  # Stop after 60 consecutive empty checks (30 seconds with 0.5s sleep)
    
    while not stop_flag[0]:
        tid = claim_next_null_conversations()
        if tid == -1:
            consecutive_empty_checks += 1
            if consecutive_empty_checks >= max_empty_checks:
                logging.info(f"[conversations] Worker stopping after {consecutive_empty_checks} consecutive empty checks")
                break
            time.sleep(0.5)
            continue
        
        # Reset counter when work is found
        consecutive_empty_checks = 0
        
        try:
            process_one_conversation(tid)
        except Exception as exc:
            logging.exception(f"[conversations] Error processing {tid}: {exc}")
            set_conversations_status(tid, "NA")


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s [%(threadName)s] %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('conversations_consumer.log', mode='a', encoding='utf-8')
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
    updated = finalize_in_progress_conversations(evaluate_conversation_status)
    if updated:
        logging.info(f"[finalize] Updated {updated} lingering 'I' conversation statuses")

    logging.info('Conversations Consumer done.')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())