#!/usr/bin/env python3
"""
Conversation Attachments Consumer

Runs independently with N workers. Continuously claims records where
`Conversation Attachments` is null in `migration/ticket_details.json`, marks them
as in-progress ("I"), checks the conversations status, and either:
1. Copies "NA" from conversations field if conversations is "NA"
2. Processes conversation attachment data from `conversation_attachments/ticket_{id}_conversation_attachments.json`
   and downloads attachments, updating status to either a mapping {id: timestamp_when_download_completed} or "NA" if none.

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
from attachment_url_tracker import track_attachment_download
from storage_config import get_attachment_storage_url, get_storage_type
from migration_store import (
    claim_next_null_conversation_attachments,
    set_conversation_attachments_status,
    get_conversations_status,
    finalize_in_progress_conversation_attachments,
)


OUTPUT_DIRS = {
    'complete_ticket_data': Path('complete_ticket_data'),
    'attachments': Path('attachments'),
}


CONFIG = {
    'workers': 7,
    'max_wait_for_json_seconds': 10,
}


def evaluate_conversation_attachment_status(ticket_id: int) -> Any:
    """Check if conversation attachments JSON exists and has data, return mapping or NA."""
    conv_att_file = OUTPUT_DIRS['conversation_attachments'] / f"ticket_{ticket_id}_conversation_attachments.json"
    if conv_att_file.exists():
        try:
            data = json.loads(conv_att_file.read_text(encoding='utf-8') or '[]')
            if isinstance(data, list) and len(data) > 0:
                now_iso = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
                return {str(att.get('id', 'unknown')): now_iso for att in data if att.get('id')}
        except Exception:
            pass
    return "NA"


def process_one_conversation_attachment(ticket_id: int) -> None:
    logging.info(f"[conv_attachments] Claimed ticket {ticket_id} -> I")
    
    # First check conversations status
    conversations_status = get_conversations_status(ticket_id)
    if conversations_status == "NA":
        # Copy NA from conversations field
        set_conversation_attachments_status(ticket_id, "NA")
        logging.info(f"[conv_attachments] Finalized ticket {ticket_id} -> NA (conversations is NA)")
        return
    
    # Read from consolidated complete ticket data file
    complete_file = OUTPUT_DIRS['complete_ticket_data'] / f"ticket_{ticket_id}_complete.json"

    waited = 0.0
    while not complete_file.exists() and waited < CONFIG['max_wait_for_json_seconds']:
        time.sleep(0.5)
        waited += 0.5
    
    if not complete_file.exists():
        set_conversation_attachments_status(ticket_id, "NA")
        logging.info(f"[conv_attachments] Finalized ticket {ticket_id} -> NA (complete file not found after {waited}s)")
        return

    try:
        complete_data = json.loads(complete_file.read_text(encoding='utf-8') or '{}')
        # Extract conversation attachments from all conversations
        raw_list = []
        conversations = complete_data.get('conversations', [])
        for conv in conversations:
            for att in conv.get('attachments', []):
                att['conversation_id'] = conv.get('id')
                att['ticket_id'] = ticket_id
                att['user_id'] = conv.get('user_id')
                raw_list.append(att)
    except Exception:
        set_conversation_attachments_status(ticket_id, "NA")
        logging.info(f"[conv_attachments] Finalized ticket {ticket_id} -> NA (malformed JSON)")
        return

    if not isinstance(raw_list, list) or len(raw_list) == 0:
        set_conversation_attachments_status(ticket_id, "NA")
        logging.info(f"[conv_attachments] Finalized ticket {ticket_id} -> NA (no conversation attachments)")
        return

    def attempt_downloads(attachments_list: List[Dict[str, Any]]) -> Dict[str, str]:
        # Create ticket directory for conversation attachments
        ticket_dir = Path('attachments') / str(ticket_id)
        ticket_dir.mkdir(parents=True, exist_ok=True)
        
        mapping_local: Dict[str, str] = {}
        for att in attachments_list:
            att_id = att.get('id')
            name = att.get('name')
            url = att.get('url') or att.get('attachment_url')
            if not (att_id and name and url):
                continue
            safe_name = sanitize_filename(str(name))
            # Prefix conversation attachments to distinguish from ticket attachments
            safe_name = f"conv_{safe_name}"
            dest = ticket_dir / safe_name
            if dest.exists():
                mapping_local[str(att_id)] = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
                # Track existing file (already downloaded)
                try:
                    storage_url = get_attachment_storage_url(str(dest), ticket_id, name)
                    track_attachment_download(
                        ticket_id=ticket_id,
                        freshdesk_url=url,
                        saved_location=storage_url,
                        attachment_id=str(att_id),
                        attachment_name=name,
                        attachment_type="conversation_attachment",
                        storage_type=get_storage_type()
                    )
                except Exception as e:
                    logging.warning(f"[conv_attachments] Failed to track existing file for {att_id}: {e}")
                continue
            ok = download_file(url, str(dest))
            if ok:
                mapping_local[str(att_id)] = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
                # Track the attachment download
                try:
                    storage_url = get_attachment_storage_url(str(dest), ticket_id, name)
                    track_attachment_download(
                        ticket_id=ticket_id,
                        freshdesk_url=url,
                        saved_location=storage_url,
                        attachment_id=str(att_id),
                        attachment_name=name,
                        attachment_type="conversation_attachment",
                        storage_type=get_storage_type()
                    )
                except Exception as e:
                    logging.warning(f"[conv_attachments] Failed to track download for {att_id}: {e}")
        return mapping_local

    mapping: Dict[str, str] = attempt_downloads(raw_list)

    # If nothing succeeded, conversation attachment URLs may have expired. Refresh once.
    if not mapping:
        try:
            # Refresh conversation attachments by re-fetching complete data
            from extract_ticket_details import get_complete_ticket_data
            latest = get_complete_ticket_data(ticket_id) or {}
            conversations = latest.get('conversations', []) or []
            refreshed_attachments = []
            for conv in conversations:
                for att in conv.get('attachments', []) or []:
                    att['conversation_id'] = conv.get('id')
                    att['ticket_id'] = ticket_id
                    att['user_id'] = conv.get('user_id')
                    refreshed_attachments.append(att)
            # Persist refreshed JSON for traceability (update complete file)
            try:
                new_file = OUTPUT_DIRS['complete_ticket_data'] / f"ticket_{ticket_id}_complete.json"
                new_file.write_text(json.dumps(latest, ensure_ascii=False, indent=2), encoding='utf-8')
            except Exception:
                pass
            mapping = attempt_downloads(refreshed_attachments)
        except Exception as exc:
            logging.warning(f"[conv_attachments] refresh failed for {ticket_id}: {exc}")

    if mapping:
        set_conversation_attachments_status(ticket_id, mapping)
        logging.info(f"[conv_attachments] Finalized ticket {ticket_id} -> mapping({len(mapping)})")
    else:
        set_conversation_attachments_status(ticket_id, "NA")
        logging.info(f"[conv_attachments] Finalized ticket {ticket_id} -> NA (no successful downloads)")


def worker_loop(stop_flag: List[bool]) -> None:
    consecutive_empty_checks = 0
    max_empty_checks = 60  # Stop after 60 consecutive empty checks (30 seconds with 0.5s sleep)
    
    while not stop_flag[0]:
        tid = claim_next_null_conversation_attachments()
        if tid == -1:
            consecutive_empty_checks += 1
            if consecutive_empty_checks >= max_empty_checks:
                logging.info(f"[conv_attachments] Worker stopping after {consecutive_empty_checks} consecutive empty checks")
                break
            time.sleep(0.5)
            continue
        
        # Reset counter when work is found
        consecutive_empty_checks = 0
        
        try:
            process_one_conversation_attachment(tid)
        except Exception as exc:
            logging.exception(f"[conv_attachments] Error processing {tid}: {exc}")
            set_conversation_attachments_status(tid, "NA")


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s [%(threadName)s] %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('conversation_attachments_consumer.log', mode='a', encoding='utf-8')
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
    updated = finalize_in_progress_conversation_attachments(evaluate_conversation_attachment_status)
    if updated:
        logging.info(f"[finalize] Updated {updated} lingering 'I' conversation attachment statuses")

    logging.info('Conversation Attachments Consumer done.')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())