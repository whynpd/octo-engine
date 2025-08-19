#!/usr/bin/env python3
"""
Attachment URL Tracker
======================
Tracks attachment URLs and their local download paths in attachment_url.json
"""

import json
import logging
from pathlib import Path
from typing import List, Dict, Any
import fcntl
from datetime import datetime

# Setup logging
logger = logging.getLogger(__name__)

ATTACHMENT_URL_JSON_PATH = Path("migration/attachment_url.json")
ATTACHMENT_URL_LOCK_PATH = Path("migration/attachment_url.json.lock")

class AttachmentURLTracker:
    """Thread-safe tracker for attachment URLs and download paths"""
    
    def __init__(self):
        self._ensure_paths()
    
    def _ensure_paths(self):
        """Ensure migration directory and lock file exist"""
        ATTACHMENT_URL_JSON_PATH.parent.mkdir(parents=True, exist_ok=True)
        ATTACHMENT_URL_LOCK_PATH.touch(exist_ok=True)
    
    def _read_records_unlocked(self) -> List[Dict[str, Any]]:
        """Read attachment URL records without locking"""
        if ATTACHMENT_URL_JSON_PATH.exists():
            try:
                content = ATTACHMENT_URL_JSON_PATH.read_text(encoding='utf-8')
                if content.strip():
                    return json.loads(content)
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                logger.warning(f"Could not parse {ATTACHMENT_URL_JSON_PATH}: {e}")
        return []
    
    def _write_records_unlocked(self, records: List[Dict[str, Any]]):
        """Write attachment URL records without locking"""
        tmp = ATTACHMENT_URL_JSON_PATH.with_suffix(ATTACHMENT_URL_JSON_PATH.suffix + ".tmp")
        tmp.write_text(json.dumps(records, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(ATTACHMENT_URL_JSON_PATH)
    
    def add_attachment_record(self, ticket_id: int, freshdesk_url: str, saved_location: str, 
                            attachment_id: str = None, attachment_name: str = None, 
                            attachment_type: str = None, storage_type: str = "local"):
        """
        Add an attachment record with thread-safe file locking
        
        Args:
            ticket_id: The ticket ID
            freshdesk_url: Original Freshdesk URL
            saved_location: Where file was saved (local path, S3 URL, or server URL)
            attachment_id: Freshdesk attachment ID (optional)
            attachment_name: Original attachment name (optional)
            attachment_type: 'ticket_attachment' or 'conversation_attachment' (optional)
            storage_type: 'local', 's3', 'server', etc. (optional)
        """
        record = {
            "ticket_id": ticket_id,
            "freshdesk_url": freshdesk_url,
            "saved_location": saved_location,
            "storage_type": storage_type,
            "recorded_at": datetime.now().isoformat()
        }
        
        # Add optional fields if provided
        if attachment_id:
            record["attachment_id"] = attachment_id
        if attachment_name:
            record["attachment_name"] = attachment_name
        if attachment_type:
            record["attachment_type"] = attachment_type
        
        # Thread-safe file update
        with FileLock(ATTACHMENT_URL_LOCK_PATH):
            records = self._read_records_unlocked()
            records.append(record)
            self._write_records_unlocked(records)
        
        logger.info(f"[attachment_tracker] Added record for ticket {ticket_id}: {attachment_name or 'unknown'}")
    
    def add_batch_records(self, records: List[Dict[str, Any]]):
        """Add multiple attachment records in a single transaction"""
        if not records:
            return
        
        # Add timestamp to all records
        for record in records:
            if "recorded_at" not in record:
                record["recorded_at"] = datetime.now().isoformat()
        
        with FileLock(ATTACHMENT_URL_LOCK_PATH):
            existing_records = self._read_records_unlocked()
            existing_records.extend(records)
            self._write_records_unlocked(existing_records)
        
        logger.info(f"[attachment_tracker] Added {len(records)} attachment records")
    
    def get_records_by_ticket(self, ticket_id: int) -> List[Dict[str, Any]]:
        """Get all attachment records for a specific ticket"""
        with FileLock(ATTACHMENT_URL_LOCK_PATH):
            records = self._read_records_unlocked()
            return [r for r in records if r.get("ticket_id") == ticket_id]
    
    def get_all_records(self) -> List[Dict[str, Any]]:
        """Get all attachment records"""
        with FileLock(ATTACHMENT_URL_LOCK_PATH):
            return self._read_records_unlocked()
    
    def generate_summary(self) -> Dict[str, Any]:
        """Generate summary statistics about tracked attachments"""
        records = self.get_all_records()
        
        total_attachments = len(records)
        unique_tickets = len(set(r.get("ticket_id") for r in records))
        
        # Count by type
        ticket_attachments = len([r for r in records if r.get("attachment_type") == "ticket_attachment"])
        conversation_attachments = len([r for r in records if r.get("attachment_type") == "conversation_attachment"])
        untyped_attachments = len([r for r in records if not r.get("attachment_type")])
        
        # Most recent tracking
        latest_record = None
        if records:
            latest_record = max(records, key=lambda x: x.get("recorded_at", ""))
        
        summary = {
            "total_attachments_tracked": total_attachments,
            "unique_tickets_with_attachments": unique_tickets,
            "attachment_breakdown": {
                "ticket_attachments": ticket_attachments,
                "conversation_attachments": conversation_attachments,
                "untyped_attachments": untyped_attachments
            },
            "latest_tracking": latest_record.get("recorded_at") if latest_record else None,
            "summary_generated": datetime.now().isoformat()
        }
        
        return summary


class FileLock:
    """Simple exclusive lock based on fcntl.flock over a dedicated lock file."""

    def __init__(self, lock_path: Path):
        self.lock_path = lock_path
        self.lock_fd = None

    def __enter__(self):
        self.lock_fd = open(self.lock_path, 'w')
        fcntl.flock(self.lock_fd.fileno(), fcntl.LOCK_EX)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.lock_fd:
            fcntl.flock(self.lock_fd.fileno(), fcntl.LOCK_UN)
            self.lock_fd.close()


# Global tracker instance
attachment_tracker = AttachmentURLTracker()

def track_attachment_download(ticket_id: int, freshdesk_url: str, saved_location: str, 
                            attachment_type: str = None, storage_type: str = "local"):
    """
    Convenience function to track an attachment download
    
    Args:
        ticket_id: The ticket ID
        freshdesk_url: Original Freshdesk URL
        saved_location: Where file was saved (local path, S3 URL, or server URL)
        attachment_type: 'tic' or 'conv' (optional)
        storage_type: 'local', 's3', 'server', etc. (optional)
    """
    attachment_tracker.add_attachment_record(
        ticket_id=ticket_id,
        freshdesk_url=freshdesk_url,
        saved_location=saved_location,
        attachment_type=attachment_type,
        storage_type=storage_type
    )

def generate_attachment_summary():
    """Generate and save attachment tracking summary"""
    summary = attachment_tracker.generate_summary()
    
    summary_file = Path("migration/attachment_summary.json")
    with open(summary_file, 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)
    
    logger.info(f"📊 Attachment summary saved to {summary_file}")
    return summary


if __name__ == "__main__":
    # Example usage and testing
    print("📊 Attachment URL Tracker")
    print("=" * 40)
    
    # Check if attachment_url.json exists
    if ATTACHMENT_URL_JSON_PATH.exists():
        records = attachment_tracker.get_all_records()
        print(f"📁 Found {len(records)} existing attachment records")
        
        if records:
            print("\n🔍 Sample records:")
            for i, record in enumerate(records[:3]):  # Show first 3
                print(f"   {i+1}. Ticket {record.get('ticket_id')}: {record.get('attachment_name', 'unknown')}")
                print(f"      Freshdesk URL: {record.get('freshdesk_url', 'unknown')[:50]}...")
                print(f"      Local Path: {record.get('local_path', 'unknown')}")
                print()
        
        # Generate summary
        summary = generate_attachment_summary()
        print("📈 Attachment Summary:")
        print(f"   Total Attachments: {summary['total_attachments_tracked']}")
        print(f"   Unique Tickets: {summary['unique_tickets_with_attachments']}")
        print(f"   Ticket Attachments: {summary['attachment_breakdown']['ticket_attachments']}")
        print(f"   Conversation Attachments: {summary['attachment_breakdown']['conversation_attachments']}")
    else:
        print("📭 No attachment tracking data found")
        print("💡 Attachment tracking will start when consumers download files")