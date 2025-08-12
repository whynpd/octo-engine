"""
Cross-process safe store for `migration/ticket_details.json`.

Provides atomic, file-locked operations used by both the details producer
and the attachments consumer so they can run as independent programs safely.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import fcntl  # POSIX file locking (darwin/linux)


MIGRATION_JSON_PATH = Path("migration/ticket_details.json")
LOCK_FILE_PATH = Path("migration/ticket_details.json.lock")


def _ensure_paths() -> None:
    MIGRATION_JSON_PATH.parent.mkdir(parents=True, exist_ok=True)
    # Create lock file if missing
    LOCK_FILE_PATH.touch(exist_ok=True)


class FileLock:
    """Simple exclusive lock based on fcntl.flock over a dedicated lock file."""

    def __init__(self, lock_path: Path) -> None:
        self.lock_path = lock_path
        self._fh = None

    def __enter__(self):
        _ensure_paths()
        self._fh = open(self.lock_path, "r+")
        fcntl.flock(self._fh.fileno(), fcntl.LOCK_EX)
        return self

    def __exit__(self, exc_type, exc, tb):
        try:
            if self._fh is not None:
                fcntl.flock(self._fh.fileno(), fcntl.LOCK_UN)
                self._fh.close()
        finally:
            self._fh = None


def _read_records_unlocked() -> List[Dict[str, Any]]:
    if not MIGRATION_JSON_PATH.exists():
        return []
    try:
        text = MIGRATION_JSON_PATH.read_text(encoding="utf-8")
        if not text.strip():
            return []
        data = json.loads(text)
        if isinstance(data, list):
            return data
    except Exception:
        # On parse error, return empty to avoid crash; caller should be resilient
        return []
    return []


def _write_records_unlocked(records: List[Dict[str, Any]]) -> None:
    tmp = MIGRATION_JSON_PATH.with_suffix(MIGRATION_JSON_PATH.suffix + ".tmp")
    tmp.write_text(json.dumps(records, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(MIGRATION_JSON_PATH)


def merge_entries(new_entries: List[Dict[str, Any]]) -> List[int]:
    """Merge entries by `Ticket_ID`, only adding if not present. Returns list of added Ticket_IDs.

    Fields `Ticket Attachments`, `Conversations`, `Conversation Attachments` should
    already be present in entries (typically all None on creation).
    """
    _ensure_paths()
    added: List[int] = []
    with FileLock(LOCK_FILE_PATH):
        existing = _read_records_unlocked()
        seen = {int(rec.get("Ticket_ID")) for rec in existing if str(rec.get("Ticket_ID", "")).isdigit()}
        for rec in new_entries:
            tid = int(rec["Ticket_ID"])  # raise if malformed
            if tid not in seen:
                existing.append(rec)
                seen.add(tid)
                added.append(tid)
        _write_records_unlocked(existing)
    return added


ATTACHMENTS_FIELD = "Ticket Attachments"
CONVERSATIONS_FIELD = "Conversations"
CONVERSATION_ATTACHMENTS_FIELD = "Conversation Attachments"


def claim_next_null_attachments() -> int:
    """Find a record where attachments == None, set to "I" and return its Ticket_ID.
    Returns -1 if none available.
    """
    _ensure_paths()
    with FileLock(LOCK_FILE_PATH):
        records = _read_records_unlocked()
        for rec in records:
            if rec.get(ATTACHMENTS_FIELD, None) is None:
                rec[ATTACHMENTS_FIELD] = "I"
                _write_records_unlocked(records)
                return int(rec.get("Ticket_ID"))
    return -1


def set_attachments_status(ticket_id: int, value: Any) -> None:
    _ensure_paths()
    with FileLock(LOCK_FILE_PATH):
        records = _read_records_unlocked()
        for rec in records:
            if int(rec.get("Ticket_ID", -1)) == ticket_id:
                rec[ATTACHMENTS_FIELD] = value
                break
        _write_records_unlocked(records)


def list_null_attachment_tickets(limit: Optional[int] = None) -> List[int]:
    _ensure_paths()
    with FileLock(LOCK_FILE_PATH):
        records = _read_records_unlocked()
        tids = [int(rec.get("Ticket_ID")) for rec in records if rec.get(ATTACHMENTS_FIELD) is None]
    return tids[:limit] if limit else tids


def list_in_progress_attachment_tickets() -> List[int]:
    _ensure_paths()
    with FileLock(LOCK_FILE_PATH):
        records = _read_records_unlocked()
        return [int(rec.get("Ticket_ID")) for rec in records if rec.get(ATTACHMENTS_FIELD) == "I"]


def claim_next_null_conversations() -> int:
    """Find a record where conversations == None, set to "I" and return its Ticket_ID.
    Returns -1 if none available.
    """
    _ensure_paths()
    with FileLock(LOCK_FILE_PATH):
        records = _read_records_unlocked()
        for rec in records:
            if rec.get(CONVERSATIONS_FIELD, None) is None:
                rec[CONVERSATIONS_FIELD] = "I"
                _write_records_unlocked(records)
                return int(rec.get("Ticket_ID"))
    return -1


def set_conversations_status(ticket_id: int, value: Any) -> None:
    _ensure_paths()
    with FileLock(LOCK_FILE_PATH):
        records = _read_records_unlocked()
        for rec in records:
            if int(rec.get("Ticket_ID", -1)) == ticket_id:
                rec[CONVERSATIONS_FIELD] = value
                break
        _write_records_unlocked(records)


def list_null_conversation_tickets(limit: Optional[int] = None) -> List[int]:
    _ensure_paths()
    with FileLock(LOCK_FILE_PATH):
        records = _read_records_unlocked()
        tids = [int(rec.get("Ticket_ID")) for rec in records if rec.get(CONVERSATIONS_FIELD) is None]
    return tids[:limit] if limit else tids


def list_in_progress_conversation_tickets() -> List[int]:
    _ensure_paths()
    with FileLock(LOCK_FILE_PATH):
        records = _read_records_unlocked()
        return [int(rec.get("Ticket_ID")) for rec in records if rec.get(CONVERSATIONS_FIELD) == "I"]


def finalize_in_progress(evaluate_status_fn) -> int:
    """Resolve lingering "I" statuses using the provided evaluator.

    The evaluator receives a ticket_id and returns either a mapping or "NA".
    Returns number of records updated.
    """
    _ensure_paths()
    updated = 0
    with FileLock(LOCK_FILE_PATH):
        records = _read_records_unlocked()
        for rec in records:
            if rec.get(ATTACHMENTS_FIELD) == "I":
                tid = int(rec.get("Ticket_ID", -1))
                rec[ATTACHMENTS_FIELD] = evaluate_status_fn(tid)
                updated += 1
        if updated:
            _write_records_unlocked(records)
    return updated


def claim_next_null_conversation_attachments() -> int:
    """Find a record where conversation_attachments == None, set to "I" and return its Ticket_ID.
    Returns -1 if none available.
    """
    _ensure_paths()
    with FileLock(LOCK_FILE_PATH):
        records = _read_records_unlocked()
        for rec in records:
            if rec.get(CONVERSATION_ATTACHMENTS_FIELD, None) is None:
                rec[CONVERSATION_ATTACHMENTS_FIELD] = "I"
                _write_records_unlocked(records)
                return int(rec.get("Ticket_ID"))
    return -1


def set_conversation_attachments_status(ticket_id: int, value: Any) -> None:
    _ensure_paths()
    with FileLock(LOCK_FILE_PATH):
        records = _read_records_unlocked()
        for rec in records:
            if int(rec.get("Ticket_ID", -1)) == ticket_id:
                rec[CONVERSATION_ATTACHMENTS_FIELD] = value
                break
        _write_records_unlocked(records)


def get_conversations_status(ticket_id: int) -> Any:
    """Get the current conversations status for a ticket."""
    _ensure_paths()
    with FileLock(LOCK_FILE_PATH):
        records = _read_records_unlocked()
        for rec in records:
            if int(rec.get("Ticket_ID", -1)) == ticket_id:
                return rec.get(CONVERSATIONS_FIELD)
    return None


def list_null_conversation_attachment_tickets(limit: Optional[int] = None) -> List[int]:
    _ensure_paths()
    with FileLock(LOCK_FILE_PATH):
        records = _read_records_unlocked()
        tids = [int(rec.get("Ticket_ID")) for rec in records if rec.get(CONVERSATION_ATTACHMENTS_FIELD) is None]
    return tids[:limit] if limit else tids


def list_in_progress_conversation_attachment_tickets() -> List[int]:
    _ensure_paths()
    with FileLock(LOCK_FILE_PATH):
        records = _read_records_unlocked()
        return [int(rec.get("Ticket_ID")) for rec in records if rec.get(CONVERSATION_ATTACHMENTS_FIELD) == "I"]


def finalize_in_progress_conversations(evaluate_status_fn) -> int:
    """Resolve lingering "I" conversation statuses using the provided evaluator.

    The evaluator receives a ticket_id and returns either a mapping or "NA".
    Returns number of records updated.
    """
    _ensure_paths()
    updated = 0
    with FileLock(LOCK_FILE_PATH):
        records = _read_records_unlocked()
        for rec in records:
            if rec.get(CONVERSATIONS_FIELD) == "I":
                tid = int(rec.get("Ticket_ID", -1))
                rec[CONVERSATIONS_FIELD] = evaluate_status_fn(tid)
                updated += 1
        if updated:
            _write_records_unlocked(records)
    return updated


def finalize_in_progress_conversation_attachments(evaluate_status_fn) -> int:
    """Resolve lingering "I" conversation attachment statuses using the provided evaluator.

    The evaluator receives a ticket_id and returns either a mapping or "NA".
    Returns number of records updated.
    """
    _ensure_paths()
    updated = 0
    with FileLock(LOCK_FILE_PATH):
        records = _read_records_unlocked()
        for rec in records:
            if rec.get(CONVERSATION_ATTACHMENTS_FIELD) == "I":
                tid = int(rec.get("Ticket_ID", -1))
                rec[CONVERSATION_ATTACHMENTS_FIELD] = evaluate_status_fn(tid)
                updated += 1
        if updated:
            _write_records_unlocked(records)
    return updated

