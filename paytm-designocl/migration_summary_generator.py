#!/usr/bin/env python3
"""
Migration Summary Generator
===========================
Generates migration_summary.json with detailed statistics about the migration process.
"""

import json
import logging
from pathlib import Path
from datetime import datetime
from typing import List

# Setup logging
logger = logging.getLogger(__name__)

def generate_migration_summary(processed_tickets: List[int], start_time: datetime = None):
    """Generate a summary of the migration process compatible with existing format"""
    if start_time is None:
        start_time = datetime.now()
    
    # Count files created
    complete_ticket_data_dir = Path("complete_ticket_data")
    conversations_dir = Path("conversations")
    ticket_attachments_dir = Path("ticket_attachments")
    conversation_attachments_dir = Path("conversation_attachments")
    ticket_details_dir = Path("ticket_details")
    
    files_created = {
        "ticket_details": len(list(ticket_details_dir.glob("*.json"))) if ticket_details_dir.exists() else 0,
        "ticket_attachments": len(list(ticket_attachments_dir.glob("*.json"))) if ticket_attachments_dir.exists() else 0,
        "conversations": len(list(conversations_dir.glob("*.json"))) if conversations_dir.exists() else 0,
        "conversation_attachments": len(list(conversation_attachments_dir.glob("*.json"))) if conversation_attachments_dir.exists() else 0
    }
    
    # Count statistics from complete ticket data files
    total_conversations = 0
    total_ticket_attachments = 0 
    total_conversation_attachments = 0
    unique_requesters = set()
    unique_conversation_users = set()
    
    if complete_ticket_data_dir.exists():
        for ticket_id in processed_tickets:
            ticket_file = complete_ticket_data_dir / f"ticket_{ticket_id}_complete.json"
            if ticket_file.exists():
                try:
                    with open(ticket_file, 'r', encoding='utf-8') as f:
                        ticket_data = json.load(f)
                    
                    # Count conversations and attachments
                    conversations = ticket_data.get("conversations", [])
                    attachments = ticket_data.get("attachments", [])
                    
                    total_conversations += len(conversations)
                    
                    # Count ticket-level attachments
                    ticket_level_attachments = [att for att in attachments if att.get('type') == 'ticket_attachment']
                    total_ticket_attachments += len(ticket_level_attachments)
                    
                    # Count conversation attachments
                    for conv in conversations:
                        conv_attachments = conv.get("attachments", [])
                        total_conversation_attachments += len(conv_attachments)
                        
                        # Track unique users
                        user_id = conv.get("user_id")
                        if user_id:
                            unique_conversation_users.add(str(user_id))
                    
                    # Track unique requesters
                    requester_id = ticket_data.get("requester_id")
                    if requester_id:
                        unique_requesters.add(str(requester_id))
                        
                except Exception as e:
                    logger.warning(f"Could not read ticket file for summary: {ticket_file}")
    
    # Read migration store for CSV reconciliation data
    csv_reconciliation_records = {
        "ticket_attachments": 0,
        "conversation_attachments": 0,
        "conversations": 0,
        "ticket_details": len(processed_tickets)
    }
    
    # Try to read migration data for more accurate counts
    migration_file = Path("migration/ticket_details.json")
    if migration_file.exists():
        try:
            with open(migration_file, 'r', encoding='utf-8') as f:
                migration_data = json.load(f)
            
            for record in migration_data:
                # Count conversations
                conversations_field = record.get("Conversations")
                if isinstance(conversations_field, list) and len(conversations_field) > 0:
                    csv_reconciliation_records["conversations"] += len(conversations_field)
                elif isinstance(conversations_field, dict):
                    csv_reconciliation_records["conversations"] += len(conversations_field)
                
                # Count conversation attachments
                conv_attachments_field = record.get("Conversation Attachments")
                if isinstance(conv_attachments_field, dict):
                    csv_reconciliation_records["conversation_attachments"] += len(conv_attachments_field)
                elif isinstance(conv_attachments_field, list):
                    csv_reconciliation_records["conversation_attachments"] += len(conv_attachments_field)
                
                # Count ticket attachments
                ticket_attachments_field = record.get("Ticket Attachments")
                if isinstance(ticket_attachments_field, dict):
                    csv_reconciliation_records["ticket_attachments"] += len(ticket_attachments_field)
                elif isinstance(ticket_attachments_field, list):
                    csv_reconciliation_records["ticket_attachments"] += len(ticket_attachments_field)
                    
        except Exception as e:
            logger.warning(f"Could not read migration file for reconciliation data: {e}")
    
    # Create summary in the expected format
    summary = {
        "migration_completed": datetime.now().isoformat(),
        "duration_seconds": (datetime.now() - start_time).total_seconds(),
        "tickets_processed": len(processed_tickets),
        "files_created": files_created,
        "total_files_created": sum(files_created.values()),
        "csv_reconciliation_records": csv_reconciliation_records,
        "data_summary": {
            "tickets": len(processed_tickets),
            "conversations": total_conversations,
            "ticket_attachments": total_ticket_attachments,
            "conversation_attachments": total_conversation_attachments,
            "unique_requesters": len(unique_requesters),
            "unique_conversation_users": len(unique_conversation_users)
        },
        "errors": []
    }
    
    # Save summary
    summary_file = "migration_summary.json"
    with open(summary_file, 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)
    
    logger.info(f"📊 Migration summary saved to {summary_file}")
    
    # Log key statistics
    logger.info(f"📈 Migration Statistics:")
    logger.info(f"   🎫 Total Tickets: {summary['tickets_processed']}")
    logger.info(f"   💬 Total Conversations: {total_conversations}")
    logger.info(f"   📎 Total Ticket Attachments: {total_ticket_attachments}")
    logger.info(f"   📎 Total Conversation Attachments: {total_conversation_attachments}")
    logger.info(f"   👥 Unique Requesters: {len(unique_requesters)}")
    logger.info(f"   👥 Unique Conversation Users: {len(unique_conversation_users)}")
    logger.info(f"   📁 Total Files Created: {summary['total_files_created']}")
    
    return summary

if __name__ == "__main__":
    # Example usage
    from pathlib import Path
    
    # Get processed tickets from complete_ticket_data directory
    complete_dir = Path("complete_ticket_data")
    if complete_dir.exists():
        processed_tickets = []
        for file in complete_dir.glob("ticket_*_complete.json"):
            try:
                ticket_id = int(file.stem.split("_")[1])
                processed_tickets.append(ticket_id)
            except (ValueError, IndexError):
                continue
        
        if processed_tickets:
            generate_migration_summary(processed_tickets)
        else:
            print("No processed tickets found")
    else:
        print("complete_ticket_data directory not found")