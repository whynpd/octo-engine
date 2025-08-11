# step2_extract_ticket_details.py - Enhanced for 40K tickets
import requests
import csv
import json
import time
import os
import logging
import gzip
from pathlib import Path
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Get credentials from environment variables
FRESHDESK_DOMAIN = os.getenv("FRESHDESK_DOMAIN")
API_KEY = os.getenv("FRESHDESK_API_KEY")

# Validate that required environment variables are set
if not FRESHDESK_DOMAIN:
    raise ValueError("FRESHDESK_DOMAIN environment variable is not set")
if not API_KEY:
    raise ValueError("FRESHDESK_API_KEY environment variable is not set")

# Configuration for large-scale migration (40K tickets)
CONFIG = {
    'batch_size': 100,  # Process 100 tickets per batch
    'delay_between_requests': 0.1,  # 1000 requests/hour = 3.6 seconds between requests
    'delay_between_batches': 10,  # Additional delay between batches
    'max_retries': 3,
    'save_interval': 10,  # Save progress every 10 batches
    'rate_limit_requests_per_hour': 10000
}

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('step2_migration.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Setup session with retry strategy
def setup_session_with_retry():
    """Create a requests session with retry strategy for rate limiting"""
    session = requests.Session()
    retry_strategy = Retry(
        total=CONFIG['max_retries'],
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "POST", "PUT", "DELETE", "OPTIONS", "TRACE"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

# Global session
session = setup_session_with_retry()

HEADERS = {
    "Content-Type": "application/json"
}

def rate_limited_request(url, auth=None, **kwargs):
    """Make a rate-limited request with proper delays"""
    try:
        response = session.get(url, auth=auth, headers=HEADERS, **kwargs)
        
        # Handle rate limiting
        if response.status_code == 429:
            retry_after = int(response.headers.get('Retry-After', 60))
            logger.warning(f"Rate limited. Waiting {retry_after} seconds...")
            time.sleep(retry_after)
            return rate_limited_request(url, auth=auth, **kwargs)
        
        return response
    except Exception as e:
        logger.error(f"Request failed: {e}")
        raise

def save_progress(processed_tickets, batch_number, filename="step2_progress.json"):
    """Save migration progress to resume later"""
    progress_data = {
        'processed_tickets': processed_tickets,
        'batch_number': batch_number,
        'timestamp': datetime.now().isoformat(),
        'total_processed': len(processed_tickets)
    }
    
    with open(filename, 'w') as f:
        json.dump(progress_data, f, indent=2)
    
    logger.info(f"Progress saved: {len(processed_tickets)} tickets processed")

def load_progress(filename="step2_progress.json"):
    """Load migration progress to resume"""
    if os.path.exists(filename):
        with open(filename, 'r') as f:
            return json.load(f)
    return None

def save_batch_json(tickets, batch_number, base_dir="batches"):
    """Save a batch of tickets to compressed JSON"""
    Path(base_dir).mkdir(exist_ok=True)
    
    filename = f"{base_dir}/batch_{batch_number:03d}.json.gz"
    with gzip.open(filename, 'wt', encoding='utf-8') as f:
        json.dump(tickets, f, indent=2, ensure_ascii=False)
    
    logger.info(f"Batch {batch_number} saved: {len(tickets)} tickets to {filename}")

def read_ticket_ids_from_csv(filename='ticket_ids.csv'):
    """Read ticket IDs from CSV file"""
    ticket_ids = []
    
    try:
        with open(filename, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                ticket_ids.append(int(row['id']))
        
        logger.info(f"✅ Read {len(ticket_ids)} ticket IDs from {filename}")
        return ticket_ids
    except FileNotFoundError:
        logger.error(f"❌ File {filename} not found. Please run step1_get_ticket_ids.py first.")
        return []
    except Exception as e:
        logger.error(f"❌ Error reading CSV: {e}")
        return []

def get_ticket_details(ticket_id):
    """Get detailed information for a specific ticket with rate limiting"""
    url = f"https://{FRESHDESK_DOMAIN}/api/v2/tickets/{ticket_id}"
    
    try:
        response = rate_limited_request(url, auth=(API_KEY, 'X'))
        
        if response.status_code == 200:
            ticket_data = response.json()
            
            # Enhance attachments with more details if they exist
            if ticket_data.get("attachments"):
                enhanced_attachments = []
                for att in ticket_data["attachments"]:
                    enhanced_attachments.append({
                        "id": att.get("id"),
                        "name": att.get("name"),
                        "url": att.get("attachment_url") or att.get("url"),
                        "content_type": att.get("content_type"),
                        "size": att.get("size"),
                        "created_at": att.get("created_at")
                    })
                ticket_data["attachments"] = enhanced_attachments
            
            return ticket_data
        else:
            logger.error(f"Error fetching ticket details for {ticket_id}: {response.status_code}")
            return None
            
    except Exception as e:
        logger.error(f"Error fetching ticket {ticket_id}: {e}")
        return None

def fetch_conversations(ticket_id):
    """Fetch conversations for a ticket with rate limiting"""
    url = f"https://{FRESHDESK_DOMAIN}/api/v2/tickets/{ticket_id}/conversations"
    
    try:
        response = rate_limited_request(url, auth=(API_KEY, 'X'))

        if response.status_code != 200:
            logger.error(f"Error fetching conversations for ticket {ticket_id}")
            return []

        conversations = []
        for convo in response.json():
            attachments = [
                {
                    "id": att.get("id"),
                    "name": att.get("name"),
                    "url": att.get("attachment_url") or att.get("url"),
                    "content_type": att.get("content_type"),
                    "size": att.get("size"),
                    "created_at": att.get("created_at")
                }
                for att in convo.get("attachments", [])
            ]
            conversations.append({
                "id": convo.get("id"),
                "body_text": convo.get("body_text"),
                "body": convo.get("body"),
                "private": convo.get("private"),
                "created_at": convo.get("created_at"),
                "attachments": attachments
            })

        return conversations
        
    except Exception as e:
        logger.error(f"Error fetching conversations for ticket {ticket_id}: {e}")
        return []

def process_ticket_batch(ticket_ids, batch_number):
    """Process a batch of tickets with rate limiting"""
    logger.info(f"\n--- Processing batch {batch_number} ({len(ticket_ids)} tickets) ---")
    
    batch_tickets = []
    
    for i, ticket_id in enumerate(ticket_ids, 1):
        logger.info(f"Processing ticket {i}/{len(ticket_ids)} (ID: {ticket_id})")
        
        # Get individual ticket details
        ticket_details = get_ticket_details(ticket_id)
        
        if ticket_details:
            # Add conversations
            ticket_details["conversations"] = fetch_conversations(ticket_id)
            batch_tickets.append(ticket_details)
            logger.info(f"✅ Extracted details for ticket {ticket_id}")
        else:
            logger.error(f"❌ Failed to extract details for ticket {ticket_id}")
        
        # Rate limiting between tickets
        if i < len(ticket_ids):
            logger.info(f"Waiting {CONFIG['delay_between_requests']} seconds...")
            time.sleep(CONFIG['delay_between_requests'])
    
    return batch_tickets

def main():
    """Main function to run step 2 with rate limiting and batch processing"""
    logger.info("🚀 Starting Step 2: Extract detailed ticket information")
    logger.info(f"Configuration: {CONFIG}")
    
    # Check for existing progress
    progress = load_progress()
    if progress and CONFIG.get('resume_enabled', True):
        logger.info(f"Found existing progress: {progress['total_processed']} tickets processed")
        resume_choice = input("Do you want to resume from where you left off? (y/n): ").lower()
        if resume_choice == 'y':
            processed_tickets = progress['processed_tickets']
            start_batch = progress['batch_number']
        else:
            processed_tickets = []
            start_batch = 1
    else:
        processed_tickets = []
        start_batch = 1
    
    # Read ticket IDs from CSV
    ticket_ids = read_ticket_ids_from_csv()
    
    if not ticket_ids:
        logger.error("❌ No ticket IDs found. Please run step1_get_ticket_ids.py first.")
        return
    
    # Skip already processed tickets
    if processed_tickets:
        ticket_ids = [tid for tid in ticket_ids if tid not in processed_tickets]
        logger.info(f"Skipping {len(processed_tickets)} already processed tickets")
    
    total_tickets = len(ticket_ids)
    total_batches = (total_tickets + CONFIG['batch_size'] - 1) // CONFIG['batch_size']
    
    logger.info(f"Processing {total_tickets} tickets in {total_batches} batches")
    
    # Process tickets in batches
    for batch_num in range(start_batch, total_batches + 1):
        start_idx = (batch_num - 1) * CONFIG['batch_size']
        end_idx = min(start_idx + CONFIG['batch_size'], total_tickets)
        batch_ticket_ids = ticket_ids[start_idx:end_idx]
        
        logger.info(f"\n{'='*50}")
        logger.info(f"BATCH {batch_num}/{total_batches}")
        logger.info(f"Processing tickets {start_idx+1}-{end_idx} of {total_tickets}")
        logger.info(f"{'='*50}")
        
        # Process batch
        batch_tickets = process_ticket_batch(batch_ticket_ids, batch_num)
        
        if batch_tickets:
            # Save batch immediately
            save_batch_json(batch_tickets, batch_num)
            
            # Update progress
            processed_tickets.extend(batch_ticket_ids)
            save_progress(processed_tickets, batch_num)
            
            logger.info(f"✅ Batch {batch_num} completed: {len(batch_tickets)} tickets")
        else:
            logger.error(f"❌ Batch {batch_num} failed")
        
        # Rate limiting between batches
        if batch_num < total_batches:
            logger.info(f"Waiting {CONFIG['delay_between_batches']} seconds before next batch...")
            time.sleep(CONFIG['delay_between_batches'])
    
    logger.info(f"\n🎉 Step 2 completed! Processed {len(processed_tickets)} tickets")
    logger.info("📁 Batch files saved in 'batches/' directory")
    
    # Clean up progress file on successful completion
    if os.path.exists("step2_progress.json"):
        os.remove("step2_progress.json")
        logger.info("Cleaned up progress file")

if __name__ == "__main__":
    main() 