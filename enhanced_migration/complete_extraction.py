# complete_extraction.py - Enhanced for 40K tickets
import requests
import json
import csv
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
    'delay_between_requests': 0.01,  # 1000 requests/hour = 3.6 seconds between requests
    'delay_between_batches': 1,  # Additional delay between batches
    'max_retries': 3,
    'save_interval': 10,  # Save progress every 10 batches
    'max_workers': 5,  # For parallel attachment downloads
    'resume_enabled': True,
    'rate_limit_requests_per_hour': 10000
}

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('complete_migration.log'),
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

def save_progress(processed_tickets, batch_number, filename="complete_progress.json"):
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

def load_progress(filename="complete_progress.json"):
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

def consolidate_batches(base_dir="batches", output_file="complete_tickets_export.json.gz"):
    """Consolidate all batch files into single compressed JSON"""
    all_tickets = []
    batch_files = sorted(Path(base_dir).glob("batch_*.json.gz"))
    
    logger.info(f"Consolidating {len(batch_files)} batch files...")
    
    for batch_file in batch_files:
        with gzip.open(batch_file, 'rt', encoding='utf-8') as f:
            batch_data = json.load(f)
            all_tickets.extend(batch_data)
            logger.info(f"Loaded {len(batch_data)} tickets from {batch_file.name}")
    
    # Save consolidated file
    with gzip.open(output_file, 'wt', encoding='utf-8') as f:
        json.dump(all_tickets, f, indent=2, ensure_ascii=False)
    
    logger.info(f"Consolidation complete: {len(all_tickets)} tickets saved to {output_file}")
    return all_tickets

def download_file(url, filepath):
    """Download a file from URL to the specified filepath with rate limiting"""
    try:
        # For S3 URLs, don't use Freshdesk authentication
        if 's3.amazonaws.com' in url or 'cdn.freshdesk.com' in url:
            response = rate_limited_request(url, stream=True)
        else:
            # For Freshdesk API URLs, use authentication
            response = rate_limited_request(url, auth=(API_KEY, 'X'), stream=True)
        
        response.raise_for_status()
        
        with open(filepath, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        return True
    except Exception as e:
        logger.error(f"Error downloading {url}: {e}")
        return False

def sanitize_filename(filename):
    """Sanitize filename to be safe for filesystem"""
    # Remove or replace problematic characters
    invalid_chars = '<>:"/\\|?*'
    for char in invalid_chars:
        filename = filename.replace(char, '_')
    
    # Limit length
    if len(filename) > 200:
        name, ext = os.path.splitext(filename)
        filename = name[:200-len(ext)] + ext
    
    return filename

def download_attachments_from_tickets(tickets, base_dir="attachments"):
    """Download attachments from ticket data with rate limiting"""
    if not tickets:
        logger.info("No tickets provided for attachment download")
        return
    
    # Create base directory
    Path(base_dir).mkdir(exist_ok=True)
    
    downloaded_count = 0
    failed_count = 0
    total_attachments = 0
    
    logger.info(f"📁 Downloading attachments from {len(tickets)} tickets...")
    
    for ticket in tickets:
        ticket_id = ticket.get('id')
        if not ticket_id:
            continue
        
        # Process ticket attachments
        if 'attachments' in ticket:
            for attachment in ticket['attachments']:
                total_attachments += 1
                attachment_url = attachment.get('attachment_url') or attachment.get('url')
                filename = attachment.get('name')
                
                if not attachment_url or not filename:
                    logger.warning(f"Skipping attachment with missing URL or filename for ticket {ticket_id}")
                    continue
                
                # Create ticket directory
                ticket_dir = Path(base_dir) / str(ticket_id)
                ticket_dir.mkdir(exist_ok=True)
                
                # Sanitize filename
                safe_filename = sanitize_filename(filename)
                filepath = ticket_dir / safe_filename
                
                # Check if file already exists
                if filepath.exists():
                    logger.info(f"File already exists: {filepath}")
                    continue
                
                logger.info(f"Downloading: {filename} for ticket {ticket_id}")
                
                if download_file(attachment_url, filepath):
                    downloaded_count += 1
                    logger.info(f"✅ Downloaded: {filepath}")
                else:
                    failed_count += 1
                    logger.error(f"❌ Failed to download: {filename}")
                
                # Rate limiting for downloads
                time.sleep(CONFIG['delay_between_requests'])
        
        # Process conversation attachments
        if 'conversations' in ticket:
            for conv in ticket['conversations']:
                if 'attachments' in conv:
                    for attachment in conv['attachments']:
                        total_attachments += 1
                        attachment_url = attachment.get('attachment_url') or attachment.get('url')
                        filename = attachment.get('name')
                        
                        if not attachment_url or not filename:
                            continue
                        
                        # Create ticket directory
                        ticket_dir = Path(base_dir) / str(ticket_id)
                        ticket_dir.mkdir(exist_ok=True)
                        
                        # Sanitize filename and add conversation prefix
                        safe_filename = sanitize_filename(filename)
                        safe_filename = f"conv_{safe_filename}"
                        filepath = ticket_dir / safe_filename
                        
                        # Check if file already exists
                        if filepath.exists():
                            logger.info(f"File already exists: {filepath}")
                            continue
                        
                        logger.info(f"Downloading conversation attachment: {filename} for ticket {ticket_id}")
                        
                        if download_file(attachment_url, filepath):
                            downloaded_count += 1
                            logger.info(f"✅ Downloaded: {filepath}")
                        else:
                            failed_count += 1
                            logger.error(f"❌ Failed to download: {filename}")
                        
                        # Rate limiting for downloads
                        time.sleep(CONFIG['delay_between_requests'])
    
    logger.info(f"\n📊 Attachment Download Summary:")
    logger.info(f"   - Total attachments found: {total_attachments}")
    logger.info(f"   - Successfully downloaded: {downloaded_count}")
    logger.info(f"   - Failed downloads: {failed_count}")
    logger.info(f"   - Files saved to: {base_dir}/")

def step1_get_ticket_ids():
    """Step 1: Get all ticket IDs with rate limiting"""
    logger.info("=" * 50)
    logger.info("STEP 1: Getting all ticket IDs...")
    logger.info("=" * 50)
    
    all_tickets = []
    page = 1
    per_page = 100  # Freshdesk API allows up to 100 per page
    
    logger.info("Fetching all tickets from Freshdesk...")
    
    while True:
        url = f"https://{FRESHDESK_DOMAIN}/api/v2/tickets?page={page}&per_page={per_page}"
        
        try:
            logger.info(f"Fetching page {page}...")
            response = rate_limited_request(url, auth=(API_KEY, 'X'))
            
            if response.status_code != 200:
                logger.error(f"Error fetching tickets on page {page}: {response.status_code}")
                break
            
            tickets = response.json()
            
            # If no tickets returned, we've reached the end
            if not tickets:
                logger.info(f"No more tickets found on page {page}")
                break
            
            logger.info(f"Page {page}: Found {len(tickets)} tickets")
            
            # Extract ticket IDs and basic info
            for ticket in tickets:
                ticket_info = {
                    'id': ticket.get('id'),
                    'subject': ticket.get('subject'),
                    'status': ticket.get('status'),
                    'priority': ticket.get('priority'),
                    'created_at': ticket.get('created_at')
                }
                all_tickets.append(ticket_info)
                print(f"  Ticket ID: {ticket_info['id']} - {ticket_info['subject']}")
            
            # If we got fewer tickets than per_page, we've reached the end
            if len(tickets) < per_page:
                logger.info(f"Reached end of tickets (got {len(tickets)} on page {page})")
                break
            
            page += 1
            
            # Rate limiting between pages
            logger.info(f"Waiting {CONFIG['delay_between_requests']} seconds before next request...")
            time.sleep(CONFIG['delay_between_requests'])
            
        except Exception as e:
            logger.error(f"Error processing page {page}: {e}")
            break
    
    logger.info(f"\nTotal tickets found: {len(all_tickets)}")
    
    # Save to CSV
    with open('ticket_ids.csv', 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['id', 'subject', 'status', 'priority', 'created_at']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        writer.writeheader()
        for ticket in all_tickets:
            writer.writerow(ticket)
    
    logger.info(f"✅ Saved {len(all_tickets)} ticket IDs to ticket_ids.csv")
    return [ticket['id'] for ticket in all_tickets]

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
                "user_id": convo.get("user_id"),
                "created_by": convo.get("created_by"),
                "source": convo.get("source"),
                "thread_id": convo.get("thread_id"),
                "attachments": attachments
            })

        return conversations
        
    except Exception as e:
        logger.error(f"Error fetching conversations for ticket {ticket_id}: {e}")
        return []

def step2_extract_detailed_info_batch(ticket_ids, batch_number):
    """Step 2: Extract detailed information for a batch of tickets"""
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
            time.sleep(CONFIG['delay_between_requests'])
    
    return batch_tickets

def main():
    """Main function to run the complete extraction process with rate limiting"""
    logger.info("🚀 Starting Complete Freshdesk Ticket Extraction Process")
    logger.info("This will extract all tickets with full details including attachments and conversations")
    logger.info(f"Configuration: {CONFIG}")
    
    # Check for existing progress
    progress = load_progress()
    if progress and CONFIG['resume_enabled']:
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
    
    # Step 1: Get ticket IDs
    ticket_ids = step1_get_ticket_ids()
    
    if not ticket_ids:
        logger.error("❌ No ticket IDs found. Exiting.")
        return
    
    # Skip already processed tickets
    if processed_tickets:
        ticket_ids = [tid for tid in ticket_ids if tid not in processed_tickets]
        logger.info(f"Skipping {len(processed_tickets)} already processed tickets")
    
    total_tickets = len(ticket_ids)
    total_batches = (total_tickets + CONFIG['batch_size'] - 1) // CONFIG['batch_size']
    
    logger.info(f"Processing {total_tickets} tickets in {total_batches} batches")
    
    # Step 2: Extract detailed information in batches
    for batch_num in range(start_batch, total_batches + 1):
        start_idx = (batch_num - 1) * CONFIG['batch_size']
        end_idx = min(start_idx + CONFIG['batch_size'], total_tickets)
        batch_ticket_ids = ticket_ids[start_idx:end_idx]
        
        logger.info(f"\n{'='*50}")
        logger.info(f"BATCH {batch_num}/{total_batches}")
        logger.info(f"Processing tickets {start_idx+1}-{end_idx} of {total_tickets}")
        logger.info(f"{'='*50}")
        
        # Extract batch
        batch_tickets = step2_extract_detailed_info_batch(batch_ticket_ids, batch_num)
        
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
    
    # Step 3: Consolidate all batches
    logger.info(f"\n{'='*50}")
    logger.info("CONSOLIDATING BATCHES")
    logger.info(f"{'='*50}")
    
    all_tickets = consolidate_batches()
    
    if all_tickets:
        logger.info(f"\n🎉 SUCCESS! Extracted detailed information for {len(all_tickets)} tickets!")
        logger.info("📁 Files created:")
        logger.info("   - ticket_ids.csv (ticket IDs and basic info)")
        logger.info("   - batches/ (individual batch files)")
        logger.info("   - complete_tickets_export.json.gz (consolidated data)")
        
        # Show summary
        total_conversations = sum(len(ticket.get('conversations', [])) for ticket in all_tickets)
        total_attachments = sum(len(ticket.get('attachments', [])) for ticket in all_tickets)
        total_conv_attachments = sum(
            len(conv.get('attachments', [])) 
            for ticket in all_tickets 
            for conv in ticket.get('conversations', [])
        )
        
        logger.info(f"\n📊 Summary:")
        logger.info(f"   - Tickets: {len(all_tickets)}")
        logger.info(f"   - Conversations: {total_conversations}")
        logger.info(f"   - Ticket Attachments: {total_attachments}")
        logger.info(f"   - Conversation Attachments: {total_conv_attachments}")
        
        # Step 4: Download attachments
        if total_attachments > 0 or total_conv_attachments > 0:
            logger.info(f"\n{'='*50}")
            logger.info("STEP 4: Downloading attachments...")
            logger.info(f"{'='*50}")
            download_attachments_from_tickets(all_tickets)
        else:
            logger.info(f"\n📁 No attachments found to download")
        
        # Clean up progress file
        if os.path.exists("complete_progress.json"):
            os.remove("complete_progress.json")
            logger.info("Cleaned up progress file")
        
    else:
        logger.error("❌ No detailed ticket information was extracted")

if __name__ == "__main__":
    main() 