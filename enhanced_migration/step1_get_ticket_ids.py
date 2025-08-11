# step1_get_ticket_ids.py - Enhanced for 40K tickets
import requests
import csv
import json
import os
import time
import logging
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
    'delay_between_requests': 0.1,  # 1000 requests/hour = 3.6 seconds between requests
    'max_retries': 3,
    'rate_limit_requests_per_hour': 10000
}

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('step1_migration.log'),
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

def save_progress(processed_pages, total_tickets, filename="step1_progress.json"):
    """Save migration progress to resume later"""
    progress_data = {
        'processed_pages': processed_pages,
        'total_tickets': total_tickets,
        'timestamp': datetime.now().isoformat(),
        'last_page': processed_pages[-1] if processed_pages else 0
    }
    
    with open(filename, 'w') as f:
        json.dump(progress_data, f, indent=2)
    
    logger.info(f"Progress saved: {len(processed_pages)} pages processed, {len(total_tickets)} tickets found")

def load_progress(filename="step1_progress.json"):
    """Load migration progress to resume"""
    if os.path.exists(filename):
        with open(filename, 'r') as f:
            return json.load(f)
    return None

def get_ticket_ids():
    """Get all ticket IDs with rate limiting and progress tracking"""
    all_tickets = []
    page = 1
    per_page = 100  # Freshdesk API allows up to 100 per page
    processed_pages = []
    
    # Check for existing progress
    progress = load_progress()
    if progress:
        logger.info(f"Found existing progress: {progress['last_page']} pages processed")
        resume_choice = input("Do you want to resume from where you left off? (y/n): ").lower()
        if resume_choice == 'y':
            page = progress['last_page'] + 1
            all_tickets = progress.get('total_tickets', [])
            processed_pages = progress.get('processed_pages', [])
            logger.info(f"Resuming from page {page}")
    
    logger.info("Fetching all tickets from Freshdesk...")
    logger.info(f"Rate limiting: {CONFIG['delay_between_requests']} seconds between requests")
    
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
            
            # Update progress
            processed_pages.append(page)
            save_progress(processed_pages, all_tickets, page)
            
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
    return all_tickets

def save_to_csv(ticket_ids, filename='ticket_ids.csv'):
    """Save ticket IDs to CSV file"""
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['id', 'subject', 'status', 'priority', 'created_at']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        writer.writeheader()
        for ticket in ticket_ids:
            writer.writerow(ticket)
    
    logger.info(f"✅ Saved {len(ticket_ids)} ticket IDs to {filename}")

if __name__ == "__main__":
    logger.info("Step 1: Getting all ticket IDs with rate limiting...")
    logger.info(f"Configuration: {CONFIG}")
    
    ticket_ids = get_ticket_ids()
    
    if ticket_ids:
        save_to_csv(ticket_ids)
        logger.info(f"Successfully extracted {len(ticket_ids)} ticket IDs")
        
        # Clean up progress file on successful completion
        if os.path.exists("step1_progress.json"):
            os.remove("step1_progress.json")
            logger.info("Cleaned up progress file")
    else:
        logger.error("No tickets found or error occurred") 