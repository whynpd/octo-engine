# step3_download_attachments.py - Enhanced for 40K tickets
import json
import os
import requests
import csv
import time
import logging
import gzip
from urllib.parse import urlparse
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
    'delay_between_requests': 0.1,  # 1000 requests/hour = 3.6 seconds between requests
    'max_retries': 3,
    'max_workers': 5,  # For parallel attachment downloads
    'batch_size': 50,  # Process attachments in batches
    'rate_limit_requests_per_hour': 10000
}

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('step3_migration.log'),
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

def save_progress(downloaded_attachments, filename="step3_progress.json"):
    """Save download progress to resume later"""
    progress_data = {
        'downloaded_attachments': downloaded_attachments,
        'timestamp': datetime.now().isoformat(),
        'total_downloaded': len(downloaded_attachments)
    }
    
    with open(filename, 'w') as f:
        json.dump(progress_data, f, indent=2)
    
    logger.info(f"Progress saved: {len(downloaded_attachments)} attachments downloaded")

def load_progress(filename="step3_progress.json"):
    """Load download progress to resume"""
    if os.path.exists(filename):
        with open(filename, 'r') as f:
            return json.load(f)
    return None

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

def extract_attachments_from_json(json_file_path):
    """Extract attachment information from JSON file"""
    try:
        with open(json_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        attachments = []
        
        # Handle different JSON structures
        if isinstance(data, list):
            # If it's a list of tickets
            for ticket in data:
                ticket_id = ticket.get('id')
                if ticket_id:
                    # Main ticket attachments
                    if 'attachments' in ticket:
                        for attachment in ticket['attachments']:
                            attachments.append({
                                'ticket_id': ticket_id,
                                'attachment_url': attachment.get('attachment_url') or attachment.get('url'),
                                'name': attachment.get('name'),
                                'content_type': attachment.get('content_type'),
                                'size': attachment.get('size'),
                                'source': 'ticket'
                            })
                    
                    # Conversation attachments
                    if 'conversations' in ticket:
                        for conv in ticket['conversations']:
                            if 'attachments' in conv:
                                for attachment in conv['attachments']:
                                    attachments.append({
                                        'ticket_id': ticket_id,
                                        'attachment_url': attachment.get('attachment_url') or attachment.get('url'),
                                        'name': attachment.get('name'),
                                        'content_type': attachment.get('content_type'),
                                        'size': attachment.get('size'),
                                        'source': 'conversation'
                                    })
        elif isinstance(data, dict):
            # If it's a single ticket
            ticket_id = data.get('id')
            if ticket_id:
                # Main ticket attachments
                if 'attachments' in data:
                    for attachment in data['attachments']:
                        attachments.append({
                            'ticket_id': ticket_id,
                            'attachment_url': attachment.get('attachment_url') or attachment.get('url'),
                            'name': attachment.get('name'),
                            'content_type': attachment.get('content_type'),
                            'size': attachment.get('size'),
                            'source': 'ticket'
                        })
                
                # Conversation attachments
                if 'conversations' in data:
                    for conv in data['conversations']:
                        if 'attachments' in conv:
                            for attachment in conv['attachments']:
                                attachments.append({
                                    'ticket_id': ticket_id,
                                    'attachment_url': attachment.get('attachment_url') or attachment.get('url'),
                                    'name': attachment.get('name'),
                                    'content_type': attachment.get('content_type'),
                                    'size': attachment.get('size'),
                                    'source': 'conversation'
                                })
        
        return attachments
    
    except Exception as e:
        logger.error(f"Error parsing JSON file {json_file_path}: {e}")
        return []

def download_attachments_batch(attachments, base_dir="attachments", downloaded_attachments=None):
    """Download a batch of attachments with rate limiting"""
    if not attachments:
        logger.info("No attachments found to download")
        return []
    
    if downloaded_attachments is None:
        downloaded_attachments = []
    
    # Create base directory
    Path(base_dir).mkdir(exist_ok=True)
    
    downloaded_count = 0
    failed_count = 0
    
    logger.info(f"Processing {len(attachments)} attachments...")
    
    for i, attachment in enumerate(attachments, 1):
        ticket_id = attachment['ticket_id']
        attachment_url = attachment['attachment_url']
        filename = attachment['name']
        source = attachment['source']
        
        if not attachment_url or not filename:
            logger.warning(f"Skipping attachment with missing URL or filename for ticket {ticket_id}")
            continue
        
        # Create ticket directory
        ticket_dir = Path(base_dir) / str(ticket_id)
        ticket_dir.mkdir(exist_ok=True)
        
        # Sanitize filename
        safe_filename = sanitize_filename(filename)
        
        # Add source prefix to avoid conflicts
        if source == 'conversation':
            safe_filename = f"conv_{safe_filename}"
        
        filepath = ticket_dir / safe_filename
        
        # Check if file already exists
        if filepath.exists():
            logger.info(f"File already exists: {filepath}")
            downloaded_attachments.append(f"{ticket_id}_{filename}")
            continue
        
        logger.info(f"Downloading ({i}/{len(attachments)}): {filename} for ticket {ticket_id}")
        
        if download_file(attachment_url, filepath):
            downloaded_count += 1
            downloaded_attachments.append(f"{ticket_id}_{filename}")
            logger.info(f"✅ Downloaded: {filepath}")
        else:
            failed_count += 1
            logger.error(f"❌ Failed to download: {filename}")
        
        # Rate limiting for downloads
        if i < len(attachments):
            logger.info(f"Waiting {CONFIG['delay_between_requests']} seconds...")
            time.sleep(CONFIG['delay_between_requests'])
    
    logger.info(f"Batch completed: {downloaded_count} downloaded, {failed_count} failed")
    return downloaded_attachments

def main():
    """Main function to process JSON files and download attachments with rate limiting"""
    logger.info("=" * 60)
    logger.info("STEP 3: Downloading Attachments from JSON Files")
    logger.info("=" * 60)
    logger.info(f"Configuration: {CONFIG}")
    
    # Check for existing progress
    progress = load_progress()
    if progress:
        logger.info(f"Found existing progress: {progress['total_downloaded']} attachments downloaded")
        resume_choice = input("Do you want to resume from where you left off? (y/n): ").lower()
        if resume_choice == 'y':
            downloaded_attachments = progress['downloaded_attachments']
        else:
            downloaded_attachments = []
    else:
        downloaded_attachments = []
    
    # Look for JSON files in current directory and batches directory
    json_files = []
    
    # Check current directory
    for file in os.listdir('.'):
        if file.endswith('.json') and file != 'package.json':
            json_files.append(file)
    
    # Check batches directory
    if os.path.exists('batches'):
        for file in os.listdir('batches'):
            if file.endswith('.json.gz'):
                json_files.append(f"batches/{file}")
    
    if not json_files:
        logger.error("No JSON files found in current directory or batches/")
        logger.error("Please run step 2 first to generate JSON files with ticket details")
        return
    
    logger.info(f"Found {len(json_files)} JSON file(s):")
    for file in json_files:
        logger.info(f"  - {file}")
    
    all_attachments = []
    
    # Process each JSON file
    for json_file in json_files:
        logger.info(f"\nProcessing: {json_file}")
        
        if json_file.endswith('.gz'):
            # Handle compressed files
            with gzip.open(json_file, 'rt', encoding='utf-8') as f:
                data = json.load(f)
                # Save temporarily for processing
                temp_file = f"temp_{os.path.basename(json_file)}"
                with open(temp_file, 'w') as tf:
                    json.dump(data, tf)
                attachments = extract_attachments_from_json(temp_file)
                os.remove(temp_file)
        else:
            attachments = extract_attachments_from_json(json_file)
        
        all_attachments.extend(attachments)
        logger.info(f"Found {len(attachments)} attachments in {json_file}")
    
    if all_attachments:
        logger.info(f"\nTotal attachments found: {len(all_attachments)}")
        
        # Process attachments in batches
        batch_size = CONFIG['batch_size']
        total_batches = (len(all_attachments) + batch_size - 1) // batch_size
        
        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size
            end_idx = min(start_idx + batch_size, len(all_attachments))
            batch_attachments = all_attachments[start_idx:end_idx]
            
            logger.info(f"\n{'='*50}")
            logger.info(f"ATTACHMENT BATCH {batch_num + 1}/{total_batches}")
            logger.info(f"Processing attachments {start_idx + 1}-{end_idx} of {len(all_attachments)}")
            logger.info(f"{'='*50}")
            
            downloaded_attachments = download_attachments_batch(
                batch_attachments, 
                downloaded_attachments=downloaded_attachments
            )
            
            # Save progress after each batch
            save_progress(downloaded_attachments)
        
        logger.info(f"\n📊 Final Download Summary:")
        logger.info(f"   - Total attachments found: {len(all_attachments)}")
        logger.info(f"   - Successfully downloaded: {len(downloaded_attachments)}")
        logger.info(f"   - Files saved to: attachments/")
        
        # Clean up progress file on successful completion
        if os.path.exists("step3_progress.json"):
            os.remove("step3_progress.json")
            logger.info("Cleaned up progress file")
        
    else:
        logger.info("No attachments found in any JSON files")

if __name__ == "__main__":
    main() 