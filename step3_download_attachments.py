import json
import os
import requests
import csv
from urllib.parse import urlparse
from pathlib import Path
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

HEADERS = {
    "Content-Type": "application/json"
}

def download_file(url, filepath):
    """Download a file from URL to the specified filepath"""
    try:
        # For S3 URLs, don't use Freshdesk authentication
        if 's3.amazonaws.com' in url or 'cdn.freshdesk.com' in url:
            response = requests.get(url, stream=True)
        else:
            # For Freshdesk API URLs, use authentication
            response = requests.get(url, auth=(API_KEY, 'X'), stream=True)
        
        response.raise_for_status()
        
        with open(filepath, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        return True
    except Exception as e:
        print(f"Error downloading {url}: {e}")
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
        print(f"Error parsing JSON file {json_file_path}: {e}")
        return []

def download_attachments(attachments, base_dir="attachments"):
    """Download attachments to folders named by ticket ID"""
    if not attachments:
        print("No attachments found to download")
        return
    
    # Create base directory
    Path(base_dir).mkdir(exist_ok=True)
    
    downloaded_count = 0
    failed_count = 0
    
    print(f"Found {len(attachments)} attachments to download")
    
    for attachment in attachments:
        ticket_id = attachment['ticket_id']
        attachment_url = attachment['attachment_url']
        filename = attachment['name']
        source = attachment['source']
        
        if not attachment_url or not filename:
            print(f"Skipping attachment with missing URL or filename for ticket {ticket_id}")
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
            print(f"File already exists: {filepath}")
            continue
        
        print(f"Downloading: {filename} for ticket {ticket_id}")
        
        if download_file(attachment_url, filepath):
            downloaded_count += 1
            print(f"✅ Downloaded: {filepath}")
        else:
            failed_count += 1
            print(f"❌ Failed to download: {filename}")
    
    print(f"\nDownload Summary:")
    print(f"✅ Successfully downloaded: {downloaded_count}")
    print(f"❌ Failed downloads: {failed_count}")
    print(f"📁 Files saved to: {base_dir}/")

def main():
    """Main function to process JSON files and download attachments"""
    print("=" * 60)
    print("STEP 3: Downloading Attachments from JSON Files")
    print("=" * 60)
    
    # Look for JSON files in current directory
    json_files = []
    for file in os.listdir('.'):
        if file.endswith('.json') and file != 'package.json':
            json_files.append(file)
    
    if not json_files:
        print("No JSON files found in current directory")
        print("Please run step 2 first to generate JSON files with ticket details")
        return
    
    print(f"Found {len(json_files)} JSON file(s):")
    for file in json_files:
        print(f"  - {file}")
    
    all_attachments = []
    
    # Process each JSON file
    for json_file in json_files:
        print(f"\nProcessing: {json_file}")
        attachments = extract_attachments_from_json(json_file)
        all_attachments.extend(attachments)
        print(f"Found {len(attachments)} attachments in {json_file}")
    
    if all_attachments:
        print(f"\nTotal attachments found: {len(all_attachments)}")
        download_attachments(all_attachments)
    else:
        print("No attachments found in any JSON files")

if __name__ == "__main__":
    main() 