import requests
import csv
import json
import time
import os
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

def read_ticket_ids_from_csv(filename='ticket_ids.csv'):
    """Read ticket IDs from CSV file"""
    ticket_ids = []
    
    try:
        with open(filename, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                ticket_ids.append(int(row['id']))
        
        print(f"✅ Read {len(ticket_ids)} ticket IDs from {filename}")
        return ticket_ids
    except FileNotFoundError:
        print(f"❌ File {filename} not found. Please run step1_get_ticket_ids.py first.")
        return []
    except Exception as e:
        print(f"❌ Error reading CSV: {e}")
        return []

def get_ticket_details(ticket_id):
    """Get detailed information for a specific ticket"""
    url = f"https://{FRESHDESK_DOMAIN}/api/v2/tickets/{ticket_id}"
    response = requests.get(url, auth=(API_KEY, 'X'), headers=HEADERS)
    
    if response.status_code == 200:
        ticket_data = response.json()
        
        # Enhance attachments with more details if they exist
        if ticket_data.get("attachments"):
            enhanced_attachments = []
            for att in ticket_data["attachments"]:
                enhanced_attachments.append({
                    "id": att.get("id"),
                    "name": att.get("name"),
                    "url": att.get("attachment_url"),
                    "content_type": att.get("content_type"),
                    "size": att.get("size"),
                    "created_at": att.get("created_at")
                })
            ticket_data["attachments"] = enhanced_attachments
        
        return ticket_data
    else:
        print(f"Error fetching ticket details for {ticket_id}: {response.status_code}")
        return None

def fetch_conversations(ticket_id):
    """Fetch conversations for a ticket"""
    url = f"https://{FRESHDESK_DOMAIN}/api/v2/tickets/{ticket_id}/conversations"
    response = requests.get(url, auth=(API_KEY, 'X'), headers=HEADERS)

    if response.status_code != 200:
        print(f"Error fetching conversations for ticket {ticket_id}")
        return []

    conversations = []
    for convo in response.json():
        attachments = [
            {
                "id": att.get("id"),
                "name": att.get("name"),
                "url": att.get("attachment_url"),
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
            "user_id": convo.get("user_id"),  # User who created the conversation
            "created_by": convo.get("created_by"),  # Alternative field name
            "incoming": convo.get("incoming"),  # Boolean indicating if it's from customer
            "outgoing": convo.get("outgoing"),  # Boolean indicating if it's from agent
            "source": convo.get("source"),  # Source of the conversation (email, portal, etc.)
            "thread_id": convo.get("thread_id"),  # Thread ID for grouping conversations
            "attachments": attachments
        })

    return conversations

def enrich_ticket(ticket_id):
    """Enrich ticket with detailed information"""
    print(f"Processing ticket ID: {ticket_id}")
    
    # Get individual ticket details
    ticket_details = get_ticket_details(ticket_id)
    
    if not ticket_details:
        return None
    
    # Add conversations
    ticket_details["conversations"] = fetch_conversations(ticket_id)
    
    print(f"✅ Extracted details for ticket {ticket_id}")
    return ticket_details

def extract_all_ticket_details(ticket_ids):
    """Extract detailed information for all tickets"""
    all_tickets = []
    
    for i, ticket_id in enumerate(ticket_ids, 1):
        print(f"\n--- Processing ticket {i}/{len(ticket_ids)} ---")
        
        ticket_data = enrich_ticket(ticket_id)
        if ticket_data:
            all_tickets.append(ticket_data)
        
        # Add a small delay to respect API rate limits
        if i < len(ticket_ids):
            time.sleep(0.5)
    
    return all_tickets

def save_to_json(data, filename='detailed_tickets_export.json'):
    """Save detailed ticket data to JSON file"""
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"✅ Saved {len(data)} detailed tickets to {filename}")

if __name__ == "__main__":
    print("Step 2: Reading ticket IDs and extracting detailed information...")
    
    # Read ticket IDs from CSV
    ticket_ids = read_ticket_ids_from_csv()
    
    if not ticket_ids:
        exit(1)
    
    # Extract detailed information for each ticket
    detailed_tickets = extract_all_ticket_details(ticket_ids)
    
    if detailed_tickets:
        save_to_json(detailed_tickets)
        print(f"\n🎉 Successfully extracted detailed information for {len(detailed_tickets)} tickets!")
    else:
        print("❌ No detailed ticket information was extracted") 