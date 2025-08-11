import requests
import csv
import json
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

def get_ticket_ids():
    """Get all ticket IDs and save to CSV"""
    all_tickets = []
    page = 1
    per_page = 100  # Freshdesk API allows up to 100 per page
    
    print("Fetching all tickets from Freshdesk...")
    
    while True:
        url = f"https://{FRESHDESK_DOMAIN}/api/v2/tickets?page={page}&per_page={per_page}"
        response = requests.get(url, auth=(API_KEY, 'X'), headers=HEADERS)
        
        if response.status_code != 200:
            print(f"Error fetching tickets on page {page}: {response.status_code}")
            break
        
        tickets = response.json()
        
        # If no tickets returned, we've reached the end
        if not tickets:
            print(f"No more tickets found on page {page}")
            break
        
        print(f"Page {page}: Found {len(tickets)} tickets")
        
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
            print(f"Reached end of tickets (got {len(tickets)} on page {page})")
            break
        
        page += 1
    
    print(f"\nTotal tickets found: {len(all_tickets)}")
    return all_tickets

def save_to_csv(ticket_ids, filename='ticket_ids.csv'):
    """Save ticket IDs to CSV file"""
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['id', 'subject', 'status', 'priority', 'created_at']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        writer.writeheader()
        for ticket in ticket_ids:
            writer.writerow(ticket)
    
    print(f"✅ Saved {len(ticket_ids)} ticket IDs to {filename}")

if __name__ == "__main__":
    print("Step 1: Getting all ticket IDs...")
    ticket_ids = get_ticket_ids()
    
    if ticket_ids:
        save_to_csv(ticket_ids)
        print(f"Successfully extracted {len(ticket_ids)} ticket IDs")
    else:
        print("No tickets found or error occurred") 