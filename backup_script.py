import requests
import json
import time

FRESHDESK_DOMAIN = "ocltp.freshdesk.com"
API_KEY = "OkRAFkaX5B91LI7bjHmP"  

HEADERS = {
    "Content-Type": "application/json"
}

def fetch_tickets():
    all_tickets = []
    page = 1

    while True:
        # Try different API endpoint formats
        url = f"https://{FRESHDESK_DOMAIN}/api/v2/tickets?page={page}&per_page=1"
        print(f"Making request to: {url}")
        
        # Use Basic Auth which works
        response = requests.get(url, auth=(API_KEY, 'X'), headers=HEADERS)

        print(f"Response status code: {response.status_code}")
        print(f"Response headers: {dict(response.headers)}")
        
        if response.status_code != 200:
            print(f"Error fetching tickets page {page}: {response.status_code}")
            print(f"Response text: {response.text}")
            break

        try:
            tickets = response.json()
        except json.JSONDecodeError as e:
            print(f"JSON decode error: {e}")
            print(f"Response text: {response.text}")
            break

        if not tickets:
            print("No tickets found")
            break

        for ticket in tickets:
            enriched = enrich_ticket(ticket)
            all_tickets.append(enriched)

        print(f"Fetched and enriched page {page} with {len(tickets)} tickets")
        break  # Stop after first page to get only 1 ticket

    return all_tickets

def enrich_ticket(ticket):
    ticket_id = ticket.get("id")
    
    # Get individual ticket details to include description
    ticket_details = get_ticket_details(ticket_id)
    
    ticket_data = {
        "id": ticket_id,
        "subject": ticket.get("subject"),
        "description_text": ticket_details.get("description_text") if ticket_details else None,
        "description": ticket_details.get("description") if ticket_details else None,
        "custom_fields": ticket.get("custom_fields", {}),
        "created_at": ticket.get("created_at"),
        "updated_at": ticket.get("updated_at"),
        "status": ticket.get("status"),
        "priority": ticket.get("priority"),
        "requester_id": ticket.get("requester_id"),
        "attachments": ticket_details.get("attachments", []) if ticket_details else [],  # Main ticket attachments
        "conversations": fetch_conversations(ticket_id)
    }
    return ticket_data

def get_ticket_details(ticket_id):
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
            "body": convo.get("body"),  # Try different body field
            "private": convo.get("private"),
            "created_at": convo.get("created_at"),
            "attachments": attachments
        })

    return conversations

def save_to_json(data, filename='freshdesk_tickets_export.json'):
    with open(filename, 'w') as f:
        json.dump(data, f, indent=2)
    print(f"✅ Saved {len(data)} tickets to {filename}")

if __name__ == "__main__":
    tickets_data = fetch_tickets()
    save_to_json(tickets_data)