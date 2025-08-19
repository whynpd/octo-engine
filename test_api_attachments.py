#!/usr/bin/env python3
"""
Standalone API test script for Freshdesk API
Calls the tickets API with conversations and stores output in a file
"""

import requests
import json
import os
import logging
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
FRESHDESK_DOMAIN = os.getenv("FRESHDESK_DOMAIN")
API_KEY = os.getenv("FRESHDESK_API_KEY")
TEST_TICKET_ID = "26172"  # Default ticket ID for testing, can be changed

# Validate required environment variables
if not FRESHDESK_DOMAIN:
    raise ValueError("FRESHDESK_DOMAIN environment variable is not set")
if not API_KEY:
    raise ValueError("FRESHDESK_API_KEY environment variable is not set")

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_freshdesk_api(ticket_id=None):
    """
    Test the Freshdesk API by calling the tickets endpoint with conversations
    
    Args:
        ticket_id (str): The ticket ID to fetch. Defaults to TEST_TICKET_ID if None.
    
    Returns:
        dict: API response data or None if failed
    """
    if ticket_id is None:
        ticket_id = TEST_TICKET_ID
    
    # Construct API URL
    api_url = f"https://{FRESHDESK_DOMAIN}/api/v2/tickets/{ticket_id}?include=attachments"
    
    logger.info(f"Testing API call to: {api_url}")
    
    try:
        # Make API request with authentication
        response = requests.get(
            api_url,
            auth=(API_KEY, 'X'),  # Freshdesk uses API key with 'X' as password
            headers={
                'Content-Type': 'application/json',
                'User-Agent': 'Freshdesk-API-Test/1.0'
            },
            timeout=30
        )
        
        # Check if request was successful
        if response.status_code == 200:
            logger.info(f"✅ API call successful! Status: {response.status_code}")
            return response.json()
        else:
            logger.error(f"❌ API call failed! Status: {response.status_code}")
            logger.error(f"Response: {response.text}")
            return None
            
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Request failed with error: {str(e)}")
        return None
    except json.JSONDecodeError as e:
        logger.error(f"❌ Failed to parse JSON response: {str(e)}")
        return None

def save_api_response(data, ticket_id):
    """
    Save API response to a JSON file
    
    Args:
        data (dict): The API response data
        ticket_id (str): The ticket ID used for the filename
    """
    if data is None:
        logger.warning("No data to save")
        return
    
    # Create filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"api_test_attachments_output_ticket_{ticket_id}_{timestamp}.json"
    
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"✅ API response saved to: {filename}")
        logger.info(f"📊 Response contains {len(data)} top-level keys")
        
        # Log some basic info about the response
        if 'id' in data:
            logger.info(f"📋 Ticket ID: {data['id']}")
        if 'subject' in data:
            logger.info(f"📝 Subject: {data['subject']}")
        if 'conversations' in data:
            logger.info(f"💬 Conversations: {len(data['conversations'])} items")
            
    except Exception as e:
        logger.error(f"❌ Failed to save response to file: {str(e)}")

def main():
    """
    Main function to run the API test
    """
    print("🚀 Starting Freshdesk API Test")
    print(f"🌐 Domain: {FRESHDESK_DOMAIN}")
    print(f"🎫 Testing with Ticket ID: {TEST_TICKET_ID}")
    print("-" * 50)
    
    # Test the API
    response_data = test_freshdesk_api(TEST_TICKET_ID)
    
    # Save the response
    save_api_response(response_data, TEST_TICKET_ID)
    
    print("-" * 50)
    if response_data:
        print("✅ Test completed successfully!")
    else:
        print("❌ Test failed! Check logs for details.")

if __name__ == "__main__":
    main()
