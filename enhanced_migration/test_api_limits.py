#!/usr/bin/env python3
"""
Test Freshdesk API Rate Limits
This script tests how many requests you can make before hitting rate limits.
"""

import os
import requests
import threading
import time
from dotenv import load_dotenv
import logging

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Freshdesk configuration
FRESHDESK_DOMAIN = os.getenv('FRESHDESK_DOMAIN')
API_KEY = os.getenv('FRESHDESK_API_KEY')

if not FRESHDESK_DOMAIN or not API_KEY:
    print("❌ Error: FRESHDESK_DOMAIN and FRESHDESK_API_KEY must be set in .env file")
    exit(1)

# Test configuration
REQUEST_TIMEOUT = 10  # seconds per request
test_ticket_id = 26507  # Use existing ticket ID

def test_single_request():
    """Test a single request to verify API is working"""
    print("🔍 Testing single request to verify API connectivity...")
    
    try:
        print(f"  🔑 Using auth=(API_KEY, 'X') method")
        
        url = f"https://{FRESHDESK_DOMAIN}/api/v2/tickets/{test_ticket_id}"
        headers = {"Content-Type": "application/json"}
        
        response = requests.get(url, auth=(API_KEY, 'X'), headers=headers, timeout=REQUEST_TIMEOUT)
        
        print(f"    📊 Status: {response.status_code}")
        
        if response.status_code == 200:
            print(f"    ✅ SUCCESS with auth=(API_KEY, 'X')!")
            return True
        elif response.status_code == 401:
            print(f"    ❌ Still unauthorized - check your API key")
            return False
        elif response.status_code == 429:
            print(f"    🚫 Rate limited - API is working!")
            return True  # Rate limiting means API is accessible
        else:
            print(f"    ⚠️  Unexpected status {response.status_code}")
            return False
            
    except Exception as e:
        print(f"    ❌ Error: {e}")
        return False

def test_rate_limit(requests_per_second, duration=60):  # Changed from 10 to 60 seconds
    """Test a specific rate limit scenario"""
    total_requests = int(requests_per_second * duration)  # Convert to integer
    
    print(f"\n📊 Testing: {requests_per_second:.2f} requests/second for {duration} seconds")
    print(f"   Total requests: {total_requests}")
    print("-" * 50)
    
    # Results tracking
    results = {
        'success': 0,
        'rate_limited': 0,
        'timeout': 0,
        'connection_error': 0,
        'other_error': 0
    }
    
    # Thread synchronization
    results_lock = threading.Lock()
    request_complete = threading.Event()
    
    def make_request(thread_id):
        """Make a single API request"""
        try:
            url = f"https://{FRESHDESK_DOMAIN}/api/v2/tickets/{test_ticket_id}"
            headers = {"Content-Type": "application/json"}
            
            response = requests.get(url, auth=(API_KEY, 'X'), headers=headers, timeout=REQUEST_TIMEOUT)
            
            with results_lock:
                if response.status_code == 200:
                    results['success'] += 1
                elif response.status_code == 429:  # Rate limited
                    results['rate_limited'] += 1
                else:
                    results['other_error'] += 1
                    
        except requests.exceptions.Timeout:
            with results_lock:
                results['timeout'] += 1
        except requests.exceptions.ConnectionError:
            with results_lock:
                results['connection_error'] += 1
        except Exception as e:
            with results_lock:
                results['other_error'] += 1
    
    # Launch threads with controlled timing
    start_time = time.time()
    threads = []
    
    for i in range(total_requests):
        thread = threading.Thread(target=make_request, args=(i,))
        threads.append(thread)
        thread.start()
        
        # Calculate delay to achieve target rate
        if i < total_requests - 1:  # Don't sleep after last thread
            delay = 1.0 / requests_per_second
            time.sleep(delay)
    
    # Wait for all threads to complete
    for thread in threads:
        thread.join()
    
    end_time = time.time()
    actual_duration = end_time - start_time
    
    # Calculate metrics
    total_requests_made = sum(results.values())
    success_rate = (results['success'] / total_requests_made * 100) if total_requests_made > 0 else 0
    actual_rate = total_requests_made / actual_duration if actual_duration > 0 else 0
    
    # Display results
    print(f"📈 Results:")
    print(f"  ✅ Successful: {results['success']}")
    print(f"  🚫 Rate limited: {results['rate_limited']}")
    print(f"  ⏰ Timeouts: {results['timeout']}")
    print(f"  🔌 Connection errors: {results['connection_error']}")
    print(f"  ❌ Other errors: {results['other_error']}")
    print(f"  📊 Total: {total_requests_made}")
    print(f"  ⏱️  Duration: {actual_duration:.2f}s")
    print(f"  🚀 Actual rate: {actual_rate:.2f} req/s")
    print(f"  📈 Success rate: {success_rate:.1f}%")
    
    # Determine if this rate is sustainable
    if results['rate_limited'] == 0 and success_rate >= 95:
        print(f"  🟢 SUSTAINABLE: {requests_per_second} req/s works well")
        return True, results['success']
    elif results['rate_limited'] > 0:
        print(f"  🟡 PARTIALLY SUSTAINABLE: {requests_per_second} req/s hits some limits")
        return False, results['success']
    else:
        print(f"  🔴 NOT SUSTAINABLE: {requests_per_second} req/s fails")
        return False, results['success']

def find_rate_limit():
    """Systematically find the actual rate limit"""
    print(f"🚀 Testing Freshdesk API rate limits for {FRESHDESK_DOMAIN}")
    print(f"🎯 Target: Find sustainable request rate for individual ticket API")
    print(f"🔍 Testing ticket ID: {test_ticket_id}")
    print("=" * 70)
    
    # First, test if API is working at all
    if not test_single_request():
        print("\n❌ API is not working or has extremely strict limits")
        return
    
    print("\n" + "=" * 70)
    print("🔍 SYSTEMATIC RATE LIMIT TESTING")
    print("=" * 70)
    
    # Test different rates systematically
    test_rates = [
        
          # 350 requests per minute (5.83 req/s)
        
        700.0/60,
         # 700 requests per minute (11.67 req/s)
    ]
    
    sustainable_rates = []
    max_successful_requests = 0
    
    for rate in test_rates:
        try:
            sustainable, successful_requests = test_rate_limit(rate, duration=60)  # Changed from 10 to 60 seconds
            
            if sustainable:
                sustainable_rates.append(rate)
                max_successful_requests = max(max_successful_requests, successful_requests)
            
            # If we hit rate limits, wait before next test
            if not sustainable:
                print(f"  ⏳ Waiting 30 seconds before next test...")
                time.sleep(30)
            
        except KeyboardInterrupt:
            print("\n⏹️  Testing interrupted by user")
            break
        except Exception as e:
            print(f"  ❌ Error testing rate {rate}: {e}")
            continue
    
    # Summary and recommendations
    print("\n" + "=" * 70)
    print("📊 RATE LIMIT ANALYSIS SUMMARY")
    print("=" * 70)
    
    if sustainable_rates:
        max_sustainable_rate = max(sustainable_rates)
        print(f"✅ SUSTAINABLE RATES FOUND:")
        print(f"   🚀 Maximum sustainable rate: {max_sustainable_rate} requests/second")
        print(f"   📊 All sustainable rates: {sustainable_rates}")
        print(f"   🎯 Recommended production rate: {max_sustainable_rate * 0.8:.2f} req/s (80% of max)")
        
        # Calculate migration time
        if max_sustainable_rate > 0:
            tickets_per_hour = max_sustainable_rate * 3600
            production_rate = max_sustainable_rate * 0.8
            production_tickets_per_hour = production_rate * 3600
            
            print(f"\n📈 MIGRATION PLANNING:")
            print(f"   🚀 Maximum capacity: {tickets_per_hour:,.0f} tickets/hour")
            print(f"   🛡️  Safe production: {production_tickets_per_hour:,.0f} tickets/hour")
            
            # Example calculations
            for total_tickets in [1000, 10000, 100000, 1000000]:
                hours = total_tickets / production_tickets_per_hour
                days = hours / 24
                print(f"   📅 {total_tickets:,} tickets: {hours:.1f} hours ({days:.1f} days)")
    else:
        print("❌ NO SUSTAINABLE RATES FOUND")
        print("   🚫 Freshdesk has extremely strict rate limiting")
        print("   💡 Consider contacting Freshdesk support for higher limits")
        print("   💡 Or use alternative migration strategies")
    
    print("\n💡 RECOMMENDATIONS:")
    if sustainable_rates:
        print(f"   🛡️  Use {max_sustainable_rate * 0.8:.2f} requests/second for production")
        print(f"   ⏱️  Add delays between requests: {1/max_sustainable_rate:.2f} seconds")
        print(f"   🔄 Implement exponential backoff for rate limit handling")
    else:
        print(f"   🚫 Current approach not feasible for large migrations")
        print(f"   📞 Contact Freshdesk support immediately")
        print(f"   🔍 Explore bulk export or migration services")

if __name__ == "__main__":
    find_rate_limit() 