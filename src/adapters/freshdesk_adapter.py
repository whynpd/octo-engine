"""
Freshdesk API Adapter
Handles all interactions with Freshdesk instances
"""

import requests
import time
import json
from typing import Dict, List, Any, Optional, Generator
from datetime import datetime, timedelta
from pathlib import Path
import os
from tenacity import retry, stop_after_attempt, wait_exponential
from loguru import logger


class FreshdeskAdapter:
    def __init__(self, instance_config: Dict[str, Any]):
        self.instance_config = instance_config
        self.base_url = instance_config['url'].rstrip('/')
        self.api_key = instance_config['api_key']
        self.headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Basic {self.api_key}'
        }
        self.headers.update(instance_config.get('headers', {}))
        self.rate_limit = instance_config.get('rate_limit', 100)
        self.timeout = instance_config.get('timeout', 30)
        self.batch_size = instance_config.get('batch_size', 100)
        
        # Rate limiting
        self.last_request_time = 0
        self.min_request_interval = 60.0 / self.rate_limit
    
    def _rate_limit_wait(self):
        """Implement rate limiting"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        if time_since_last < self.min_request_interval:
            sleep_time = self.min_request_interval - time_since_last
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def _make_request(self, method: str, endpoint: str, **kwargs) -> Dict[str, Any]:
        """Make HTTP request to Freshdesk API with retry logic"""
        self._rate_limit_wait()
        
        url = f"{self.base_url}/api/v2/{endpoint}"
        
        try:
            response = requests.request(
                method=method,
                url=url,
                headers=self.headers,
                timeout=self.timeout,
                **kwargs
            )
            
            response.raise_for_status()
            
            if response.content:
                return response.json()
            return {}
            
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {e}")
            raise
    
    def test_connection(self) -> bool:
        """Test connection to Freshdesk instance"""
        try:
            response = self._make_request('GET', 'tickets')
            return True
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False
    
    def get_tickets(self, filters: Optional[Dict[str, Any]] = None, 
                   page: int = 1, per_page: int = None) -> List[Dict[str, Any]]:
        """Get tickets from Freshdesk with optional filtering"""
        per_page = per_page or self.batch_size
        
        params = {
            'page': page,
            'per_page': per_page,
            'order_by': 'created_at',
            'order_type': 'asc'
        }
        
        if filters:
            # Build filter string
            filter_parts = []
            
            if 'statuses' in filters:
                status_filter = f"({','.join(f'status:{status}' for status in filters['statuses'])})"
                filter_parts.append(status_filter)
            
            if 'priorities' in filters:
                priority_filter = f"({','.join(f'priority:{priority}' for priority in filters['priorities'])})"
                filter_parts.append(priority_filter)
            
            if 'created_after' in filters:
                filter_parts.append(f"created_at:>'{filters['created_after']}'")
            
            if 'created_before' in filters:
                filter_parts.append(f"created_at:<'{filters['created_before']}'")
            
            if filter_parts:
                params['query'] = ' AND '.join(filter_parts)
        
        try:
            response = self._make_request('GET', 'tickets', params=params)
            return response.get('results', [])
        except Exception as e:
            logger.error(f"Failed to get tickets: {e}")
            return []
    
    def get_all_tickets(self, filters: Optional[Dict[str, Any]] = None) -> Generator[List[Dict[str, Any]], None, None]:
        """Get all tickets using pagination"""
        page = 1
        
        while True:
            tickets = self.get_tickets(filters, page, self.batch_size)
            
            if not tickets:
                break
            
            yield tickets
            page += 1
    
    def get_ticket(self, ticket_id: int) -> Optional[Dict[str, Any]]:
        """Get specific ticket by ID"""
        try:
            response = self._make_request('GET', f'tickets/{ticket_id}')
            return response
        except Exception as e:
            logger.error(f"Failed to get ticket {ticket_id}: {e}")
            return None
    
    def get_ticket_comments(self, ticket_id: int) -> List[Dict[str, Any]]:
        """Get comments for a specific ticket"""
        try:
            response = self._make_request('GET', f'tickets/{ticket_id}/conversations')
            return response.get('conversations', [])
        except Exception as e:
            logger.error(f"Failed to get comments for ticket {ticket_id}: {e}")
            return []
    
    def get_ticket_attachments(self, ticket_id: int) -> List[Dict[str, Any]]:
        """Get attachments for a specific ticket"""
        try:
            response = self._make_request('GET', f'tickets/{ticket_id}/attachments')
            return response.get('attachments', [])
        except Exception as e:
            logger.error(f"Failed to get attachments for ticket {ticket_id}: {e}")
            return []
    
    def download_attachment(self, attachment_url: str, download_path: str) -> bool:
        """Download attachment file"""
        try:
            self._rate_limit_wait()
            
            response = requests.get(
                attachment_url,
                headers=self.headers,
                timeout=self.timeout,
                stream=True
            )
            response.raise_for_status()
            
            # Create directory if it doesn't exist
            Path(download_path).parent.mkdir(parents=True, exist_ok=True)
            
            with open(download_path, 'wb') as file:
                for chunk in response.iter_content(chunk_size=8192):
                    file.write(chunk)
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to download attachment {attachment_url}: {e}")
            return False
    
    def get_users(self) -> List[Dict[str, Any]]:
        """Get all users from Freshdesk"""
        try:
            response = self._make_request('GET', 'contacts')
            return response.get('results', [])
        except Exception as e:
            logger.error(f"Failed to get users: {e}")
            return []
    
    def get_user(self, user_id: int) -> Optional[Dict[str, Any]]:
        """Get specific user by ID"""
        try:
            response = self._make_request('GET', f'contacts/{user_id}')
            return response
        except Exception as e:
            logger.error(f"Failed to get user {user_id}: {e}")
            return None
    
    def get_custom_fields(self) -> List[Dict[str, Any]]:
        """Get custom fields configuration"""
        try:
            response = self._make_request('GET', 'ticket_fields')
            return response.get('ticket_fields', [])
        except Exception as e:
            logger.error(f"Failed to get custom fields: {e}")
            return []
    
    def get_ticket_statistics(self) -> Dict[str, Any]:
        """Get ticket statistics"""
        try:
            # Get total count
            response = self._make_request('GET', 'tickets', params={'per_page': 1})
            total_count = response.get('total', 0)
            
            # Get status distribution
            status_stats = {}
            for status in ['open', 'pending', 'resolved', 'closed']:
                status_response = self._make_request('GET', 'tickets', 
                                                   params={'query': f'status:{status}', 'per_page': 1})
                status_stats[status] = status_response.get('total', 0)
            
            return {
                'total_tickets': total_count,
                'status_distribution': status_stats
            }
            
        except Exception as e:
            logger.error(f"Failed to get ticket statistics: {e}")
            return {}
    
    def search_tickets(self, query: str, page: int = 1, per_page: int = None) -> List[Dict[str, Any]]:
        """Search tickets using Freshdesk search API"""
        per_page = per_page or self.batch_size
        
        params = {
            'query': query,
            'page': page,
            'per_page': per_page
        }
        
        try:
            response = self._make_request('GET', 'search/tickets', params=params)
            return response.get('results', [])
        except Exception as e:
            logger.error(f"Failed to search tickets: {e}")
            return []
    
    def get_ticket_metadata(self) -> Dict[str, Any]:
        """Get ticket metadata including fields, statuses, priorities"""
        try:
            # Get ticket fields
            fields = self.get_custom_fields()
            
            # Get statuses and priorities (these might need to be hardcoded based on Freshdesk version)
            statuses = ['open', 'pending', 'resolved', 'closed']
            priorities = ['low', 'medium', 'high', 'urgent']
            
            return {
                'fields': fields,
                'statuses': statuses,
                'priorities': priorities
            }
            
        except Exception as e:
            logger.error(f"Failed to get ticket metadata: {e}")
            return {}
    
    def validate_ticket_data(self, ticket_data: Dict[str, Any]) -> bool:
        """Validate ticket data structure"""
        required_fields = ['id', 'subject', 'status', 'priority', 'created_at']
        
        for field in required_fields:
            if field not in ticket_data:
                logger.warning(f"Missing required field in ticket data: {field}")
                return False
        
        return True
    
    def get_ticket_count(self, filters: Optional[Dict[str, Any]] = None) -> int:
        """Get total count of tickets matching filters"""
        try:
            params = {'per_page': 1}
            
            if filters:
                # Build filter string (same logic as get_tickets)
                filter_parts = []
                
                if 'statuses' in filters:
                    status_filter = f"({','.join(f'status:{status}' for status in filters['statuses'])})"
                    filter_parts.append(status_filter)
                
                if 'priorities' in filters:
                    priority_filter = f"({','.join(f'priority:{priority}' for priority in filters['priorities'])})"
                    filter_parts.append(priority_filter)
                
                if 'created_after' in filters:
                    filter_parts.append(f"created_at:>'{filters['created_after']}'")
                
                if 'created_before' in filters:
                    filter_parts.append(f"created_at:<'{filters['created_before']}'")
                
                if filter_parts:
                    params['query'] = ' AND '.join(filter_parts)
            
            response = self._make_request('GET', 'tickets', params=params)
            return response.get('total', 0)
            
        except Exception as e:
            logger.error(f"Failed to get ticket count: {e}")
            return 0 