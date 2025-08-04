"""
Jira API Adapter
Handles all interactions with Jira instances
"""

import requests
import time
import json
import base64
from typing import Dict, List, Any, Optional, Generator
from datetime import datetime, timedelta
from pathlib import Path
import os
from tenacity import retry, stop_after_attempt, wait_exponential
from loguru import logger


class JiraAdapter:
    def __init__(self, jira_config: Dict[str, Any]):
        self.jira_config = jira_config
        self.base_url = jira_config['url'].rstrip('/')
        self.username = jira_config['username']
        self.api_token = jira_config['api_token']
        self.project_key = jira_config['project_key']
        self.issue_type = jira_config['issue_type']
        
        # Create basic auth header
        credentials = f"{self.username}:{self.api_token}"
        encoded_credentials = base64.b64encode(credentials.encode()).decode()
        
        self.headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Basic {encoded_credentials}'
        }
        
        self.rate_limit = jira_config.get('rate_limit', 100)
        self.timeout = jira_config.get('timeout', 30)
        self.batch_size = jira_config.get('batch_size', 50)
        
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
        """Make HTTP request to Jira API with retry logic"""
        self._rate_limit_wait()
        
        url = f"{self.base_url}/rest/api/3/{endpoint}"
        
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
            logger.error(f"Jira API request failed: {e}")
            raise
    
    def test_connection(self) -> bool:
        """Test connection to Jira instance"""
        try:
            response = self._make_request('GET', 'myself')
            return True
        except Exception as e:
            logger.error(f"Jira connection test failed: {e}")
            return False
    
    def get_project(self) -> Optional[Dict[str, Any]]:
        """Get project information"""
        try:
            response = self._make_request('GET', f'project/{self.project_key}')
            return response
        except Exception as e:
            logger.error(f"Failed to get project {self.project_key}: {e}")
            return None
    
    def get_issue_types(self) -> List[Dict[str, Any]]:
        """Get available issue types for the project"""
        try:
            response = self._make_request('GET', f'project/{self.project_key}')
            return response.get('issueTypes', [])
        except Exception as e:
            logger.error(f"Failed to get issue types: {e}")
            return []
    
    def get_issue_fields(self) -> List[Dict[str, Any]]:
        """Get available issue fields"""
        try:
            response = self._make_request('GET', 'field')
            return response
        except Exception as e:
            logger.error(f"Failed to get issue fields: {e}")
            return []
    
    def create_issue(self, issue_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Create a new issue in Jira"""
        try:
            payload = {
                'fields': {
                    'project': {
                        'key': self.project_key
                    },
                    'summary': issue_data.get('summary', 'No Subject'),
                    'description': issue_data.get('description', ''),
                    'issuetype': {
                        'name': self.issue_type
                    }
                }
            }
            
            # Add priority if available
            if 'priority' in issue_data:
                payload['fields']['priority'] = {
                    'name': issue_data['priority']
                }
            
            # Add assignee if available
            if 'assignee' in issue_data and issue_data['assignee']:
                payload['fields']['assignee'] = {
                    'accountId': issue_data['assignee']
                }
            
            # Add custom fields
            if 'custom_fields' in issue_data:
                for field_id, value in issue_data['custom_fields'].items():
                    payload['fields'][field_id] = value
            
            response = self._make_request('POST', 'issue', json=payload)
            return response
            
        except Exception as e:
            logger.error(f"Failed to create issue: {e}")
            return None
    
    def update_issue(self, issue_key: str, update_data: Dict[str, Any]) -> bool:
        """Update an existing issue"""
        try:
            payload = {
                'fields': update_data
            }
            
            self._make_request('PUT', f'issue/{issue_key}', json=payload)
            return True
            
        except Exception as e:
            logger.error(f"Failed to update issue {issue_key}: {e}")
            return False
    
    def add_comment(self, issue_key: str, comment_text: str, author: str = None) -> Optional[Dict[str, Any]]:
        """Add a comment to an issue"""
        try:
            payload = {
                'body': {
                    'type': 'doc',
                    'version': 1,
                    'content': [
                        {
                            'type': 'paragraph',
                            'content': [
                                {
                                    'type': 'text',
                                    'text': comment_text
                                }
                            ]
                        }
                    ]
                }
            }
            
            # Add author if specified
            if author:
                payload['author'] = {
                    'accountId': author
                }
            
            response = self._make_request('POST', f'issue/{issue_key}/comment', json=payload)
            return response
            
        except Exception as e:
            logger.error(f"Failed to add comment to issue {issue_key}: {e}")
            return None
    
    def add_attachment(self, issue_key: str, file_path: str) -> bool:
        """Add attachment to an issue"""
        try:
            self._rate_limit_wait()
            
            url = f"{self.base_url}/rest/api/3/issue/{issue_key}/attachments"
            
            with open(file_path, 'rb') as file:
                files = {'file': file}
                headers = {'Authorization': self.headers['Authorization']}
                
                response = requests.post(
                    url,
                    headers=headers,
                    files=files,
                    timeout=self.timeout
                )
                
                response.raise_for_status()
                return True
                
        except Exception as e:
            logger.error(f"Failed to add attachment to issue {issue_key}: {e}")
            return False
    
    def get_user(self, username: str) -> Optional[Dict[str, Any]]:
        """Get user information by username"""
        try:
            response = self._make_request('GET', f'user/search?query={username}')
            users = response.get('values', [])
            return users[0] if users else None
        except Exception as e:
            logger.error(f"Failed to get user {username}: {e}")
            return None
    
    def get_user_by_email(self, email: str) -> Optional[Dict[str, Any]]:
        """Get user information by email"""
        try:
            response = self._make_request('GET', f'user/search?query={email}')
            users = response.get('values', [])
            return users[0] if users else None
        except Exception as e:
            logger.error(f"Failed to get user by email {email}: {e}")
            return None
    
    def create_user(self, user_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Create a new user in Jira (if admin permissions)"""
        try:
            payload = {
                'name': user_data['username'],
                'emailAddress': user_data['email'],
                'displayName': user_data.get('display_name', user_data['username'])
            }
            
            response = self._make_request('POST', 'user', json=payload)
            return response
            
        except Exception as e:
            logger.error(f"Failed to create user: {e}")
            return None
    
    def get_issue(self, issue_key: str) -> Optional[Dict[str, Any]]:
        """Get issue by key"""
        try:
            response = self._make_request('GET', f'issue/{issue_key}')
            return response
        except Exception as e:
            logger.error(f"Failed to get issue {issue_key}: {e}")
            return None
    
    def search_issues(self, jql: str, max_results: int = 50) -> List[Dict[str, Any]]:
        """Search issues using JQL"""
        try:
            payload = {
                'jql': jql,
                'maxResults': max_results,
                'fields': ['summary', 'description', 'status', 'priority', 'assignee', 'created', 'updated']
            }
            
            response = self._make_request('POST', 'search', json=payload)
            return response.get('issues', [])
            
        except Exception as e:
            logger.error(f"Failed to search issues: {e}")
            return []
    
    def get_issue_comments(self, issue_key: str) -> List[Dict[str, Any]]:
        """Get comments for an issue"""
        try:
            response = self._make_request('GET', f'issue/{issue_key}/comment')
            return response.get('comments', [])
        except Exception as e:
            logger.error(f"Failed to get comments for issue {issue_key}: {e}")
            return []
    
    def get_issue_attachments(self, issue_key: str) -> List[Dict[str, Any]]:
        """Get attachments for an issue"""
        try:
            response = self._make_request('GET', f'issue/{issue_key}')
            return response.get('fields', {}).get('attachment', [])
        except Exception as e:
            logger.error(f"Failed to get attachments for issue {issue_key}: {e}")
            return []
    
    def get_issue_transitions(self, issue_key: str) -> List[Dict[str, Any]]:
        """Get available transitions for an issue"""
        try:
            response = self._make_request('GET', f'issue/{issue_key}/transitions')
            return response.get('transitions', [])
        except Exception as e:
            logger.error(f"Failed to get transitions for issue {issue_key}: {e}")
            return []
    
    def transition_issue(self, issue_key: str, transition_id: str, comment: str = None) -> bool:
        """Transition an issue to a different status"""
        try:
            payload = {
                'transition': {
                    'id': transition_id
                }
            }
            
            if comment:
                payload['update'] = {
                    'comment': [
                        {
                            'add': {
                                'body': comment
                            }
                        }
                    ]
                }
            
            self._make_request('POST', f'issue/{issue_key}/transitions', json=payload)
            return True
            
        except Exception as e:
            logger.error(f"Failed to transition issue {issue_key}: {e}")
            return False
    
    def get_project_components(self) -> List[Dict[str, Any]]:
        """Get project components"""
        try:
            response = self._make_request('GET', f'project/{self.project_key}/components')
            return response
        except Exception as e:
            logger.error(f"Failed to get project components: {e}")
            return []
    
    def create_component(self, component_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Create a new component in the project"""
        try:
            payload = {
                'name': component_data['name'],
                'project': self.project_key
            }
            
            if 'description' in component_data:
                payload['description'] = component_data['description']
            
            response = self._make_request('POST', f'project/{self.project_key}/components', json=payload)
            return response
            
        except Exception as e:
            logger.error(f"Failed to create component: {e}")
            return None
    
    def validate_issue_data(self, issue_data: Dict[str, Any]) -> bool:
        """Validate issue data before creation"""
        required_fields = ['summary']
        
        for field in required_fields:
            if field not in issue_data or not issue_data[field]:
                logger.warning(f"Missing required field in issue data: {field}")
                return False
        
        return True
    
    def get_project_metadata(self) -> Dict[str, Any]:
        """Get project metadata including fields, issue types, components"""
        try:
            project = self.get_project()
            issue_types = self.get_issue_types()
            fields = self.get_issue_fields()
            components = self.get_project_components()
            
            return {
                'project': project,
                'issue_types': issue_types,
                'fields': fields,
                'components': components
            }
            
        except Exception as e:
            logger.error(f"Failed to get project metadata: {e}")
            return {} 