"""
Data Mapper for Freshdesk to Jira Migration
Transforms Freshdesk data structures to Jira format
"""

import re
from typing import Dict, List, Any, Optional
from datetime import datetime
from loguru import logger


class DataMapper:
    def __init__(self, field_mapping: Dict[str, Any]):
        self.field_mapping = field_mapping
        self.priority_mapping = field_mapping.get('priority', {})
        self.status_mapping = field_mapping.get('status', {})
        self.custom_field_mapping = field_mapping.get('custom_fields', {})
        self.defaults = field_mapping.get('defaults', {})
        
        # User mapping cache
        self.user_mapping_cache = {}
    
    def map_ticket_to_issue(self, ticket_data: Dict[str, Any], 
                           user_mapping: Dict[str, str] = None) -> Dict[str, Any]:
        """Map Freshdesk ticket to Jira issue format"""
        try:
            issue_data = {
                'summary': self._map_summary(ticket_data),
                'description': self._map_description(ticket_data),
                'priority': self._map_priority(ticket_data),
                'assignee': self._map_assignee(ticket_data, user_mapping),
                'reporter': self._map_reporter(ticket_data, user_mapping),
                'custom_fields': self._map_custom_fields(ticket_data),
                'labels': self._map_labels(ticket_data),
                'components': self._map_components(ticket_data),
                'metadata': {
                    'freshdesk_id': ticket_data.get('id'),
                    'freshdesk_ticket_id': ticket_data.get('ticket_id'),
                    'migration_timestamp': datetime.now().isoformat()
                }
            }
            
            # Remove None values
            issue_data = {k: v for k, v in issue_data.items() if v is not None}
            
            return issue_data
            
        except Exception as e:
            logger.error(f"Error mapping ticket {ticket_data.get('id')}: {e}")
            return {}
    
    def _map_summary(self, ticket_data: Dict[str, Any]) -> str:
        """Map ticket subject to issue summary"""
        subject = ticket_data.get('subject', '')
        
        # Clean up subject
        if subject:
            # Remove common prefixes
            subject = re.sub(r'^\[.*?\]\s*', '', subject)
            # Truncate if too long (Jira limit is 255 characters)
            if len(subject) > 255:
                subject = subject[:252] + "..."
        
        return subject or "No Subject"
    
    def _map_description(self, ticket_data: Dict[str, Any]) -> str:
        """Map ticket description to Jira description format"""
        description = ticket_data.get('description', '')
        
        if not description:
            return ""
        
        # Convert to Jira format
        # Replace HTML tags with markdown-like syntax
        description = self._convert_html_to_jira_format(description)
        
        # Add ticket metadata
        metadata = []
        if ticket_data.get('source'):
            metadata.append(f"*Source:* {ticket_data['source']}")
        if ticket_data.get('type'):
            metadata.append(f"*Type:* {ticket_data['type']}")
        if ticket_data.get('created_at'):
            metadata.append(f"*Created:* {ticket_data['created_at']}")
        
        if metadata:
            description = f"{description}\n\n---\n{chr(10).join(metadata)}"
        
        return description
    
    def _convert_html_to_jira_format(self, html_text: str) -> str:
        """Convert HTML to Jira format"""
        # Basic HTML to Jira conversion
        conversions = [
            (r'<strong>(.*?)</strong>', r'*\\1*'),
            (r'<b>(.*?)</b>', r'*\\1*'),
            (r'<em>(.*?)</em>', r'_\\1_'),
            (r'<i>(.*?)</i>', r'_\\1_'),
            (r'<u>(.*?)</u>', r'+\\1+'),
            (r'<h1>(.*?)</h1>', r'h1. \\1'),
            (r'<h2>(.*?)</h2>', r'h2. \\1'),
            (r'<h3>(.*?)</h3>', r'h3. \\1'),
            (r'<br\s*/?>', r'\n'),
            (r'<p>(.*?)</p>', r'\\1\n\n'),
            (r'<ul>(.*?)</ul>', r'\\1'),
            (r'<ol>(.*?)</ol>', r'\\1'),
            (r'<li>(.*?)</li>', r'* \\1'),
            (r'<code>(.*?)</code>', r'{{{\\1}}}'),
            (r'<pre>(.*?)</pre>', r'{{{\\1}}}'),
        ]
        
        for pattern, replacement in conversions:
            html_text = re.sub(pattern, replacement, html_text, flags=re.IGNORECASE | re.DOTALL)
        
        # Remove remaining HTML tags
        html_text = re.sub(r'<[^>]+>', '', html_text)
        
        # Clean up extra whitespace
        html_text = re.sub(r'\n\s*\n\s*\n', '\n\n', html_text)
        
        return html_text.strip()
    
    def _map_priority(self, ticket_data: Dict[str, Any]) -> str:
        """Map Freshdesk priority to Jira priority"""
        freshdesk_priority = ticket_data.get('priority', '').lower()
        
        # Use mapping from config
        if freshdesk_priority in self.priority_mapping:
            return self.priority_mapping[freshdesk_priority]
        
        # Default mapping
        default_mapping = {
            'low': 'Low',
            'medium': 'Medium',
            'high': 'High',
            'urgent': 'Highest'
        }
        
        return default_mapping.get(freshdesk_priority, self.defaults.get('priority', 'Medium'))
    
    def _map_assignee(self, ticket_data: Dict[str, Any], 
                     user_mapping: Dict[str, str] = None) -> Optional[str]:
        """Map Freshdesk assignee to Jira assignee"""
        assignee_id = ticket_data.get('responder_id')
        
        if not assignee_id:
            return None
        
        # Use provided user mapping
        if user_mapping and str(assignee_id) in user_mapping:
            return user_mapping[str(assignee_id)]
        
        # Use cached mapping
        if str(assignee_id) in self.user_mapping_cache:
            return self.user_mapping_cache[str(assignee_id)]
        
        return None
    
    def _map_reporter(self, ticket_data: Dict[str, Any], 
                     user_mapping: Dict[str, str] = None) -> Optional[str]:
        """Map Freshdesk requester to Jira reporter"""
        requester_id = ticket_data.get('requester_id')
        
        if not requester_id:
            return None
        
        # Use provided user mapping
        if user_mapping and str(requester_id) in user_mapping:
            return user_mapping[str(requester_id)]
        
        # Use cached mapping
        if str(requester_id) in self.user_mapping_cache:
            return self.user_mapping_cache[str(requester_id)]
        
        return None
    
    def _map_custom_fields(self, ticket_data: Dict[str, Any]) -> Dict[str, Any]:
        """Map Freshdesk custom fields to Jira custom fields"""
        custom_fields = {}
        
        for freshdesk_field, jira_field in self.custom_field_mapping.items():
            if freshdesk_field in ticket_data:
                value = ticket_data[freshdesk_field]
                if value is not None and value != '':
                    custom_fields[jira_field] = value
        
        return custom_fields
    
    def _map_labels(self, ticket_data: Dict[str, Any]) -> List[str]:
        """Map Freshdesk tags to Jira labels"""
        labels = []
        
        # Add tags as labels
        tags = ticket_data.get('tags', [])
        if isinstance(tags, list):
            labels.extend(tags)
        
        # Add source system label
        labels.append('migrated-from-freshdesk')
        
        # Add ticket type label
        ticket_type = ticket_data.get('type', 'ticket')
        labels.append(f'freshdesk-{ticket_type}')
        
        return list(set(labels))  # Remove duplicates
    
    def _map_components(self, ticket_data: Dict[str, Any]) -> List[str]:
        """Map Freshdesk category/department to Jira components"""
        components = []
        
        # Map category to component
        category = ticket_data.get('category')
        if category:
            components.append(category)
        
        # Map department to component
        department = ticket_data.get('department')
        if department:
            components.append(department)
        
        return list(set(components))
    
    def map_comment_to_jira(self, comment_data: Dict[str, Any], 
                           user_mapping: Dict[str, str] = None) -> Dict[str, Any]:
        """Map Freshdesk comment to Jira comment format"""
        try:
            comment_text = comment_data.get('body', '')
            
            # Convert HTML to Jira format
            comment_text = self._convert_html_to_jira_format(comment_text)
            
            # Add comment metadata
            metadata = []
            if comment_data.get('created_at'):
                metadata.append(f"*Original Date:* {comment_data['created_at']}")
            if comment_data.get('source'):
                metadata.append(f"*Source:* {comment_data['source']}")
            
            if metadata:
                comment_text = f"{comment_text}\n\n---\n{chr(10).join(metadata)}"
            
            comment = {
                'body': comment_text,
                'author': self._map_comment_author(comment_data, user_mapping),
                'metadata': {
                    'freshdesk_comment_id': comment_data.get('id'),
                    'migration_timestamp': datetime.now().isoformat()
                }
            }
            
            return {k: v for k, v in comment.items() if v is not None}
            
        except Exception as e:
            logger.error(f"Error mapping comment {comment_data.get('id')}: {e}")
            return {}
    
    def _map_comment_author(self, comment_data: Dict[str, Any], 
                           user_mapping: Dict[str, str] = None) -> Optional[str]:
        """Map comment author to Jira user"""
        user_id = comment_data.get('user_id')
        
        if not user_id:
            return None
        
        # Use provided user mapping
        if user_mapping and str(user_id) in user_mapping:
            return user_mapping[str(user_id)]
        
        # Use cached mapping
        if str(user_id) in self.user_mapping_cache:
            return self.user_mapping_cache[str(user_id)]
        
        return None
    
    def map_user_to_jira(self, user_data: Dict[str, Any]) -> Dict[str, Any]:
        """Map Freshdesk user to Jira user format"""
        try:
            jira_user = {
                'username': user_data.get('email', '').split('@')[0],  # Use email prefix as username
                'email': user_data.get('email', ''),
                'display_name': user_data.get('name', ''),
                'active': user_data.get('active', True),
                'metadata': {
                    'freshdesk_user_id': user_data.get('id'),
                    'migration_timestamp': datetime.now().isoformat()
                }
            }
            
            return {k: v for k, v in jira_user.items() if v is not None}
            
        except Exception as e:
            logger.error(f"Error mapping user {user_data.get('id')}: {e}")
            return {}
    
    def map_attachment(self, attachment_data: Dict[str, Any]) -> Dict[str, Any]:
        """Map Freshdesk attachment metadata"""
        try:
            attachment = {
                'filename': attachment_data.get('name', ''),
                'content_type': attachment_data.get('content_type', ''),
                'size': attachment_data.get('size', 0),
                'url': attachment_data.get('attachment_url', ''),
                'metadata': {
                    'freshdesk_attachment_id': attachment_data.get('id'),
                    'migration_timestamp': datetime.now().isoformat()
                }
            }
            
            return {k: v for k, v in attachment.items() if v is not None}
            
        except Exception as e:
            logger.error(f"Error mapping attachment {attachment_data.get('id')}: {e}")
            return {}
    
    def update_user_mapping_cache(self, freshdesk_id: str, jira_account_id: str):
        """Update user mapping cache"""
        self.user_mapping_cache[freshdesk_id] = jira_account_id
    
    def get_user_mapping_cache(self) -> Dict[str, str]:
        """Get current user mapping cache"""
        return self.user_mapping_cache.copy()
    
    def clear_user_mapping_cache(self):
        """Clear user mapping cache"""
        self.user_mapping_cache.clear()
    
    def validate_mapping_config(self) -> bool:
        """Validate field mapping configuration"""
        required_sections = ['priority', 'status']
        
        for section in required_sections:
            if section not in self.field_mapping:
                logger.warning(f"Missing required mapping section: {section}")
                return False
        
        return True
    
    def get_mapping_summary(self) -> Dict[str, Any]:
        """Get summary of current mapping configuration"""
        return {
            'priority_mappings': len(self.priority_mapping),
            'status_mappings': len(self.status_mapping),
            'custom_field_mappings': len(self.custom_field_mapping),
            'user_mapping_cache_size': len(self.user_mapping_cache),
            'defaults': self.defaults
        } 