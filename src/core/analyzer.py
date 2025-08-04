"""
Data Analyzer for Freshdesk to Jira Migration
Analyzes Freshdesk data structure and provides insights
"""

import json
from typing import Dict, List, Any, Optional
from datetime import datetime
from pathlib import Path
from collections import Counter, defaultdict
from loguru import logger

from adapters.freshdesk_adapter import FreshdeskAdapter


class DataAnalyzer:
    def __init__(self, config_data: Dict[str, Any], specific_instance: str = None):
        self.config_data = config_data
        self.specific_instance = specific_instance
        self.freshdesk_adapter = None
        self.analysis_results = {}
        
        self._initialize_adapter()
    
    def _initialize_adapter(self):
        """Initialize Freshdesk adapter for analysis"""
        try:
            freshdesk_config = self.config_data['freshdesk']
            instances = freshdesk_config['instances']
            
            if self.specific_instance:
                # Use specific instance
                for instance_config in instances:
                    if instance_config['name'] == self.specific_instance:
                        self.freshdesk_adapter = FreshdeskAdapter(instance_config)
                        break
                else:
                    raise ValueError(f"Instance '{self.specific_instance}' not found")
            else:
                # Use first instance
                self.freshdesk_adapter = FreshdeskAdapter(instances[0])
            
        except Exception as e:
            logger.error(f"Failed to initialize Freshdesk adapter: {e}")
            raise
    
    def analyze_data(self, limit: int = 100):
        """Analyze Freshdesk data structure"""
        try:
            logger.info("Starting Freshdesk data analysis")
            
            # Analyze tickets
            ticket_analysis = self._analyze_tickets(limit)
            self.analysis_results['tickets'] = ticket_analysis
            
            # Analyze users
            user_analysis = self._analyze_users()
            self.analysis_results['users'] = user_analysis
            
            # Analyze custom fields
            field_analysis = self._analyze_custom_fields()
            self.analysis_results['custom_fields'] = field_analysis
            
            # Analyze attachments
            attachment_analysis = self._analyze_attachments(limit)
            self.analysis_results['attachments'] = attachment_analysis
            
            # Generate recommendations
            recommendations = self._generate_recommendations()
            self.analysis_results['recommendations'] = recommendations
            
            # Save analysis results
            self._save_analysis_results()
            
            # Display analysis summary
            self._display_analysis_summary()
            
            logger.info("Data analysis completed")
            
        except Exception as e:
            logger.error(f"Data analysis failed: {e}")
            raise
    
    def _analyze_tickets(self, limit: int) -> Dict[str, Any]:
        """Analyze ticket data structure"""
        try:
            logger.info("Analyzing ticket data...")
            
            analysis = {
                'total_count': 0,
                'status_distribution': {},
                'priority_distribution': {},
                'type_distribution': {},
                'source_distribution': {},
                'date_range': {},
                'field_usage': {},
                'sample_tickets': []
            }
            
            # Get ticket statistics
            stats = self.freshdesk_adapter.get_ticket_statistics()
            analysis['total_count'] = stats.get('total_tickets', 0)
            analysis['status_distribution'] = stats.get('status_distribution', {})
            
            # Get sample tickets for detailed analysis
            sample_tickets = []
            ticket_count = 0
            
            for tickets_batch in self.freshdesk_adapter.get_all_tickets():
                for ticket in tickets_batch:
                    if ticket_count >= limit:
                        break
                    
                    sample_tickets.append(ticket)
                    ticket_count += 1
                    
                    # Analyze priority
                    priority = ticket.get('priority', 'unknown')
                    analysis['priority_distribution'][priority] = analysis['priority_distribution'].get(priority, 0) + 1
                    
                    # Analyze type
                    ticket_type = ticket.get('type', 'unknown')
                    analysis['type_distribution'][ticket_type] = analysis['type_distribution'].get(ticket_type, 0) + 1
                    
                    # Analyze source
                    source = ticket.get('source', 'unknown')
                    analysis['source_distribution'][source] = analysis['source_distribution'].get(source, 0) + 1
                
                if ticket_count >= limit:
                    break
            
            analysis['sample_tickets'] = sample_tickets[:10]  # Keep only first 10 for detailed analysis
            
            # Analyze field usage
            if sample_tickets:
                analysis['field_usage'] = self._analyze_field_usage(sample_tickets)
            
            # Analyze date range
            if sample_tickets:
                created_dates = [ticket.get('created_at') for ticket in sample_tickets if ticket.get('created_at')]
                if created_dates:
                    analysis['date_range'] = {
                        'earliest': min(created_dates),
                        'latest': max(created_dates)
                    }
            
            return analysis
            
        except Exception as e:
            logger.error(f"Failed to analyze tickets: {e}")
            return {}
    
    def _analyze_users(self) -> Dict[str, Any]:
        """Analyze user data structure"""
        try:
            logger.info("Analyzing user data...")
            
            analysis = {
                'total_count': 0,
                'active_count': 0,
                'inactive_count': 0,
                'role_distribution': {},
                'sample_users': []
            }
            
            users = self.freshdesk_adapter.get_users()
            analysis['total_count'] = len(users)
            
            for user in users:
                # Analyze active status
                if user.get('active', True):
                    analysis['active_count'] += 1
                else:
                    analysis['inactive_count'] += 1
                
                # Analyze role
                role = user.get('role', 'unknown')
                analysis['role_distribution'][role] = analysis['role_distribution'].get(role, 0) + 1
            
            # Keep sample users
            analysis['sample_users'] = users[:10]
            
            return analysis
            
        except Exception as e:
            logger.error(f"Failed to analyze users: {e}")
            return {}
    
    def _analyze_custom_fields(self) -> Dict[str, Any]:
        """Analyze custom fields structure"""
        try:
            logger.info("Analyzing custom fields...")
            
            analysis = {
                'total_count': 0,
                'field_types': {},
                'required_fields': [],
                'optional_fields': [],
                'field_details': []
            }
            
            fields = self.freshdesk_adapter.get_custom_fields()
            analysis['total_count'] = len(fields)
            
            for field in fields:
                field_type = field.get('type', 'unknown')
                analysis['field_types'][field_type] = analysis['field_types'].get(field_type, 0) + 1
                
                # Check if required
                if field.get('required', False):
                    analysis['required_fields'].append(field.get('name', 'Unknown'))
                else:
                    analysis['optional_fields'].append(field.get('name', 'Unknown'))
                
                # Store field details
                analysis['field_details'].append({
                    'name': field.get('name', 'Unknown'),
                    'type': field_type,
                    'required': field.get('required', False),
                    'description': field.get('description', '')
                })
            
            return analysis
            
        except Exception as e:
            logger.error(f"Failed to analyze custom fields: {e}")
            return {}
    
    def _analyze_attachments(self, limit: int) -> Dict[str, Any]:
        """Analyze attachment data"""
        try:
            logger.info("Analyzing attachment data...")
            
            analysis = {
                'total_count': 0,
                'size_distribution': {},
                'type_distribution': {},
                'sample_attachments': []
            }
            
            # Analyze attachments from sample tickets
            ticket_count = 0
            total_attachments = 0
            
            for tickets_batch in self.freshdesk_adapter.get_all_tickets():
                for ticket in tickets_batch:
                    if ticket_count >= limit:
                        break
                    
                    attachments = self.freshdesk_adapter.get_ticket_attachments(ticket['id'])
                    total_attachments += len(attachments)
                    
                    for attachment in attachments:
                        # Analyze file type
                        file_name = attachment.get('name', '')
                        file_extension = Path(file_name).suffix.lower() if file_name else ''
                        analysis['type_distribution'][file_extension] = analysis['type_distribution'].get(file_extension, 0) + 1
                        
                        # Analyze file size
                        size_mb = attachment.get('size', 0) / (1024 * 1024)
                        size_category = self._categorize_file_size(size_mb)
                        analysis['size_distribution'][size_category] = analysis['size_distribution'].get(size_category, 0) + 1
                        
                        # Keep sample attachments
                        if len(analysis['sample_attachments']) < 10:
                            analysis['sample_attachments'].append(attachment)
                    
                    ticket_count += 1
                
                if ticket_count >= limit:
                    break
            
            analysis['total_count'] = total_attachments
            
            return analysis
            
        except Exception as e:
            logger.error(f"Failed to analyze attachments: {e}")
            return {}
    
    def _analyze_field_usage(self, tickets: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze field usage in tickets"""
        try:
            field_usage = {}
            
            for ticket in tickets:
                for field_name, field_value in ticket.items():
                    if field_name not in field_usage:
                        field_usage[field_name] = {
                            'count': 0,
                            'null_count': 0,
                            'sample_values': set()
                        }
                    
                    field_usage[field_name]['count'] += 1
                    
                    if field_value is None or field_value == '':
                        field_usage[field_name]['null_count'] += 1
                    else:
                        # Keep sample values (limit to 5)
                        if len(field_usage[field_name]['sample_values']) < 5:
                            field_usage[field_name]['sample_values'].add(str(field_value)[:50])
            
            # Convert sets to lists for JSON serialization
            for field_name in field_usage:
                field_usage[field_name]['sample_values'] = list(field_usage[field_name]['sample_values'])
                field_usage[field_name]['usage_percentage'] = (
                    (field_usage[field_name]['count'] - field_usage[field_name]['null_count']) / 
                    field_usage[field_name]['count'] * 100
                )
            
            return field_usage
            
        except Exception as e:
            logger.error(f"Failed to analyze field usage: {e}")
            return {}
    
    def _categorize_file_size(self, size_mb: float) -> str:
        """Categorize file size"""
        if size_mb < 1:
            return '< 1MB'
        elif size_mb < 5:
            return '1-5MB'
        elif size_mb < 10:
            return '5-10MB'
        elif size_mb < 50:
            return '10-50MB'
        else:
            return '> 50MB'
    
    def _generate_recommendations(self) -> List[str]:
        """Generate migration recommendations based on analysis"""
        try:
            recommendations = []
            
            # Ticket recommendations
            ticket_analysis = self.analysis_results.get('tickets', {})
            if ticket_analysis:
                total_tickets = ticket_analysis.get('total_count', 0)
                if total_tickets > 100000:
                    recommendations.append(f"Large dataset detected ({total_tickets:,} tickets). Consider running migration in batches.")
                
                # Status mapping recommendations
                status_dist = ticket_analysis.get('status_distribution', {})
                if status_dist:
                    recommendations.append(f"Found {len(status_dist)} different statuses. Ensure proper status mapping in configuration.")
            
            # User recommendations
            user_analysis = self.analysis_results.get('users', {})
            if user_analysis:
                inactive_users = user_analysis.get('inactive_count', 0)
                if inactive_users > 0:
                    recommendations.append(f"Found {inactive_users} inactive users. Consider whether to migrate them.")
            
            # Custom field recommendations
            field_analysis = self.analysis_results.get('custom_fields', {})
            if field_analysis:
                total_fields = field_analysis.get('total_count', 0)
                if total_fields > 20:
                    recommendations.append(f"Large number of custom fields ({total_fields}). Review field mapping carefully.")
                
                required_fields = field_analysis.get('required_fields', [])
                if required_fields:
                    recommendations.append(f"Found {len(required_fields)} required custom fields. Ensure these are properly mapped.")
            
            # Attachment recommendations
            attachment_analysis = self.analysis_results.get('attachments', {})
            if attachment_analysis:
                total_attachments = attachment_analysis.get('total_count', 0)
                if total_attachments > 10000:
                    recommendations.append(f"Large number of attachments ({total_attachments}). Consider attachment size limits and storage requirements.")
                
                size_dist = attachment_analysis.get('size_distribution', {})
                if '> 50MB' in size_dist:
                    recommendations.append("Found large attachments (>50MB). Review attachment size limits in configuration.")
            
            return recommendations
            
        except Exception as e:
            logger.error(f"Failed to generate recommendations: {e}")
            return []
    
    def _save_analysis_results(self):
        """Save analysis results to file"""
        try:
            output_file = Path('./data/analysis_results.json')
            output_file.parent.mkdir(parents=True, exist_ok=True)
            
            with open(output_file, 'w') as f:
                json.dump(self.analysis_results, f, indent=2, default=str)
            
            logger.info(f"Analysis results saved to {output_file}")
            
        except Exception as e:
            logger.error(f"Failed to save analysis results: {e}")
    
    def _display_analysis_summary(self):
        """Display analysis summary"""
        try:
            print("\n" + "="*60)
            print("FRESHDESK DATA ANALYSIS SUMMARY")
            print("="*60)
            
            # Ticket summary
            ticket_analysis = self.analysis_results.get('tickets', {})
            if ticket_analysis:
                print(f"\nTickets:")
                print(f"  Total Count: {ticket_analysis.get('total_count', 0):,}")
                print(f"  Statuses: {len(ticket_analysis.get('status_distribution', {}))}")
                print(f"  Priorities: {len(ticket_analysis.get('priority_distribution', {}))}")
                print(f"  Types: {len(ticket_analysis.get('type_distribution', {}))}")
            
            # User summary
            user_analysis = self.analysis_results.get('users', {})
            if user_analysis:
                print(f"\nUsers:")
                print(f"  Total Count: {user_analysis.get('total_count', 0):,}")
                print(f"  Active: {user_analysis.get('active_count', 0):,}")
                print(f"  Inactive: {user_analysis.get('inactive_count', 0):,}")
                print(f"  Roles: {len(user_analysis.get('role_distribution', {}))}")
            
            # Custom fields summary
            field_analysis = self.analysis_results.get('custom_fields', {})
            if field_analysis:
                print(f"\nCustom Fields:")
                print(f"  Total Count: {field_analysis.get('total_count', 0)}")
                print(f"  Required: {len(field_analysis.get('required_fields', []))}")
                print(f"  Optional: {len(field_analysis.get('optional_fields', []))}")
                print(f"  Types: {len(field_analysis.get('field_types', {}))}")
            
            # Attachments summary
            attachment_analysis = self.analysis_results.get('attachments', {})
            if attachment_analysis:
                print(f"\nAttachments:")
                print(f"  Total Count: {attachment_analysis.get('total_count', 0):,}")
                print(f"  File Types: {len(attachment_analysis.get('type_distribution', {}))}")
                print(f"  Size Categories: {len(attachment_analysis.get('size_distribution', {}))}")
            
            # Recommendations
            recommendations = self.analysis_results.get('recommendations', [])
            if recommendations:
                print(f"\nRecommendations:")
                for i, rec in enumerate(recommendations, 1):
                    print(f"  {i}. {rec}")
            
            print("="*60 + "\n")
            
        except Exception as e:
            logger.error(f"Failed to display analysis summary: {e}")
    
    def get_analysis_results(self) -> Dict[str, Any]:
        """Get analysis results"""
        return self.analysis_results.copy() 