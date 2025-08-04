"""
Configuration Validator for Freshdesk to Jira Migration
Validates configuration and tests connectivity
"""

import re
from typing import Dict, Any, List
from urllib.parse import urlparse
from loguru import logger


class ConfigValidator:
    def __init__(self, config_data: Dict[str, Any]):
        self.config_data = config_data
        self.errors = []
        self.warnings = []
    
    def validate(self) -> bool:
        """Validate complete configuration"""
        self.errors = []
        self.warnings = []
        
        # Validate all sections
        self._validate_freshdesk_config()
        self._validate_jira_config()
        self._validate_migration_config()
        self._validate_field_mapping()
        self._validate_performance_config()
        
        # Report results
        if self.errors:
            logger.error("Configuration validation failed:")
            for error in self.errors:
                logger.error(f"  - {error}")
        
        if self.warnings:
            logger.warning("Configuration warnings:")
            for warning in self.warnings:
                logger.warning(f"  - {warning}")
        
        return len(self.errors) == 0
    
    def _validate_freshdesk_config(self):
        """Validate Freshdesk configuration"""
        freshdesk_config = self.config_data.get('freshdesk', {})
        
        if not freshdesk_config:
            self.errors.append("Freshdesk configuration is missing")
            return
        
        instances = freshdesk_config.get('instances', [])
        if not instances:
            self.errors.append("No Freshdesk instances configured")
            return
        
        for i, instance in enumerate(instances):
            self._validate_freshdesk_instance(instance, i)
    
    def _validate_freshdesk_instance(self, instance: Dict[str, Any], index: int):
        """Validate individual Freshdesk instance configuration"""
        prefix = f"Freshdesk instance {index + 1}"
        
        # Required fields
        required_fields = ['name', 'url', 'api_key']
        for field in required_fields:
            if not instance.get(field):
                self.errors.append(f"{prefix}: Missing required field '{field}'")
        
        # URL validation
        url = instance.get('url', '')
        if url and not self._is_valid_url(url):
            self.errors.append(f"{prefix}: Invalid URL format: {url}")
        
        # API key validation
        api_key = instance.get('api_key', '')
        if api_key and len(api_key) < 10:
            self.warnings.append(f"{prefix}: API key seems too short")
        
        # Rate limiting validation
        rate_limit = instance.get('rate_limit', 100)
        if rate_limit <= 0 or rate_limit > 1000:
            self.warnings.append(f"{prefix}: Rate limit {rate_limit} seems unusual")
        
        # Timeout validation
        timeout = instance.get('timeout', 30)
        if timeout <= 0 or timeout > 300:
            self.warnings.append(f"{prefix}: Timeout {timeout} seconds seems unusual")
        
        # Batch size validation
        batch_size = instance.get('batch_size', 100)
        if batch_size <= 0 or batch_size > 1000:
            self.warnings.append(f"{prefix}: Batch size {batch_size} seems unusual")
    
    def _validate_jira_config(self):
        """Validate Jira configuration"""
        jira_config = self.config_data.get('jira', {})
        
        if not jira_config:
            self.errors.append("Jira configuration is missing")
            return
        
        # Required fields
        required_fields = ['url', 'username', 'api_token', 'project_key']
        for field in required_fields:
            if not jira_config.get(field):
                self.errors.append(f"Jira: Missing required field '{field}'")
        
        # URL validation
        url = jira_config.get('url', '')
        if url and not self._is_valid_url(url):
            self.errors.append(f"Jira: Invalid URL format: {url}")
        
        # Project key validation
        project_key = jira_config.get('project_key', '')
        if project_key and not re.match(r'^[A-Z]+$', project_key):
            self.errors.append(f"Jira: Invalid project key format: {project_key}")
        
        # API token validation
        api_token = jira_config.get('api_token', '')
        if api_token and len(api_token) < 10:
            self.warnings.append("Jira: API token seems too short")
        
        # Issue type validation
        issue_type = jira_config.get('issue_type', 'Incident')
        if issue_type not in ['Bug', 'Task', 'Story', 'Incident', 'Request']:
            self.warnings.append(f"Jira: Issue type '{issue_type}' might not exist")
    
    def _validate_migration_config(self):
        """Validate migration configuration"""
        migration_config = self.config_data.get('migration', {})
        
        if not migration_config:
            self.errors.append("Migration configuration is missing")
            return
        
        # Checkpoint interval validation
        checkpoint_interval = migration_config.get('checkpoint_interval', 1000)
        if checkpoint_interval <= 0:
            self.errors.append("Migration: Checkpoint interval must be positive")
        elif checkpoint_interval < 100:
            self.warnings.append("Migration: Checkpoint interval seems too small")
        
        # Retry configuration validation
        max_retries = migration_config.get('max_retries', 3)
        if max_retries < 0 or max_retries > 10:
            self.warnings.append(f"Migration: Max retries {max_retries} seems unusual")
        
        retry_delay = migration_config.get('retry_delay', 5)
        if retry_delay < 0 or retry_delay > 60:
            self.warnings.append(f"Migration: Retry delay {retry_delay} seconds seems unusual")
        
        # Attachment configuration validation
        attachments_config = migration_config.get('attachments', {})
        if attachments_config:
            max_file_size = attachments_config.get('max_file_size_mb', 50)
            if max_file_size <= 0 or max_file_size > 1000:
                self.warnings.append(f"Migration: Max file size {max_file_size}MB seems unusual")
    
    def _validate_field_mapping(self):
        """Validate field mapping configuration"""
        field_mapping = self.config_data.get('field_mapping', {})
        
        if not field_mapping:
            self.warnings.append("Field mapping configuration is missing")
            return
        
        # Priority mapping validation
        priority_mapping = field_mapping.get('priority', {})
        if not priority_mapping:
            self.warnings.append("Priority mapping is missing")
        else:
            valid_priorities = ['Low', 'Medium', 'High', 'Highest']
            for freshdesk_priority, jira_priority in priority_mapping.items():
                if jira_priority not in valid_priorities:
                    self.warnings.append(f"Priority mapping: '{jira_priority}' might not be valid")
        
        # Status mapping validation
        status_mapping = field_mapping.get('status', {})
        if not status_mapping:
            self.warnings.append("Status mapping is missing")
        else:
            valid_statuses = ['To Do', 'In Progress', 'Done', 'Cancelled']
            for freshdesk_status, jira_status in status_mapping.items():
                if jira_status not in valid_statuses:
                    self.warnings.append(f"Status mapping: '{jira_status}' might not be valid")
    
    def _validate_performance_config(self):
        """Validate performance configuration"""
        performance_config = self.config_data.get('performance', {})
        
        if not performance_config:
            return
        
        # Concurrent requests validation
        max_concurrent = performance_config.get('max_concurrent_requests', 10)
        if max_concurrent <= 0 or max_concurrent > 50:
            self.warnings.append(f"Performance: Max concurrent requests {max_concurrent} seems unusual")
        
        # Memory usage validation
        max_memory = performance_config.get('max_memory_usage_mb', 2048)
        if max_memory <= 0 or max_memory > 16384:
            self.warnings.append(f"Performance: Max memory usage {max_memory}MB seems unusual")
    
    def _is_valid_url(self, url: str) -> bool:
        """Validate URL format"""
        try:
            result = urlparse(url)
            return all([result.scheme, result.netloc])
        except Exception:
            return False
    
    def test_connectivity(self) -> bool:
        """Test connectivity to all configured systems"""
        try:
            from adapters.freshdesk_adapter import FreshdeskAdapter
            from adapters.jira_adapter import JiraAdapter
            
            # Test Freshdesk connectivity
            freshdesk_config = self.config_data.get('freshdesk', {})
            instances = freshdesk_config.get('instances', [])
            
            for instance in instances:
                try:
                    adapter = FreshdeskAdapter(instance)
                    if not adapter.test_connection():
                        self.errors.append(f"Failed to connect to Freshdesk instance: {instance.get('name')}")
                        return False
                    logger.info(f"✓ Connected to Freshdesk instance: {instance.get('name')}")
                except Exception as e:
                    self.errors.append(f"Failed to connect to Freshdesk instance {instance.get('name')}: {e}")
                    return False
            
            # Test Jira connectivity
            jira_config = self.config_data.get('jira', {})
            try:
                adapter = JiraAdapter(jira_config)
                if not adapter.test_connection():
                    self.errors.append("Failed to connect to Jira instance")
                    return False
                logger.info("✓ Connected to Jira instance")
            except Exception as e:
                self.errors.append(f"Failed to connect to Jira instance: {e}")
                return False
            
            return True
            
        except Exception as e:
            self.errors.append(f"Connectivity test failed: {e}")
            return False
    
    def get_validation_summary(self) -> Dict[str, Any]:
        """Get validation summary"""
        return {
            'valid': len(self.errors) == 0,
            'error_count': len(self.errors),
            'warning_count': len(self.warnings),
            'errors': self.errors,
            'warnings': self.warnings
        }
    
    def validate_specific_instance(self, instance_name: str) -> bool:
        """Validate configuration for a specific Freshdesk instance"""
        freshdesk_config = self.config_data.get('freshdesk', {})
        instances = freshdesk_config.get('instances', [])
        
        for instance in instances:
            if instance.get('name') == instance_name:
                self._validate_freshdesk_instance(instance, 0)
                return len(self.errors) == 0
        
        self.errors.append(f"Freshdesk instance '{instance_name}' not found")
        return False 