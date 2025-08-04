"""
Configuration Manager for Freshdesk to Jira Migration
Handles loading, validation, and management of configuration files
"""

import yaml
import os
from pathlib import Path
from typing import Dict, Any, Optional
from pydantic import BaseModel, ValidationError
import json


class FreshdeskInstanceConfig(BaseModel):
    name: str
    url: str
    api_key: str
    headers: Optional[Dict[str, str]] = {}
    rate_limit: int = 100
    timeout: int = 30
    batch_size: int = 100


class JiraConfig(BaseModel):
    url: str
    username: str
    api_token: str
    project_key: str
    issue_type: str = "Incident"
    rate_limit: int = 100
    timeout: int = 30
    batch_size: int = 50


class MigrationConfig(BaseModel):
    dry_run: bool = False
    resume_from_checkpoint: bool = True
    checkpoint_interval: int = 1000
    migrate_tickets: bool = True
    migrate_comments: bool = True
    migrate_attachments: bool = True
    migrate_users: bool = True
    migrate_custom_fields: bool = True
    max_retries: int = 3
    retry_delay: int = 5
    continue_on_error: bool = True


class ConfigManager:
    def __init__(self, config_path: str):
        self.config_path = Path(config_path)
        self.config_data: Optional[Dict[str, Any]] = None
        
    def load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file"""
        if not self.config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")
        
        try:
            with open(self.config_path, 'r', encoding='utf-8') as file:
                self.config_data = yaml.safe_load(file)
            
            # Validate configuration structure
            self._validate_config_structure()
            
            return self.config_data
            
        except yaml.YAMLError as e:
            raise ValueError(f"Invalid YAML configuration: {e}")
        except Exception as e:
            raise Exception(f"Error loading configuration: {e}")
    
    def _validate_config_structure(self):
        """Validate basic configuration structure"""
        required_sections = ['freshdesk', 'jira', 'migration']
        
        for section in required_sections:
            if section not in self.config_data:
                raise ValueError(f"Missing required configuration section: {section}")
        
        # Validate Freshdesk instances
        if 'instances' not in self.config_data['freshdesk']:
            raise ValueError("Freshdesk configuration must contain 'instances' section")
        
        if not self.config_data['freshdesk']['instances']:
            raise ValueError("At least one Freshdesk instance must be configured")
    
    def get_freshdesk_instances(self) -> Dict[str, FreshdeskInstanceConfig]:
        """Get Freshdesk instances configuration"""
        instances = {}
        for instance_config in self.config_data['freshdesk']['instances']:
            try:
                instance = FreshdeskInstanceConfig(**instance_config)
                instances[instance.name] = instance
            except ValidationError as e:
                raise ValueError(f"Invalid Freshdesk instance configuration: {e}")
        
        return instances
    
    def get_jira_config(self) -> JiraConfig:
        """Get Jira configuration"""
        try:
            return JiraConfig(**self.config_data['jira'])
        except ValidationError as e:
            raise ValueError(f"Invalid Jira configuration: {e}")
    
    def get_migration_config(self) -> MigrationConfig:
        """Get migration configuration"""
        try:
            return MigrationConfig(**self.config_data['migration'])
        except ValidationError as e:
            raise ValueError(f"Invalid migration configuration: {e}")
    
    def get_field_mapping(self) -> Dict[str, Any]:
        """Get field mapping configuration"""
        return self.config_data.get('field_mapping', {})
    
    def get_logging_config(self) -> Dict[str, Any]:
        """Get logging configuration"""
        return self.config_data.get('logging', {})
    
    def get_performance_config(self) -> Dict[str, Any]:
        """Get performance configuration"""
        return self.config_data.get('performance', {})
    
    def update_config(self, updates: Dict[str, Any]):
        """Update configuration with new values"""
        if self.config_data is None:
            self.load_config()
        
        self._update_nested_dict(self.config_data, updates)
    
    def _update_nested_dict(self, base_dict: Dict[str, Any], updates: Dict[str, Any]):
        """Recursively update nested dictionary"""
        for key, value in updates.items():
            if key in base_dict and isinstance(base_dict[key], dict) and isinstance(value, dict):
                self._update_nested_dict(base_dict[key], value)
            else:
                base_dict[key] = value
    
    def save_config(self, output_path: Optional[str] = None):
        """Save current configuration to file"""
        if self.config_data is None:
            raise ValueError("No configuration loaded")
        
        save_path = Path(output_path) if output_path else self.config_path
        
        try:
            with open(save_path, 'w', encoding='utf-8') as file:
                yaml.dump(self.config_data, file, default_flow_style=False, indent=2)
        except Exception as e:
            raise Exception(f"Error saving configuration: {e}")
    
    def create_backup(self):
        """Create backup of current configuration"""
        if self.config_data is None:
            self.load_config()
        
        backup_path = self.config_path.with_suffix('.yaml.backup')
        self.save_config(str(backup_path))
        return backup_path
    
    def validate_instance_config(self, instance_name: str) -> bool:
        """Validate specific instance configuration"""
        instances = self.get_freshdesk_instances()
        
        if instance_name not in instances:
            return False
        
        instance = instances[instance_name]
        
        # Basic validation
        if not instance.url or not instance.api_key:
            return False
        
        # URL format validation
        if not instance.url.startswith(('http://', 'https://')):
            return False
        
        return True
    
    def get_instance_by_name(self, instance_name: str) -> Optional[FreshdeskInstanceConfig]:
        """Get specific Freshdesk instance configuration"""
        instances = self.get_freshdesk_instances()
        return instances.get(instance_name)
    
    def list_instances(self) -> list:
        """List all configured Freshdesk instances"""
        instances = self.get_freshdesk_instances()
        return list(instances.keys()) 