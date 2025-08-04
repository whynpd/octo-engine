#!/usr/bin/env python3
"""
Setup script for Freshdesk to Jira Migration Tool
Helps configure the migration settings
"""

import sys
import os
from pathlib import Path
import yaml
import getpass

# Add src to path
sys.path.append(str(Path(__file__).parent.parent / 'src'))

from core.config_manager import ConfigManager


def setup_migration():
    """Interactive setup for migration configuration"""
    print("="*60)
    print("FRESHDESK TO JIRA MIGRATION TOOL SETUP")
    print("="*60)
    
    config = {
        'freshdesk': {
            'instances': []
        },
        'jira': {},
        'migration': {
            'dry_run': True,
            'resume_from_checkpoint': True,
            'checkpoint_interval': 1000,
            'migrate_tickets': True,
            'migrate_comments': True,
            'migrate_attachments': True,
            'migrate_users': True,
            'migrate_custom_fields': True,
            'max_retries': 3,
            'retry_delay': 5,
            'continue_on_error': True,
            'ticket_filters': {
                'statuses': ['open', 'pending', 'resolved', 'closed'],
                'priorities': ['low', 'medium', 'high', 'urgent']
            },
            'attachments': {
                'download_path': './data/attachments',
                'max_file_size_mb': 50,
                'allowed_extensions': ['.pdf', '.doc', '.docx', '.txt', '.jpg', '.png', '.gif']
            }
        },
        'field_mapping': {
            'priority': {
                'low': 'Low',
                'medium': 'Medium',
                'high': 'High',
                'urgent': 'Highest'
            },
            'status': {
                'open': 'To Do',
                'pending': 'In Progress',
                'resolved': 'Done',
                'closed': 'Done'
            },
            'custom_fields': {},
            'defaults': {
                'priority': 'Medium',
                'status': 'To Do',
                'assignee': 'admin'
            }
        },
        'logging': {
            'level': 'INFO',
            'file': './logs/migration.log',
            'max_file_size': '100MB',
            'backup_count': 5,
            'format': '{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}'
        },
        'performance': {
            'max_concurrent_requests': 10,
            'max_memory_usage_mb': 2048,
            'connection_pool_size': 20
        }
    }
    
    # Setup Freshdesk instances
    print("\n1. FRESHDESK CONFIGURATION")
    print("-" * 30)
    
    while True:
        instance = {}
        
        instance['name'] = input("Enter Freshdesk instance name: ").strip()
        if not instance['name']:
            break
        
        instance['url'] = input("Enter Freshdesk URL (e.g., https://company.freshdesk.com): ").strip()
        instance['api_key'] = getpass.getpass("Enter Freshdesk API key: ").strip()
        
        # Optional settings
        rate_limit = input("Enter rate limit (requests per minute, default 100): ").strip()
        instance['rate_limit'] = int(rate_limit) if rate_limit else 100
        
        timeout = input("Enter timeout in seconds (default 30): ").strip()
        instance['timeout'] = int(timeout) if timeout else 30
        
        batch_size = input("Enter batch size (default 100): ").strip()
        instance['batch_size'] = int(batch_size) if batch_size else 100
        
        config['freshdesk']['instances'].append(instance)
        
        add_another = input("\nAdd another Freshdesk instance? (y/n): ").strip().lower()
        if add_another != 'y':
            break
    
    # Setup Jira configuration
    print("\n2. JIRA CONFIGURATION")
    print("-" * 30)
    
    config['jira']['url'] = input("Enter Jira URL (e.g., https://company.atlassian.net): ").strip()
    config['jira']['username'] = input("Enter Jira username/email: ").strip()
    config['jira']['api_token'] = getpass.getpass("Enter Jira API token: ").strip()
    config['jira']['project_key'] = input("Enter Jira project key (e.g., ITSM): ").strip().upper()
    
    issue_type = input("Enter issue type (default Incident): ").strip()
    config['jira']['issue_type'] = issue_type if issue_type else 'Incident'
    
    # Optional Jira settings
    rate_limit = input("Enter Jira rate limit (requests per minute, default 100): ").strip()
    config['jira']['rate_limit'] = int(rate_limit) if rate_limit else 100
    
    timeout = input("Enter Jira timeout in seconds (default 30): ").strip()
    config['jira']['timeout'] = int(timeout) if timeout else 30
    
    batch_size = input("Enter Jira batch size (default 50): ").strip()
    config['jira']['batch_size'] = int(batch_size) if batch_size else 50
    
    # Migration settings
    print("\n3. MIGRATION SETTINGS")
    print("-" * 30)
    
    dry_run = input("Run in dry-run mode? (y/n, default y): ").strip().lower()
    config['migration']['dry_run'] = dry_run != 'n'
    
    migrate_users = input("Migrate users? (y/n, default y): ").strip().lower()
    config['migration']['migrate_users'] = migrate_users != 'n'
    
    migrate_comments = input("Migrate comments? (y/n, default y): ").strip().lower()
    config['migration']['migrate_comments'] = migrate_comments != 'n'
    
    migrate_attachments = input("Migrate attachments? (y/n, default y): ").strip().lower()
    config['migration']['migrate_attachments'] = migrate_attachments != 'n'
    
    checkpoint_interval = input("Enter checkpoint interval (default 1000): ").strip()
    config['migration']['checkpoint_interval'] = int(checkpoint_interval) if checkpoint_interval else 1000
    
    # Field mapping
    print("\n4. FIELD MAPPING")
    print("-" * 30)
    print("Configure custom field mappings (press Enter to skip):")
    
    while True:
        freshdesk_field = input("Enter Freshdesk field name: ").strip()
        if not freshdesk_field:
            break
        
        jira_field = input("Enter corresponding Jira field ID: ").strip()
        if jira_field:
            config['field_mapping']['custom_fields'][freshdesk_field] = jira_field
    
    # Save configuration
    print("\n5. SAVE CONFIGURATION")
    print("-" * 30)
    
    config_path = input("Enter configuration file path (default config/migration_config.yaml): ").strip()
    if not config_path:
        config_path = "config/migration_config.yaml"
    
    # Create directory if it doesn't exist
    config_file = Path(config_path)
    config_file.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        with open(config_file, 'w') as f:
            yaml.dump(config, f, default_flow_style=False, indent=2)
        
        print(f"\n✓ Configuration saved to {config_file}")
        print("\nNext steps:")
        print("1. Review the configuration file")
        print("2. Run: python src/main.py validate")
        print("3. Run: python src/main.py analyze")
        print("4. Run: python src/main.py migrate --dry-run")
        print("5. Run: python src/main.py migrate")
        
    except Exception as e:
        print(f"✗ Failed to save configuration: {e}")
        return False
    
    return True


if __name__ == '__main__':
    try:
        setup_migration()
    except KeyboardInterrupt:
        print("\n\nSetup cancelled by user.")
    except Exception as e:
        print(f"\nSetup failed: {e}") 