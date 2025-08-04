"""
Migration Manager for Freshdesk to Jira Migration
Orchestrates the entire migration process
"""

import time
import json
from typing import Dict, List, Any, Optional
from datetime import datetime
from pathlib import Path
from tqdm import tqdm
from loguru import logger

from adapters.freshdesk_adapter import FreshdeskAdapter
from adapters.jira_adapter import JiraAdapter
from mappers.data_mapper import DataMapper
from core.status_manager import StatusManager
from utils.validator import ConfigValidator


class MigrationManager:
    def __init__(self, config_data: Dict[str, Any], specific_instance: str = None):
        self.config_data = config_data
        self.specific_instance = specific_instance
        
        # Initialize components
        self.freshdesk_adapters = {}
        self.jira_adapter = None
        self.data_mapper = None
        self.status_manager = None
        
        # Migration state
        self.migration_state = {
            'start_time': None,
            'end_time': None,
            'total_tickets': 0,
            'migrated_tickets': 0,
            'failed_tickets': 0,
            'current_instance': None,
            'current_batch': 0,
            'errors': []
        }
        
        self._initialize_components()
    
    def _initialize_components(self):
        """Initialize all migration components"""
        try:
            # Initialize Freshdesk adapters
            freshdesk_config = self.config_data['freshdesk']
            instances = freshdesk_config['instances']
            
            for instance_config in instances:
                if self.specific_instance and instance_config['name'] != self.specific_instance:
                    continue
                
                adapter = FreshdeskAdapter(instance_config)
                self.freshdesk_adapters[instance_config['name']] = adapter
            
            # Initialize Jira adapter
            jira_config = self.config_data['jira']
            self.jira_adapter = JiraAdapter(jira_config)
            
            # Initialize data mapper
            field_mapping = self.config_data.get('field_mapping', {})
            self.data_mapper = DataMapper(field_mapping)
            
            # Initialize status manager
            self.status_manager = StatusManager(self.config_data)
            
            logger.info("Migration components initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize migration components: {e}")
            raise
    
    def execute_migration(self) -> bool:
        """Execute the complete migration process"""
        try:
            self.migration_state['start_time'] = datetime.now()
            logger.info("Starting Freshdesk to Jira migration")
            
            # Validate configuration and connectivity
            if not self._validate_setup():
                return False
            
            # Load migration state if resuming
            if self.config_data['migration']['resume_from_checkpoint']:
                self._load_migration_state()
            
            # Execute migration for each instance
            for instance_name, freshdesk_adapter in self.freshdesk_adapters.items():
                if not self._migrate_instance(instance_name, freshdesk_adapter):
                    if not self.config_data['migration']['continue_on_error']:
                        return False
            
            self.migration_state['end_time'] = datetime.now()
            self._save_migration_state()
            
            # Generate migration report
            self._generate_migration_report()
            
            logger.info("Migration completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Migration failed: {e}")
            self.migration_state['errors'].append(str(e))
            self._save_migration_state()
            return False
    
    def _validate_setup(self) -> bool:
        """Validate configuration and test connectivity"""
        try:
            # Validate configuration
            validator = ConfigValidator(self.config_data)
            if not validator.validate():
                logger.error("Configuration validation failed")
                return False
            
            # Test Freshdesk connectivity
            for instance_name, adapter in self.freshdesk_adapters.items():
                if not adapter.test_connection():
                    logger.error(f"Failed to connect to Freshdesk instance: {instance_name}")
                    return False
                logger.info(f"Connected to Freshdesk instance: {instance_name}")
            
            # Test Jira connectivity
            if not self.jira_adapter.test_connection():
                logger.error("Failed to connect to Jira instance")
                return False
            logger.info("Connected to Jira instance")
            
            # Validate field mapping
            if not self.data_mapper.validate_mapping_config():
                logger.warning("Field mapping validation failed")
            
            return True
            
        except Exception as e:
            logger.error(f"Setup validation failed: {e}")
            return False
    
    def _migrate_instance(self, instance_name: str, freshdesk_adapter: FreshdeskAdapter) -> bool:
        """Migrate data from a specific Freshdesk instance"""
        try:
            logger.info(f"Starting migration for instance: {instance_name}")
            self.migration_state['current_instance'] = instance_name
            
            # Get migration configuration
            migration_config = self.config_data['migration']
            filters = migration_config.get('ticket_filters', {})
            
            # Get total ticket count
            total_tickets = freshdesk_adapter.get_ticket_count(filters)
            logger.info(f"Total tickets to migrate: {total_tickets}")
            
            if total_tickets == 0:
                logger.info(f"No tickets found for instance {instance_name}")
                return True
            
            # Migrate users if enabled
            if migration_config.get('migrate_users', True):
                self._migrate_users(instance_name, freshdesk_adapter)
            
            # Migrate tickets
            if migration_config.get('migrate_tickets', True):
                if not self._migrate_tickets(instance_name, freshdesk_adapter, filters, total_tickets):
                    return False
            
            logger.info(f"Migration completed for instance: {instance_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to migrate instance {instance_name}: {e}")
            self.migration_state['errors'].append(f"Instance {instance_name}: {str(e)}")
            return False
    
    def _migrate_users(self, instance_name: str, freshdesk_adapter: FreshdeskAdapter):
        """Migrate users from Freshdesk to Jira"""
        try:
            logger.info(f"Migrating users for instance: {instance_name}")
            
            users = freshdesk_adapter.get_users()
            logger.info(f"Found {len(users)} users to migrate")
            
            migrated_users = 0
            for user in tqdm(users, desc="Migrating users"):
                try:
                    jira_user_data = self.data_mapper.map_user_to_jira(user)
                    
                    if jira_user_data:
                        # Check if user already exists
                        existing_user = self.jira_adapter.get_user_by_email(jira_user_data['email'])
                        
                        if not existing_user:
                            # Create user in Jira
                            if not self.config_data['migration']['dry_run']:
                                created_user = self.jira_adapter.create_user(jira_user_data)
                                if created_user:
                                    # Update user mapping cache
                                    self.data_mapper.update_user_mapping_cache(
                                        str(user['id']), 
                                        created_user.get('accountId', '')
                                    )
                                    migrated_users += 1
                        else:
                            # User exists, update mapping cache
                            self.data_mapper.update_user_mapping_cache(
                                str(user['id']), 
                                existing_user.get('accountId', '')
                            )
                            migrated_users += 1
                
                except Exception as e:
                    logger.error(f"Failed to migrate user {user.get('id')}: {e}")
                    continue
            
            logger.info(f"Successfully migrated {migrated_users} users")
            
        except Exception as e:
            logger.error(f"Failed to migrate users: {e}")
    
    def _migrate_tickets(self, instance_name: str, freshdesk_adapter: FreshdeskAdapter, 
                        filters: Dict[str, Any], total_tickets: int) -> bool:
        """Migrate tickets from Freshdesk to Jira"""
        try:
            logger.info(f"Migrating tickets for instance: {instance_name}")
            
            migration_config = self.config_data['migration']
            batch_size = self.config_data['freshdesk']['instances'][0]['batch_size']
            checkpoint_interval = migration_config['checkpoint_interval']
            
            # Create progress bar
            pbar = tqdm(total=total_tickets, desc=f"Migrating tickets from {instance_name}")
            
            batch_count = 0
            migrated_count = 0
            failed_count = 0
            
            # Process tickets in batches
            for tickets_batch in freshdesk_adapter.get_all_tickets(filters):
                batch_count += 1
                self.migration_state['current_batch'] = batch_count
                
                for ticket in tickets_batch:
                    try:
                        if self._migrate_single_ticket(ticket, instance_name, freshdesk_adapter):
                            migrated_count += 1
                            self.migration_state['migrated_tickets'] += 1
                        else:
                            failed_count += 1
                            self.migration_state['failed_tickets'] += 1
                        
                        pbar.update(1)
                        
                    except Exception as e:
                        logger.error(f"Failed to migrate ticket {ticket.get('id')}: {e}")
                        failed_count += 1
                        self.migration_state['failed_tickets'] += 1
                        pbar.update(1)
                        
                        if not migration_config['continue_on_error']:
                            pbar.close()
                            return False
                
                # Save checkpoint
                if batch_count % checkpoint_interval == 0:
                    self._save_migration_state()
                    logger.info(f"Checkpoint saved at batch {batch_count}")
            
            pbar.close()
            
            logger.info(f"Ticket migration completed: {migrated_count} migrated, {failed_count} failed")
            return True
            
        except Exception as e:
            logger.error(f"Failed to migrate tickets: {e}")
            return False
    
    def _migrate_single_ticket(self, ticket: Dict[str, Any], instance_name: str, 
                              freshdesk_adapter: FreshdeskAdapter) -> bool:
        """Migrate a single ticket with all its data"""
        try:
            # Map ticket to Jira format
            issue_data = self.data_mapper.map_ticket_to_issue(ticket)
            
            if not issue_data:
                logger.warning(f"Failed to map ticket {ticket.get('id')}")
                return False
            
            # Create issue in Jira
            if not self.config_data['migration']['dry_run']:
                created_issue = self.jira_adapter.create_issue(issue_data)
                
                if not created_issue:
                    logger.error(f"Failed to create issue for ticket {ticket.get('id')}")
                    return False
                
                issue_key = created_issue.get('key')
                
                # Migrate comments
                if self.config_data['migration'].get('migrate_comments', True):
                    self._migrate_ticket_comments(ticket['id'], issue_key, freshdesk_adapter)
                
                # Migrate attachments
                if self.config_data['migration'].get('migrate_attachments', True):
                    self._migrate_ticket_attachments(ticket['id'], issue_key, freshdesk_adapter)
                
                # Update status if needed
                self._update_issue_status(issue_key, ticket)
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to migrate ticket {ticket.get('id')}: {e}")
            return False
    
    def _migrate_ticket_comments(self, ticket_id: int, issue_key: str, 
                                freshdesk_adapter: FreshdeskAdapter):
        """Migrate comments for a specific ticket"""
        try:
            comments = freshdesk_adapter.get_ticket_comments(ticket_id)
            
            for comment in comments:
                try:
                    jira_comment = self.data_mapper.map_comment_to_jira(comment)
                    
                    if jira_comment and not self.config_data['migration']['dry_run']:
                        self.jira_adapter.add_comment(
                            issue_key, 
                            jira_comment['body'], 
                            jira_comment.get('author')
                        )
                
                except Exception as e:
                    logger.error(f"Failed to migrate comment {comment.get('id')}: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"Failed to migrate comments for ticket {ticket_id}: {e}")
    
    def _migrate_ticket_attachments(self, ticket_id: int, issue_key: str, 
                                   freshdesk_adapter: FreshdeskAdapter):
        """Migrate attachments for a specific ticket"""
        try:
            attachments = freshdesk_adapter.get_ticket_attachments(ticket_id)
            attachment_config = self.config_data['migration'].get('attachments', {})
            
            for attachment in attachments:
                try:
                    # Check file size limit
                    file_size_mb = attachment.get('size', 0) / (1024 * 1024)
                    if file_size_mb > attachment_config.get('max_file_size_mb', 50):
                        logger.warning(f"Attachment {attachment.get('name')} exceeds size limit")
                        continue
                    
                    # Check file extension
                    file_extension = Path(attachment.get('name', '')).suffix.lower()
                    allowed_extensions = attachment_config.get('allowed_extensions', [])
                    if allowed_extensions and file_extension not in allowed_extensions:
                        logger.warning(f"Attachment {attachment.get('name')} has disallowed extension")
                        continue
                    
                    # Download attachment
                    download_path = Path(attachment_config.get('download_path', './data/attachments'))
                    download_path = download_path / f"{ticket_id}_{attachment.get('id')}_{attachment.get('name')}"
                    
                    if freshdesk_adapter.download_attachment(attachment['attachment_url'], str(download_path)):
                        # Upload to Jira
                        if not self.config_data['migration']['dry_run']:
                            self.jira_adapter.add_attachment(issue_key, str(download_path))
                        
                        # Clean up downloaded file
                        if download_path.exists():
                            download_path.unlink()
                
                except Exception as e:
                    logger.error(f"Failed to migrate attachment {attachment.get('id')}: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"Failed to migrate attachments for ticket {ticket_id}: {e}")
    
    def _update_issue_status(self, issue_key: str, ticket: Dict[str, Any]):
        """Update issue status based on ticket status"""
        try:
            freshdesk_status = ticket.get('status', '').lower()
            jira_status = self.data_mapper.status_mapping.get(freshdesk_status)
            
            if jira_status and jira_status != 'To Do':
                # Get available transitions
                transitions = self.jira_adapter.get_issue_transitions(issue_key)
                
                # Find transition to target status
                target_transition = None
                for transition in transitions:
                    if transition.get('to', {}).get('name') == jira_status:
                        target_transition = transition
                        break
                
                if target_transition:
                    self.jira_adapter.transition_issue(
                        issue_key, 
                        target_transition['id'],
                        f"Status updated during migration from Freshdesk"
                    )
                    
        except Exception as e:
            logger.error(f"Failed to update status for issue {issue_key}: {e}")
    
    def _save_migration_state(self):
        """Save current migration state"""
        try:
            state_file = Path('./data/migration_state.json')
            state_file.parent.mkdir(parents=True, exist_ok=True)
            
            with open(state_file, 'w') as f:
                json.dump(self.migration_state, f, indent=2, default=str)
                
        except Exception as e:
            logger.error(f"Failed to save migration state: {e}")
    
    def _load_migration_state(self):
        """Load migration state from file"""
        try:
            state_file = Path('./data/migration_state.json')
            
            if state_file.exists():
                with open(state_file, 'r') as f:
                    saved_state = json.load(f)
                
                # Update current state
                self.migration_state.update(saved_state)
                logger.info("Migration state loaded from checkpoint")
                
        except Exception as e:
            logger.error(f"Failed to load migration state: {e}")
    
    def _generate_migration_report(self):
        """Generate migration completion report"""
        try:
            report = {
                'migration_summary': {
                    'start_time': self.migration_state['start_time'],
                    'end_time': self.migration_state['end_time'],
                    'duration': str(self.migration_state['end_time'] - self.migration_state['start_time']),
                    'total_tickets': self.migration_state['total_tickets'],
                    'migrated_tickets': self.migration_state['migrated_tickets'],
                    'failed_tickets': self.migration_state['failed_tickets'],
                    'success_rate': f"{(self.migration_state['migrated_tickets'] / max(self.migration_state['total_tickets'], 1)) * 100:.2f}%"
                },
                'instances_migrated': list(self.freshdesk_adapters.keys()),
                'errors': self.migration_state['errors'],
                'mapping_summary': self.data_mapper.get_mapping_summary()
            }
            
            # Save report
            report_file = Path('./data/migration_report.json')
            report_file.parent.mkdir(parents=True, exist_ok=True)
            
            with open(report_file, 'w') as f:
                json.dump(report, f, indent=2, default=str)
            
            logger.info("Migration report generated successfully")
            
        except Exception as e:
            logger.error(f"Failed to generate migration report: {e}")
    
    def resume_migration(self) -> bool:
        """Resume migration from last checkpoint"""
        try:
            logger.info("Resuming migration from checkpoint")
            
            # Load migration state
            self._load_migration_state()
            
            # Continue migration
            return self.execute_migration()
            
        except Exception as e:
            logger.error(f"Failed to resume migration: {e}")
            return False
    
    def get_migration_status(self) -> Dict[str, Any]:
        """Get current migration status"""
        return self.migration_state.copy() 