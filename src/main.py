#!/usr/bin/env python3
"""
Freshdesk to Jira ITSM Migration Tool
Main entry point for the migration process
"""

import click
import sys
import os
from pathlib import Path
from typing import Dict, Any

# Add src to path for imports
sys.path.append(str(Path(__file__).parent))

from core.migration_manager import MigrationManager
from core.config_manager import ConfigManager
from utils.logger import setup_logger
from utils.validator import ConfigValidator


@click.group()
@click.version_option(version="1.0.0")
def cli():
    """Freshdesk to Jira ITSM Migration Tool"""
    pass


@cli.command()
@click.option('--config', '-c', default='config/migration_config.yaml', 
              help='Path to configuration file')
@click.option('--dry-run', is_flag=True, help='Run migration in dry-run mode')
@click.option('--instance', '-i', help='Specific Freshdesk instance to migrate')
def migrate(config: str, dry_run: bool, instance: str):
    """Execute the migration from Freshdesk to Jira"""
    try:
        # Load and validate configuration
        config_manager = ConfigManager(config)
        config_data = config_manager.load_config()
        
        # Setup logging
        logger = setup_logger(config_data.get('logging', {}))
        logger.info("Starting Freshdesk to Jira migration")
        
        # Validate configuration
        validator = ConfigValidator(config_data)
        if not validator.validate():
            logger.error("Configuration validation failed")
            sys.exit(1)
        
        # Override dry-run if specified
        if dry_run:
            config_data['migration']['dry_run'] = True
            logger.info("Running in dry-run mode")
        
        # Initialize migration manager
        migration_manager = MigrationManager(config_data, instance)
        
        # Execute migration
        success = migration_manager.execute_migration()
        
        if success:
            logger.info("Migration completed successfully")
        else:
            logger.error("Migration failed")
            sys.exit(1)
            
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


@cli.command()
@click.option('--config', '-c', default='config/migration_config.yaml', 
              help='Path to configuration file')
def validate(config: str):
    """Validate configuration and connectivity"""
    try:
        config_manager = ConfigManager(config)
        config_data = config_manager.load_config()
        
        logger = setup_logger(config_data.get('logging', {}))
        logger.info("Validating configuration and connectivity")
        
        validator = ConfigValidator(config_data)
        if validator.validate():
            logger.info("Configuration validation passed")
            if validator.test_connectivity():
                logger.info("Connectivity test passed")
            else:
                logger.error("Connectivity test failed")
                sys.exit(1)
        else:
            logger.error("Configuration validation failed")
            sys.exit(1)
            
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


@cli.command()
@click.option('--config', '-c', default='config/migration_config.yaml', 
              help='Path to configuration file')
@click.option('--instance', '-i', help='Specific Freshdesk instance to analyze')
@click.option('--limit', '-l', default=100, help='Number of tickets to analyze')
def analyze(config: str, instance: str, limit: int):
    """Analyze Freshdesk data structure and provide insights"""
    try:
        config_manager = ConfigManager(config)
        config_data = config_manager.load_config()
        
        logger = setup_logger(config_data.get('logging', {}))
        logger.info("Analyzing Freshdesk data structure")
        
        from core.analyzer import DataAnalyzer
        analyzer = DataAnalyzer(config_data, instance)
        analyzer.analyze_data(limit)
        
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


@cli.command()
@click.option('--config', '-c', default='config/migration_config.yaml', 
              help='Path to configuration file')
def status(config: str):
    """Show migration status and progress"""
    try:
        config_manager = ConfigManager(config)
        config_data = config_manager.load_config()
        
        from core.status_manager import StatusManager
        status_manager = StatusManager(config_data)
        status_manager.show_status()
        
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


@cli.command()
@click.option('--config', '-c', default='config/migration_config.yaml', 
              help='Path to configuration file')
def resume(config: str):
    """Resume migration from last checkpoint"""
    try:
        config_manager = ConfigManager(config)
        config_data = config_manager.load_config()
        
        logger = setup_logger(config_data.get('logging', {}))
        logger.info("Resuming migration from checkpoint")
        
        migration_manager = MigrationManager(config_data)
        success = migration_manager.resume_migration()
        
        if success:
            logger.info("Migration resumed successfully")
        else:
            logger.error("Failed to resume migration")
            sys.exit(1)
            
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    cli() 