"""
Logger utility for Freshdesk to Jira Migration
Provides consistent logging configuration
"""

import sys
from pathlib import Path
from loguru import logger
from typing import Dict, Any


def setup_logger(config: Dict[str, Any]):
    """Setup logger with configuration"""
    try:
        # Remove default logger
        logger.remove()
        
        # Get configuration
        log_level = config.get('level', 'INFO')
        log_file = config.get('file', './logs/migration.log')
        max_file_size = config.get('max_file_size', '100MB')
        backup_count = config.get('backup_count', 5)
        log_format = config.get('format', 
                               '{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}')
        
        # Create logs directory
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Add console handler
        logger.add(
            sys.stdout,
            format=log_format,
            level=log_level,
            colorize=True
        )
        
        # Add file handler
        logger.add(
            log_file,
            format=log_format,
            level=log_level,
            rotation=max_file_size,
            retention=backup_count,
            compression="zip"
        )
        
        logger.info("Logger configured successfully")
        return logger
        
    except Exception as e:
        print(f"Failed to setup logger: {e}")
        # Fallback to basic logging
        logger.add(sys.stdout, format="{time} | {level} | {message}")
        return logger


def get_logger():
    """Get the configured logger instance"""
    return logger 