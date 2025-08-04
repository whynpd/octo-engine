"""
Status Manager for Freshdesk to Jira Migration
Tracks and displays migration progress
"""

import json
import time
from typing import Dict, Any, Optional
from datetime import datetime, timedelta
from pathlib import Path
from loguru import logger


class StatusManager:
    def __init__(self, config_data: Dict[str, Any]):
        self.config_data = config_data
        self.status_file = Path('./data/migration_status.json')
        self.status_file.parent.mkdir(parents=True, exist_ok=True)
    
    def get_status(self) -> Dict[str, Any]:
        """Get current migration status"""
        try:
            if self.status_file.exists():
                with open(self.status_file, 'r') as f:
                    return json.load(f)
            else:
                return self._get_default_status()
        except Exception as e:
            logger.error(f"Failed to load status: {e}")
            return self._get_default_status()
    
    def _get_default_status(self) -> Dict[str, Any]:
        """Get default status structure"""
        return {
            'migration_id': None,
            'start_time': None,
            'end_time': None,
            'status': 'not_started',
            'current_instance': None,
            'current_batch': 0,
            'total_tickets': 0,
            'migrated_tickets': 0,
            'failed_tickets': 0,
            'success_rate': 0.0,
            'estimated_completion': None,
            'errors': [],
            'warnings': [],
            'last_update': None
        }
    
    def update_status(self, updates: Dict[str, Any]):
        """Update migration status"""
        try:
            current_status = self.get_status()
            current_status.update(updates)
            current_status['last_update'] = datetime.now().isoformat()
            
            # Calculate success rate
            if current_status['total_tickets'] > 0:
                current_status['success_rate'] = (
                    current_status['migrated_tickets'] / current_status['total_tickets']
                ) * 100
            
            # Estimate completion time
            if current_status['migrated_tickets'] > 0 and current_status['start_time']:
                start_time = datetime.fromisoformat(current_status['start_time'])
                elapsed_time = datetime.now() - start_time
                tickets_per_second = current_status['migrated_tickets'] / elapsed_time.total_seconds()
                
                remaining_tickets = current_status['total_tickets'] - current_status['migrated_tickets']
                if tickets_per_second > 0:
                    estimated_seconds = remaining_tickets / tickets_per_second
                    estimated_completion = datetime.now() + timedelta(seconds=estimated_seconds)
                    current_status['estimated_completion'] = estimated_completion.isoformat()
            
            # Save status
            with open(self.status_file, 'w') as f:
                json.dump(current_status, f, indent=2, default=str)
                
        except Exception as e:
            logger.error(f"Failed to update status: {e}")
    
    def show_status(self):
        """Display current migration status"""
        try:
            status = self.get_status()
            
            print("\n" + "="*60)
            print("FRESHDESK TO JIRA MIGRATION STATUS")
            print("="*60)
            
            # Basic information
            print(f"Status: {status['status'].upper()}")
            if status['migration_id']:
                print(f"Migration ID: {status['migration_id']}")
            
            # Timing information
            if status['start_time']:
                start_time = datetime.fromisoformat(status['start_time'])
                print(f"Started: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
            
            if status['end_time']:
                end_time = datetime.fromisoformat(status['end_time'])
                print(f"Completed: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
                
                if status['start_time']:
                    duration = end_time - start_time
                    print(f"Duration: {duration}")
            
            # Progress information
            print(f"\nProgress:")
            print(f"  Total Tickets: {status['total_tickets']:,}")
            print(f"  Migrated: {status['migrated_tickets']:,}")
            print(f"  Failed: {status['failed_tickets']:,}")
            print(f"  Success Rate: {status['success_rate']:.2f}%")
            
            # Current status
            if status['current_instance']:
                print(f"\nCurrent Instance: {status['current_instance']}")
                print(f"Current Batch: {status['current_batch']}")
            
            # Estimated completion
            if status['estimated_completion']:
                est_completion = datetime.fromisoformat(status['estimated_completion'])
                print(f"Estimated Completion: {est_completion.strftime('%Y-%m-%d %H:%M:%S')}")
            
            # Errors and warnings
            if status['errors']:
                print(f"\nErrors ({len(status['errors'])}):")
                for error in status['errors'][-5:]:  # Show last 5 errors
                    print(f"  - {error}")
            
            if status['warnings']:
                print(f"\nWarnings ({len(status['warnings'])}):")
                for warning in status['warnings'][-5:]:  # Show last 5 warnings
                    print(f"  - {warning}")
            
            # Last update
            if status['last_update']:
                last_update = datetime.fromisoformat(status['last_update'])
                print(f"\nLast Update: {last_update.strftime('%Y-%m-%d %H:%M:%S')}")
            
            print("="*60 + "\n")
            
        except Exception as e:
            logger.error(f"Failed to show status: {e}")
    
    def get_progress_percentage(self) -> float:
        """Get migration progress percentage"""
        status = self.get_status()
        
        if status['total_tickets'] > 0:
            return (status['migrated_tickets'] / status['total_tickets']) * 100
        return 0.0
    
    def is_migration_complete(self) -> bool:
        """Check if migration is complete"""
        status = self.get_status()
        return status['status'] == 'completed'
    
    def is_migration_failed(self) -> bool:
        """Check if migration has failed"""
        status = self.get_status()
        return status['status'] == 'failed'
    
    def is_migration_in_progress(self) -> bool:
        """Check if migration is in progress"""
        status = self.get_status()
        return status['status'] == 'in_progress'
    
    def get_migration_summary(self) -> Dict[str, Any]:
        """Get migration summary for reporting"""
        status = self.get_status()
        
        summary = {
            'status': status['status'],
            'total_tickets': status['total_tickets'],
            'migrated_tickets': status['migrated_tickets'],
            'failed_tickets': status['failed_tickets'],
            'success_rate': status['success_rate'],
            'duration': None,
            'errors_count': len(status['errors']),
            'warnings_count': len(status['warnings'])
        }
        
        # Calculate duration
        if status['start_time'] and status['end_time']:
            start_time = datetime.fromisoformat(status['start_time'])
            end_time = datetime.fromisoformat(status['end_time'])
            summary['duration'] = str(end_time - start_time)
        elif status['start_time']:
            start_time = datetime.fromisoformat(status['start_time'])
            summary['duration'] = str(datetime.now() - start_time)
        
        return summary
    
    def reset_status(self):
        """Reset migration status"""
        try:
            default_status = self._get_default_status()
            default_status['migration_id'] = f"migration_{int(time.time())}"
            default_status['status'] = 'not_started'
            
            with open(self.status_file, 'w') as f:
                json.dump(default_status, f, indent=2, default=str)
            
            logger.info("Migration status reset")
            
        except Exception as e:
            logger.error(f"Failed to reset status: {e}")
    
    def add_error(self, error: str):
        """Add error to status"""
        try:
            status = self.get_status()
            status['errors'].append({
                'timestamp': datetime.now().isoformat(),
                'message': error
            })
            
            # Keep only last 100 errors
            if len(status['errors']) > 100:
                status['errors'] = status['errors'][-100:]
            
            self.update_status(status)
            
        except Exception as e:
            logger.error(f"Failed to add error: {e}")
    
    def add_warning(self, warning: str):
        """Add warning to status"""
        try:
            status = self.get_status()
            status['warnings'].append({
                'timestamp': datetime.now().isoformat(),
                'message': warning
            })
            
            # Keep only last 100 warnings
            if len(status['warnings']) > 100:
                status['warnings'] = status['warnings'][-100:]
            
            self.update_status(status)
            
        except Exception as e:
            logger.error(f"Failed to add warning: {e}")
    
    def start_migration(self, total_tickets: int = 0):
        """Mark migration as started"""
        self.update_status({
            'status': 'in_progress',
            'start_time': datetime.now().isoformat(),
            'total_tickets': total_tickets,
            'migrated_tickets': 0,
            'failed_tickets': 0,
            'errors': [],
            'warnings': []
        })
    
    def complete_migration(self):
        """Mark migration as completed"""
        self.update_status({
            'status': 'completed',
            'end_time': datetime.now().isoformat()
        })
    
    def fail_migration(self, error: str = None):
        """Mark migration as failed"""
        updates = {
            'status': 'failed',
            'end_time': datetime.now().isoformat()
        }
        
        if error:
            self.add_error(error)
        
        self.update_status(updates) 