#!/usr/bin/env python3
"""
Storage Configuration
====================
Configuration for different storage backends (local, S3, server)
"""

import os
from pathlib import Path
from typing import Optional

class StorageConfig:
    """Configuration for where attachments are stored and how URLs are generated"""
    
    def __init__(self):
        # Storage type from environment or default to local
        self.storage_type = os.getenv("ATTACHMENT_STORAGE_TYPE", "local").lower()
        
        # Local storage configuration
        self.local_base_path = Path(os.getenv("ATTACHMENT_LOCAL_PATH", "attachments"))
        
        # S3 configuration
        self.s3_bucket = os.getenv("ATTACHMENT_S3_BUCKET", "")
        self.s3_region = os.getenv("ATTACHMENT_S3_REGION", "us-east-1")
        self.s3_base_url = os.getenv("ATTACHMENT_S3_BASE_URL", "")  # Custom S3 endpoint
        
        # Server/CDN configuration
        self.server_base_url = os.getenv("ATTACHMENT_SERVER_BASE_URL", "")
        
        # CloudFront or CDN URL (if using)
        self.cdn_base_url = os.getenv("ATTACHMENT_CDN_BASE_URL", "")
    
    def get_storage_url(self, local_file_path: str, ticket_id: int, filename: str) -> str:
        """
        Convert a local file path to the appropriate storage URL based on configuration
        
        Args:
            local_file_path: The local path where file was saved
            ticket_id: Ticket ID for organizing files
            filename: Original filename
            
        Returns:
            The URL where the file can be accessed
        """
        if self.storage_type == "s3":
            return self._get_s3_url(ticket_id, filename)
        elif self.storage_type == "server":
            return self._get_server_url(ticket_id, filename)
        elif self.storage_type == "local":
            return self._get_local_url(local_file_path)
        else:
            # Fallback to local path
            return local_file_path
    
    def _get_s3_url(self, ticket_id: int, filename: str) -> str:
        """Generate S3 URL for the file"""
        if self.s3_base_url:
            # Custom S3 endpoint
            base_url = self.s3_base_url.rstrip('/')
        else:
            # Standard S3 URL
            base_url = f"https://{self.s3_bucket}.s3.{self.s3_region}.amazonaws.com"
        
        # Use CDN URL if available
        if self.cdn_base_url:
            base_url = self.cdn_base_url.rstrip('/')
        
        return f"{base_url}/attachments/{ticket_id}/{filename}"
    
    def _get_server_url(self, ticket_id: int, filename: str) -> str:
        """Generate server URL for the file"""
        base_url = self.server_base_url.rstrip('/')
        return f"{base_url}/attachments/{ticket_id}/{filename}"
    
    def _get_local_url(self, local_file_path: str) -> str:
        """Generate local file URL"""
        # Convert to absolute path for consistency
        abs_path = Path(local_file_path).resolve()
        return f"file://{abs_path}"
    
    def get_upload_path(self, ticket_id: int, filename: str) -> str:
        """
        Get the path where file should be uploaded for the configured storage type
        
        Args:
            ticket_id: Ticket ID
            filename: Filename
            
        Returns:
            Path/key for upload
        """
        if self.storage_type in ["s3", "server"]:
            return f"attachments/{ticket_id}/{filename}"
        else:
            # Local storage
            return str(self.local_base_path / str(ticket_id) / filename)


# Global storage configuration instance
storage_config = StorageConfig()

def get_attachment_storage_url(local_file_path: str, ticket_id: int, filename: str) -> str:
    """
    Convenience function to get the storage URL for an attachment
    
    Args:
        local_file_path: Local path where file was saved
        ticket_id: Ticket ID
        filename: Original filename
        
    Returns:
        URL where the file can be accessed
    """
    return storage_config.get_storage_url(local_file_path, ticket_id, filename)

def get_storage_type() -> str:
    """Get the current storage type"""
    return storage_config.storage_type

def print_storage_config():
    """Print current storage configuration for debugging"""
    print("📁 Storage Configuration:")
    print(f"   Type: {storage_config.storage_type}")
    
    if storage_config.storage_type == "local":
        print(f"   Local Path: {storage_config.local_base_path}")
    elif storage_config.storage_type == "s3":
        print(f"   S3 Bucket: {storage_config.s3_bucket}")
        print(f"   S3 Region: {storage_config.s3_region}")
        if storage_config.cdn_base_url:
            print(f"   CDN URL: {storage_config.cdn_base_url}")
    elif storage_config.storage_type == "server":
        print(f"   Server URL: {storage_config.server_base_url}")


if __name__ == "__main__":
    # Example usage and testing
    print("🔧 Storage Configuration Test")
    print("=" * 40)
    
    print_storage_config()
    
    # Test URL generation
    test_ticket_id = 26172
    test_filename = "image.png"
    test_local_path = f"/Users/user/attachments/{test_ticket_id}/{test_filename}"
    
    print(f"\n🧪 Test URL generation:")
    print(f"   Ticket ID: {test_ticket_id}")
    print(f"   Filename: {test_filename}")
    print(f"   Local Path: {test_local_path}")
    
    storage_url = get_attachment_storage_url(test_local_path, test_ticket_id, test_filename)
    print(f"   Storage URL: {storage_url}")
    
    print(f"\n💡 To configure for S3, set environment variables:")
    print(f"   export ATTACHMENT_STORAGE_TYPE=s3")
    print(f"   export ATTACHMENT_S3_BUCKET=your-bucket-name")
    print(f"   export ATTACHMENT_CDN_BASE_URL=https://cdn.youromain.com")
    
    print(f"\n💡 To configure for server, set environment variables:")
    print(f"   export ATTACHMENT_STORAGE_TYPE=server")
    print(f"   export ATTACHMENT_SERVER_BASE_URL=https://files.yourdomain.com")