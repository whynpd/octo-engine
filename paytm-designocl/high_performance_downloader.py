#!/usr/bin/env python3
"""
High-Performance Attachment Downloader

Optimized for downloading large volumes of attachments (50+ per ticket) with:
- Aggressive parallelization (100+ concurrent downloads)
- Connection pooling and session reuse
- Batch processing and queue management
- Memory-efficient streaming
- Smart retry logic with exponential backoff
- URL pre-validation and filtering
"""

import asyncio
import aiohttp
import aiofiles
import json
import time
import logging
from pathlib import Path
from typing import List, Dict, Set, Optional
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from dataclasses import dataclass
from urllib.parse import urlparse
import os
from dotenv import load_dotenv

load_dotenv()

# Configuration for high-performance downloading
CONFIG = {
    'max_concurrent_downloads': 100,  # Concurrent downloads
    'max_connections_per_host': 20,   # Per-host connection pool
    'total_timeout': 300,             # 5 minutes per file
    'chunk_size': 64 * 1024,          # 64KB chunks
    'retry_attempts': 3,
    'retry_delay_base': 0.5,          # Exponential backoff base
    'session_timeout': aiohttp.ClientTimeout(total=300, connect=10),
    'batch_size': 1000,               # Process in batches
    'progress_report_interval': 50,   # Report every N downloads
}

@dataclass
class DownloadTask:
    ticket_id: int
    url: str
    filename: str
    filepath: Path
    source: str  # 'ticket' or 'conversation'
    size: Optional[int] = None
    content_type: Optional[str] = None

class HighPerformanceDownloader:
    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None
        self.downloaded_count = 0
        self.failed_count = 0
        self.skipped_count = 0
        self.start_time = time.time()
        self.downloaded_urls: Set[str] = set()
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s [%(levelname)s] %(message)s',
            handlers=[
                logging.FileHandler('high_perf_download.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    async def create_session(self):
        """Create optimized aiohttp session with connection pooling"""
        connector = aiohttp.TCPConnector(
            limit=CONFIG['max_concurrent_downloads'] * 2,
            limit_per_host=CONFIG['max_connections_per_host'],
            ttl_dns_cache=300,
            use_dns_cache=True,
            keepalive_timeout=60,
            enable_cleanup_closed=True
        )
        
        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=CONFIG['session_timeout'],
            headers={
                'User-Agent': 'FreshdeskMigration/1.0',
                'Accept': '*/*',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive'
            }
        )

    async def close_session(self):
        """Properly close the session"""
        if self.session:
            await self.session.close()

    def sanitize_filename(self, filename: str) -> str:
        """Sanitize filename for filesystem safety"""
        invalid_chars = '<>:"/\\|?*'
        for char in invalid_chars:
            filename = filename.replace(char, '_')
        
        # Limit length to prevent filesystem issues
        if len(filename) > 200:
            name, ext = os.path.splitext(filename)
            filename = name[:200-len(ext)] + ext
        
        return filename

    async def download_single_file(self, task: DownloadTask, semaphore: asyncio.Semaphore) -> bool:
        """Download a single file with retry logic"""
        async with semaphore:
            # Check if file already exists
            if task.filepath.exists():
                self.skipped_count += 1
                if self.downloaded_count % CONFIG['progress_report_interval'] == 0:
                    self.logger.info(f"Skipped existing: {task.filepath}")
                return True

            # Skip if URL already processed (deduplication)
            if task.url in self.downloaded_urls:
                self.skipped_count += 1
                return True

            # Ensure directory exists
            task.filepath.parent.mkdir(parents=True, exist_ok=True)

            for attempt in range(CONFIG['retry_attempts']):
                try:
                    # Determine if we need authentication
                    auth = None
                    if not ('s3.amazonaws.com' in task.url or 'cdn.freshdesk.com' in task.url):
                        auth = aiohttp.BasicAuth(os.getenv('FRESHDESK_API_KEY', ''), 'X')

                    async with self.session.get(task.url, auth=auth) as response:
                        if response.status == 200:
                            # Stream download to file
                            async with aiofiles.open(task.filepath, 'wb') as f:
                                async for chunk in response.content.iter_chunked(CONFIG['chunk_size']):
                                    await f.write(chunk)
                            
                            self.downloaded_count += 1
                            self.downloaded_urls.add(task.url)
                            
                            # Progress reporting
                            if self.downloaded_count % CONFIG['progress_report_interval'] == 0:
                                elapsed = time.time() - self.start_time
                                rate = self.downloaded_count / elapsed if elapsed > 0 else 0
                                self.logger.info(
                                    f"Downloaded {self.downloaded_count} files "
                                    f"({rate:.1f}/sec) - Latest: {task.filename}"
                                )
                            
                            return True
                        
                        elif response.status == 404:
                            self.logger.warning(f"File not found (404): {task.url}")
                            break  # Don't retry 404s
                        
                        elif response.status == 403:
                            self.logger.warning(f"Access denied (403) - URL may be expired: {task.url}")
                            break  # Don't retry 403s for now
                        
                        else:
                            self.logger.warning(f"HTTP {response.status} for {task.url}")

                except asyncio.TimeoutError:
                    self.logger.warning(f"Timeout on attempt {attempt + 1} for {task.url}")
                except Exception as e:
                    self.logger.error(f"Error on attempt {attempt + 1} for {task.url}: {e}")

                # Exponential backoff
                if attempt < CONFIG['retry_attempts'] - 1:
                    await asyncio.sleep(CONFIG['retry_delay_base'] * (2 ** attempt))

            # All attempts failed
            self.failed_count += 1
            self.logger.error(f"Failed to download after {CONFIG['retry_attempts']} attempts: {task.url}")
            return False

    def extract_download_tasks(self, base_dir: str = "attachments") -> List[DownloadTask]:
        """Extract all download tasks from JSON files"""
        tasks = []
        base_path = Path(base_dir)
        
        # Process ticket attachments
        ticket_attachments_dir = Path("ticket_attachments")
        if ticket_attachments_dir.exists():
            for json_file in ticket_attachments_dir.glob("ticket_*_attachments.json"):
                ticket_id = int(json_file.stem.split('_')[1])
                try:
                    with open(json_file, 'r') as f:
                        attachments = json.load(f)
                    
                    for att in attachments:
                        url = att.get('url') or att.get('attachment_url')
                        name = att.get('name')
                        if url and name:
                            safe_name = self.sanitize_filename(name)
                            filepath = base_path / str(ticket_id) / safe_name
                            
                            tasks.append(DownloadTask(
                                ticket_id=ticket_id,
                                url=url,
                                filename=name,
                                filepath=filepath,
                                source='ticket',
                                size=att.get('size'),
                                content_type=att.get('content_type')
                            ))
                except Exception as e:
                    self.logger.error(f"Error processing {json_file}: {e}")

        # Process conversation attachments
        conv_attachments_dir = Path("conversation_attachments")
        if conv_attachments_dir.exists():
            for json_file in conv_attachments_dir.glob("ticket_*_conversation_attachments.json"):
                ticket_id = int(json_file.stem.split('_')[1])
                try:
                    with open(json_file, 'r') as f:
                        attachments = json.load(f)
                    
                    for att in attachments:
                        url = att.get('url') or att.get('attachment_url')
                        name = att.get('name')
                        if url and name:
                            safe_name = f"conv_{self.sanitize_filename(name)}"
                            filepath = base_path / str(ticket_id) / safe_name
                            
                            tasks.append(DownloadTask(
                                ticket_id=ticket_id,
                                url=url,
                                filename=name,
                                filepath=filepath,
                                source='conversation',
                                size=att.get('size'),
                                content_type=att.get('content_type')
                            ))
                except Exception as e:
                    self.logger.error(f"Error processing {json_file}: {e}")

        return tasks

    async def download_batch(self, tasks: List[DownloadTask]) -> Dict[str, int]:
        """Download a batch of files concurrently"""
        semaphore = asyncio.Semaphore(CONFIG['max_concurrent_downloads'])
        
        # Create download tasks
        download_tasks = [
            self.download_single_file(task, semaphore)
            for task in tasks
        ]
        
        # Execute all downloads concurrently
        results = await asyncio.gather(*download_tasks, return_exceptions=True)
        
        # Count results
        successful = sum(1 for r in results if r is True)
        failed = sum(1 for r in results if r is False or isinstance(r, Exception))
        
        return {
            'successful': successful,
            'failed': failed,
            'total': len(tasks)
        }

    async def download_all_attachments(self) -> Dict[str, int]:
        """Main method to download all attachments"""
        self.logger.info("Starting high-performance attachment download...")
        
        # Extract all download tasks
        all_tasks = self.extract_download_tasks()
        
        if not all_tasks:
            self.logger.warning("No download tasks found!")
            return {'successful': 0, 'failed': 0, 'total': 0}

        self.logger.info(f"Found {len(all_tasks)} files to download")
        
        # Create session
        await self.create_session()
        
        try:
            # Process in batches if too many files
            if len(all_tasks) > CONFIG['batch_size']:
                total_stats = {'successful': 0, 'failed': 0, 'total': 0}
                
                for i in range(0, len(all_tasks), CONFIG['batch_size']):
                    batch = all_tasks[i:i + CONFIG['batch_size']]
                    self.logger.info(f"Processing batch {i//CONFIG['batch_size'] + 1} ({len(batch)} files)")
                    
                    batch_stats = await self.download_batch(batch)
                    for key in total_stats:
                        total_stats[key] += batch_stats[key]
                    
                    self.logger.info(f"Batch completed: {batch_stats}")
                
                return total_stats
            else:
                return await self.download_batch(all_tasks)
                
        finally:
            await self.close_session()

    def print_final_stats(self, stats: Dict[str, int]):
        """Print final download statistics"""
        elapsed = time.time() - self.start_time
        total_files = stats['total']
        successful = stats['successful']
        failed = stats['failed']
        rate = successful / elapsed if elapsed > 0 else 0
        
        self.logger.info("=" * 60)
        self.logger.info("DOWNLOAD COMPLETED")
        self.logger.info("=" * 60)
        self.logger.info(f"Total files processed: {total_files}")
        self.logger.info(f"Successfully downloaded: {successful}")
        self.logger.info(f"Failed downloads: {failed}")
        self.logger.info(f"Skipped (existing): {self.skipped_count}")
        self.logger.info(f"Time elapsed: {elapsed:.2f} seconds")
        self.logger.info(f"Download rate: {rate:.2f} files/second")
        self.logger.info(f"Success rate: {(successful/total_files*100):.1f}%" if total_files > 0 else "N/A")

async def main():
    """Main execution function"""
    downloader = HighPerformanceDownloader()
    stats = await downloader.download_all_attachments()
    downloader.print_final_stats(stats)

if __name__ == "__main__":
    asyncio.run(main())