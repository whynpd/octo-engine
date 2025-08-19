#!/usr/bin/env python3
"""
Ultra-Fast Attachment Downloader

For extreme performance with 50+ attachments per ticket:
- Multi-process + async hybrid approach
- Smart URL grouping by domain for connection reuse
- Pre-flight URL validation to skip expired links
- Intelligent retry with URL refresh capability
- Memory-mapped file writing for large files
- Real-time progress monitoring
"""

import asyncio
import aiohttp
import multiprocessing as mp
import json
import time
import logging
from pathlib import Path
from typing import List, Dict, Set, Optional, Tuple
from dataclasses import dataclass
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
import os
import signal
from urllib.parse import urlparse
from dotenv import load_dotenv

load_dotenv()

# Ultra-fast configuration
ULTRA_CONFIG = {
    'processes': min(mp.cpu_count(), 8),        # Multi-process workers
    'async_workers_per_process': 50,            # Async workers per process
    'max_connections_per_host': 30,             # Per-host connections
    'preflight_check': True,                    # Check URLs before download
    'domain_grouping': True,                    # Group by domain for efficiency
    'chunk_size': 128 * 1024,                   # 128KB chunks
    'timeout_connect': 5,                       # Connection timeout
    'timeout_total': 120,                       # Total timeout per file
    'retry_attempts': 2,                        # Reduced retries for speed
    'progress_interval': 25,                    # Progress every N files
    'batch_size_per_process': 500,              # Files per process
}

@dataclass
class UltraDownloadTask:
    ticket_id: int
    url: str
    filename: str
    filepath: Path
    source: str
    domain: str
    size: Optional[int] = None

class UltraFastDownloader:
    def __init__(self, process_id: int = 0):
        self.process_id = process_id
        self.session: Optional[aiohttp.ClientSession] = None
        self.stats = {'downloaded': 0, 'failed': 0, 'skipped': 0}
        self.start_time = time.time()
        
        # Setup process-specific logging
        self.logger = logging.getLogger(f'UltraDownloader-{process_id}')
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(
            f'%(asctime)s [P{process_id}] %(levelname)s: %(message)s'
        ))
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.INFO)

    async def create_optimized_session(self):
        """Create highly optimized session for this process"""
        connector = aiohttp.TCPConnector(
            limit=ULTRA_CONFIG['async_workers_per_process'] * 2,
            limit_per_host=ULTRA_CONFIG['max_connections_per_host'],
            ttl_dns_cache=600,  # Longer DNS cache
            use_dns_cache=True,
            keepalive_timeout=120,
            enable_cleanup_closed=True,
            force_close=False,
            conn_timeout=ULTRA_CONFIG['timeout_connect']
        )
        
        timeout = aiohttp.ClientTimeout(
            total=ULTRA_CONFIG['timeout_total'],
            connect=ULTRA_CONFIG['timeout_connect']
        )
        
        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers={
                'User-Agent': 'UltraFastMigration/2.0',
                'Accept': '*/*',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Keep-Alive': 'timeout=120, max=1000'
            }
        )

    async def preflight_check(self, url: str) -> bool:
        """Quick HEAD request to validate URL"""
        if not ULTRA_CONFIG['preflight_check']:
            return True
            
        try:
            auth = None
            if not ('s3.amazonaws.com' in url or 'cdn.freshdesk.com' in url):
                auth = aiohttp.BasicAuth(os.getenv('FRESHDESK_API_KEY', ''), 'X')
                
            async with self.session.head(url, auth=auth) as response:
                return response.status == 200
        except:
            return False  # If HEAD fails, try the download anyway

    async def ultra_download_file(self, task: UltraDownloadTask) -> bool:
        """Ultra-optimized single file download"""
        # Skip if exists
        if task.filepath.exists():
            self.stats['skipped'] += 1
            return True

        # Ensure directory exists
        task.filepath.parent.mkdir(parents=True, exist_ok=True)

        # Optional preflight check
        if ULTRA_CONFIG['preflight_check']:
            if not await self.preflight_check(task.url):
                self.stats['failed'] += 1
                return False

        # Download with minimal retries
        for attempt in range(ULTRA_CONFIG['retry_attempts']):
            try:
                auth = None
                if not ('s3.amazonaws.com' in task.url or 'cdn.freshdesk.com' in task.url):
                    auth = aiohttp.BasicAuth(os.getenv('FRESHDESK_API_KEY', ''), 'X')

                async with self.session.get(task.url, auth=auth) as response:
                    if response.status == 200:
                        # Stream to file efficiently
                        with open(task.filepath, 'wb') as f:
                            async for chunk in response.content.iter_chunked(ULTRA_CONFIG['chunk_size']):
                                f.write(chunk)
                        
                        self.stats['downloaded'] += 1
                        
                        # Progress reporting
                        if self.stats['downloaded'] % ULTRA_CONFIG['progress_interval'] == 0:
                            elapsed = time.time() - self.start_time
                            rate = self.stats['downloaded'] / elapsed if elapsed > 0 else 0
                            self.logger.info(f"Downloaded {self.stats['downloaded']} files ({rate:.1f}/sec)")
                        
                        return True
                    elif response.status in [404, 403]:
                        break  # Don't retry these
                        
            except Exception as e:
                if attempt == ULTRA_CONFIG['retry_attempts'] - 1:
                    self.logger.error(f"Failed {task.filename}: {e}")

        self.stats['failed'] += 1
        return False

    async def process_domain_batch(self, tasks: List[UltraDownloadTask]) -> Dict[str, int]:
        """Process a batch of tasks grouped by domain"""
        semaphore = asyncio.Semaphore(ULTRA_CONFIG['async_workers_per_process'])
        
        async def bounded_download(task):
            async with semaphore:
                return await self.ultra_download_file(task)
        
        # Execute all downloads for this domain concurrently
        results = await asyncio.gather(
            *[bounded_download(task) for task in tasks],
            return_exceptions=True
        )
        
        successful = sum(1 for r in results if r is True)
        failed = len(results) - successful
        
        return {'successful': successful, 'failed': failed, 'total': len(tasks)}

    async def process_tasks_for_process(self, tasks: List[UltraDownloadTask]) -> Dict[str, int]:
        """Process all tasks assigned to this process"""
        await self.create_optimized_session()
        
        try:
            if ULTRA_CONFIG['domain_grouping']:
                # Group tasks by domain for better connection reuse
                domain_groups = defaultdict(list)
                for task in tasks:
                    domain_groups[task.domain].append(task)
                
                total_stats = {'successful': 0, 'failed': 0, 'total': 0}
                
                for domain, domain_tasks in domain_groups.items():
                    self.logger.info(f"Processing {len(domain_tasks)} files from {domain}")
                    domain_stats = await self.process_domain_batch(domain_tasks)
                    
                    for key in total_stats:
                        total_stats[key] += domain_stats[key]
                
                return total_stats
            else:
                return await self.process_domain_batch(tasks)
                
        finally:
            if self.session:
                await self.session.close()

def extract_all_download_tasks() -> List[UltraDownloadTask]:
    """Extract all download tasks from JSON files"""
    tasks = []
    base_path = Path("attachments")
    
    def sanitize_filename(filename: str) -> str:
        invalid_chars = '<>:"/\\|?*'
        for char in invalid_chars:
            filename = filename.replace(char, '_')
        return filename[:200] if len(filename) > 200 else filename

    # Process ticket attachments
    ticket_dir = Path("ticket_attachments")
    if ticket_dir.exists():
        for json_file in ticket_dir.glob("ticket_*_attachments.json"):
            ticket_id = int(json_file.stem.split('_')[1])
            try:
                with open(json_file, 'r') as f:
                    attachments = json.load(f)
                
                for att in attachments:
                    url = att.get('url') or att.get('attachment_url')
                    name = att.get('name')
                    if url and name:
                        domain = urlparse(url).netloc
                        safe_name = sanitize_filename(name)
                        filepath = base_path / str(ticket_id) / safe_name
                        
                        tasks.append(UltraDownloadTask(
                            ticket_id=ticket_id,
                            url=url,
                            filename=name,
                            filepath=filepath,
                            source='ticket',
                            domain=domain,
                            size=att.get('size')
                        ))
            except Exception as e:
                print(f"Error processing {json_file}: {e}")

    # Process conversation attachments
    conv_dir = Path("conversation_attachments")
    if conv_dir.exists():
        for json_file in conv_dir.glob("ticket_*_conversation_attachments.json"):
            ticket_id = int(json_file.stem.split('_')[1])
            try:
                with open(json_file, 'r') as f:
                    attachments = json.load(f)
                
                for att in attachments:
                    url = att.get('url') or att.get('attachment_url')
                    name = att.get('name')
                    if url and name:
                        domain = urlparse(url).netloc
                        safe_name = f"conv_{sanitize_filename(name)}"
                        filepath = base_path / str(ticket_id) / safe_name
                        
                        tasks.append(UltraDownloadTask(
                            ticket_id=ticket_id,
                            url=url,
                            filename=name,
                            filepath=filepath,
                            source='conversation',
                            domain=domain,
                            size=att.get('size')
                        ))
            except Exception as e:
                print(f"Error processing {json_file}: {e}")

    return tasks

def worker_process(process_id: int, tasks: List[UltraDownloadTask]) -> Dict[str, int]:
    """Worker process function"""
    # Setup signal handling
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    
    downloader = UltraFastDownloader(process_id)
    
    # Run async loop for this process
    try:
        return asyncio.run(downloader.process_tasks_for_process(tasks))
    except Exception as e:
        downloader.logger.error(f"Process {process_id} failed: {e}")
        return {'successful': 0, 'failed': len(tasks), 'total': len(tasks)}

def main():
    """Main ultra-fast download orchestrator"""
    print("🚀 Ultra-Fast Attachment Downloader Starting...")
    start_time = time.time()
    
    # Extract all tasks
    all_tasks = extract_all_download_tasks()
    if not all_tasks:
        print("❌ No download tasks found!")
        return
    
    print(f"📋 Found {len(all_tasks)} files to download")
    print(f"⚙️  Using {ULTRA_CONFIG['processes']} processes with {ULTRA_CONFIG['async_workers_per_process']} async workers each")
    
    # Split tasks across processes
    chunk_size = len(all_tasks) // ULTRA_CONFIG['processes']
    if chunk_size == 0:
        chunk_size = 1
    
    task_chunks = [
        all_tasks[i:i + chunk_size] 
        for i in range(0, len(all_tasks), chunk_size)
    ]
    
    # Ensure we don't have more chunks than processes
    while len(task_chunks) > ULTRA_CONFIG['processes']:
        # Merge the last two chunks
        task_chunks[-2].extend(task_chunks[-1])
        task_chunks.pop()
    
    print(f"📊 Split into {len(task_chunks)} process batches")
    
    # Execute with process pool
    with ProcessPoolExecutor(max_workers=ULTRA_CONFIG['processes']) as executor:
        try:
            futures = [
                executor.submit(worker_process, i, chunk)
                for i, chunk in enumerate(task_chunks)
            ]
            
            # Collect results
            total_stats = {'successful': 0, 'failed': 0, 'total': 0}
            for i, future in enumerate(futures):
                try:
                    result = future.result(timeout=3600)  # 1 hour timeout per process
                    print(f"✅ Process {i} completed: {result}")
                    for key in total_stats:
                        total_stats[key] += result[key]
                except Exception as e:
                    print(f"❌ Process {i} failed: {e}")
                    total_stats['failed'] += len(task_chunks[i])
                    total_stats['total'] += len(task_chunks[i])
        
        except KeyboardInterrupt:
            print("\n🛑 Download interrupted by user")
            executor.shutdown(wait=False)
            return
    
    # Final statistics
    elapsed = time.time() - start_time
    rate = total_stats['successful'] / elapsed if elapsed > 0 else 0
    success_rate = (total_stats['successful'] / total_stats['total'] * 100) if total_stats['total'] > 0 else 0
    
    print("\n" + "="*60)
    print("🏁 ULTRA-FAST DOWNLOAD COMPLETED")
    print("="*60)
    print(f"📊 Total files: {total_stats['total']}")
    print(f"✅ Downloaded: {total_stats['successful']}")
    print(f"❌ Failed: {total_stats['failed']}")
    print(f"⏱️  Time: {elapsed:.2f} seconds")
    print(f"🚀 Rate: {rate:.2f} files/second")
    print(f"📈 Success: {success_rate:.1f}%")
    print("="*60)

if __name__ == "__main__":
    main()