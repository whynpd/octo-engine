#!/usr/bin/env python3
"""
Pipeline Orchestrator
====================
Runs the producer and consumers in sequence with proper timing:

1. Run details_producer.py
2. Wait 5 seconds, then run conversations_consumer.py and attachments_consumer.py
3. Wait 10 seconds after that, then run conversation_attachments_consumer.py
"""

import subprocess
import time
import logging
import sys
from pathlib import Path

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('pipeline_orchestrator.log')
    ]
)
logger = logging.getLogger(__name__)

def run_script(script_name, background=False):
    """Run a Python script and return the process"""
    try:
        logger.info(f"🚀 Starting {script_name}...")
        
        if background:
            # Run in background
            process = subprocess.Popen(
                [sys.executable, script_name],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            logger.info(f"✅ {script_name} started in background (PID: {process.pid})")
            return process
        else:
            # Run and wait for completion
            result = subprocess.run(
                [sys.executable, script_name],
                capture_output=True,
                text=True,
                check=True
            )
            logger.info(f"✅ {script_name} completed successfully")
            return result
            
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ {script_name} failed with exit code {e.returncode}")
        logger.error(f"Error output: {e.stderr}")
        return None
    except Exception as e:
        logger.error(f"❌ Failed to run {script_name}: {e}")
        return None

def main():
    """Main orchestration function"""
    logger.info("🎯 Starting Pipeline Orchestration")
    logger.info("=" * 50)
    
    # Check if required files exist
    required_files = [
        'details_producer.py',
        'conversations_consumer.py', 
        'attachments_consumer.py',
        'conversation_attachments_consumer.py'
    ]
    
    for file in required_files:
        if not Path(file).exists():
            logger.error(f"❌ Required file not found: {file}")
            return 1
    
    # Step 1: Run details_producer.py (and wait for completion)
    logger.info("📊 STEP 1: Running Producer")
    producer_result = run_script('details_producer.py', background=False)
    if producer_result is None:
        logger.error("❌ Producer failed, stopping pipeline")
        return 1
    
    logger.info("⏱️  Producer completed, waiting 5 seconds before starting consumers...")
    time.sleep(5)
    
    # Step 2: Run conversations_consumer.py and attachments_consumer.py (in background)
    logger.info("📊 STEP 2: Starting First Wave of Consumers")
    conversations_process = run_script('conversations_consumer.py', background=True)
    attachments_process = run_script('attachments_consumer.py', background=True)
    
    if conversations_process is None or attachments_process is None:
        logger.error("❌ Failed to start first wave consumers")
        return 1
    
    logger.info("⏱️  First wave consumers started, waiting 10 seconds before starting final consumer...")
    time.sleep(10)
    
    # Step 3: Run conversation_attachments_consumer.py (in background)
    logger.info("📊 STEP 3: Starting Final Consumer")
    conv_attachments_process = run_script('conversation_attachments_consumer.py', background=True)
    
    if conv_attachments_process is None:
        logger.error("❌ Failed to start conversation attachments consumer")
        return 1
    
    logger.info("✅ All consumers started successfully!")
    logger.info("📋 Active Processes:")
    logger.info(f"   - conversations_consumer.py (PID: {conversations_process.pid})")
    logger.info(f"   - attachments_consumer.py (PID: {attachments_process.pid})")
    logger.info(f"   - conversation_attachments_consumer.py (PID: {conv_attachments_process.pid})")
    
    logger.info("🎯 Pipeline orchestration completed!")
    logger.info("💡 Consumers are running in background. Check their individual logs for progress.")
    logger.info("💡 Use 'ps aux | grep python' to see running processes")
    logger.info("💡 Use 'pkill -f consumer' to stop all consumers if needed")
    
    return 0

if __name__ == "__main__":
    try:
        exit_code = main()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        logger.info("🛑 Pipeline orchestration interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"❌ Unexpected error: {e}")
        sys.exit(1)