#!/usr/bin/env python3
"""
Optional helper module for more sophisticated consumer termination
This provides a completion flag mechanism for better coordination between producers and consumers
"""

import json
import time
import logging
from pathlib import Path
from filelock import FileLock
from typing import Dict, Any

# Configuration
COMPLETION_FLAGS_DIR = Path("completion_flags")
COMPLETION_FLAGS_DIR.mkdir(exist_ok=True)

logger = logging.getLogger(__name__)

class CompletionTracker:
    """
    Tracks completion state for different processing stages
    """
    
    def __init__(self, stage_name: str):
        self.stage_name = stage_name
        self.flag_file = COMPLETION_FLAGS_DIR / f"{stage_name}_completion.json"
        self.lock_file = COMPLETION_FLAGS_DIR / f"{stage_name}_completion.lock"
    
    def mark_producer_complete(self) -> None:
        """Mark that the producer for this stage has finished adding work"""
        try:
            with FileLock(self.lock_file):
                data = {
                    "producer_complete": True,
                    "timestamp": time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
                    "stage": self.stage_name
                }
                self.flag_file.write_text(json.dumps(data, indent=2), encoding='utf-8')
                logger.info(f"[{self.stage_name}] Producer marked as complete")
        except Exception as e:
            logger.error(f"[{self.stage_name}] Failed to mark producer complete: {e}")
    
    def is_producer_complete(self) -> bool:
        """Check if the producer for this stage has finished"""
        try:
            if not self.flag_file.exists():
                return False
            
            with FileLock(self.lock_file):
                data = json.loads(self.flag_file.read_text(encoding='utf-8'))
                return data.get("producer_complete", False)
        except Exception as e:
            logger.warning(f"[{self.stage_name}] Failed to check producer completion: {e}")
            return False
    
    def should_consumer_stop(self, consecutive_empty_checks: int, max_empty_checks: int = 20) -> bool:
        """
        Enhanced logic to determine if consumer should stop
        
        Args:
            consecutive_empty_checks: Number of consecutive times no work was found
            max_empty_checks: Maximum empty checks to allow when producer is complete
        
        Returns:
            bool: True if consumer should stop
        """
        producer_done = self.is_producer_complete()
        
        if producer_done:
            # If producer is done, stop after fewer empty checks
            return consecutive_empty_checks >= max_empty_checks
        else:
            # If producer is still running, use a higher threshold
            return consecutive_empty_checks >= (max_empty_checks * 3)
    
    def cleanup_completion_flag(self) -> None:
        """Clean up completion flag file"""
        try:
            if self.flag_file.exists():
                self.flag_file.unlink()
                logger.info(f"[{self.stage_name}] Cleaned up completion flag")
        except Exception as e:
            logger.warning(f"[{self.stage_name}] Failed to cleanup completion flag: {e}")


def enhanced_worker_loop_template(stage_name: str, claim_function, process_function, set_status_function):
    """
    Template for enhanced worker loops with completion tracking
    
    Args:
        stage_name: Name of the processing stage (e.g., "conversations", "attachments")
        claim_function: Function to claim next work item
        process_function: Function to process a work item
        set_status_function: Function to set status on failure
    
    Returns:
        Function that can be used as worker_loop
    """
    
    def worker_loop(stop_flag):
        tracker = CompletionTracker(stage_name)
        consecutive_empty_checks = 0
        
        while not stop_flag[0]:
            tid = claim_function()
            
            if tid == -1:
                consecutive_empty_checks += 1
                
                # Use enhanced termination logic
                if tracker.should_consumer_stop(consecutive_empty_checks):
                    logger.info(f"[{stage_name}] Worker stopping - producer complete: {tracker.is_producer_complete()}, "
                               f"empty checks: {consecutive_empty_checks}")
                    break
                
                time.sleep(0.5)
                continue
            
            # Reset counter when work is found
            consecutive_empty_checks = 0
            
            try:
                process_function(tid)
            except Exception as exc:
                logger.exception(f"[{stage_name}] Error processing {tid}: {exc}")
                set_status_function(tid, "NA")
    
    return worker_loop


# Example usage functions for each consumer type
def create_conversations_worker_loop():
    """Create enhanced worker loop for conversations consumer"""
    from migration_store import claim_next_null_conversations, set_conversations_status
    from conversations_consumer import process_one_conversation
    
    return enhanced_worker_loop_template(
        "conversations",
        claim_next_null_conversations,
        process_one_conversation,
        set_conversations_status
    )

def create_attachments_worker_loop():
    """Create enhanced worker loop for attachments consumer"""
    from migration_store import claim_next_null_attachments, set_attachments_status
    from attachments_consumer import process_one
    
    return enhanced_worker_loop_template(
        "attachments",
        claim_next_null_attachments,
        process_one,
        set_attachments_status
    )

def create_conversation_attachments_worker_loop():
    """Create enhanced worker loop for conversation attachments consumer"""
    from migration_store import claim_next_null_conversation_attachments, set_conversation_attachments_status
    from conversation_attachments_consumer import process_one_conversation_attachment
    
    return enhanced_worker_loop_template(
        "conversation_attachments",
        claim_next_null_conversation_attachments,
        process_one_conversation_attachment,
        set_conversation_attachments_status
    )


if __name__ == "__main__":
    # Example of how to use this
    print("Consumer Termination Helper - Example Usage")
    
    # For producers to mark completion
    tracker = CompletionTracker("conversations")
    tracker.mark_producer_complete()
    
    # For consumers to check if they should stop
    print(f"Should stop: {tracker.should_consumer_stop(25)}")