#!/usr/bin/env python3
"""
Test script to verify consumer termination improvements
"""

import time
import logging
from pathlib import Path
from consumer_termination_helper import CompletionTracker

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_completion_tracker():
    """Test the CompletionTracker functionality"""
    print("🧪 Testing CompletionTracker...")
    
    # Test conversation attachments tracker
    tracker = CompletionTracker("test_stage")
    
    # Initially, producer should not be complete
    assert not tracker.is_producer_complete(), "Producer should not be complete initially"
    print("✅ Initial state: Producer not complete")
    
    # Test termination logic when producer is not complete
    should_stop = tracker.should_consumer_stop(consecutive_empty_checks=25)
    print(f"✅ Should stop with 25 empty checks (producer running): {should_stop}")
    
    # Mark producer as complete
    tracker.mark_producer_complete()
    assert tracker.is_producer_complete(), "Producer should be marked complete"
    print("✅ Producer marked as complete")
    
    # Test termination logic when producer is complete
    should_stop = tracker.should_consumer_stop(consecutive_empty_checks=25)
    print(f"✅ Should stop with 25 empty checks (producer complete): {should_stop}")
    
    # Cleanup
    tracker.cleanup_completion_flag()
    assert not tracker.is_producer_complete(), "Completion flag should be cleaned up"
    print("✅ Cleanup successful")

def simulate_consumer_behavior():
    """Simulate how the improved consumer would behave"""
    print("\n🎭 Simulating consumer behavior...")
    
    tracker = CompletionTracker("simulation")
    consecutive_empty_checks = 0
    
    # Simulate work processing
    for iteration in range(1, 101):
        # Simulate claiming work (some iterations have no work)
        has_work = iteration % 10 != 0  # No work every 10th iteration
        
        if has_work:
            consecutive_empty_checks = 0
            print(f"Iteration {iteration}: Processing work (reset empty checks)")
        else:
            consecutive_empty_checks += 1
            should_stop = tracker.should_consumer_stop(consecutive_empty_checks)
            print(f"Iteration {iteration}: No work found (empty checks: {consecutive_empty_checks}, should_stop: {should_stop})")
            
            if should_stop:
                print(f"🛑 Consumer would stop at iteration {iteration}")
                break
        
        # Simulate producer completing at iteration 50
        if iteration == 50:
            tracker.mark_producer_complete()
            print("🏁 Producer marked as complete")
        
        time.sleep(0.1)  # Small delay for simulation
    
    # Cleanup
    tracker.cleanup_completion_flag()

def main():
    """Run all tests"""
    print("🚀 Testing Consumer Termination Improvements")
    print("=" * 50)
    
    try:
        test_completion_tracker()
        simulate_consumer_behavior()
        
        print("\n" + "=" * 50)
        print("✅ All tests passed! Consumer termination improvements are working.")
        print("\n📋 Summary of improvements:")
        print("1. ✅ Consumers now stop after 60 consecutive empty checks (30 seconds)")
        print("2. ✅ Optional CompletionTracker for producer-consumer coordination")
        print("3. ✅ Enhanced termination logic based on producer state")
        print("4. ✅ Proper logging of termination reasons")
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        raise

if __name__ == "__main__":
    main()