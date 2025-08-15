#!/usr/bin/env python3
"""
Test script for enhanced resume functionality
"""

import sys
import os

# Add src directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), '../src'))

from logger_setup import setup_logger, get_logger
from utils import load_config, ensure_directory_exists
from task_tracker import TaskTracker, TaskStatus
from task_generator import generate_file_level_tasks
from downloader import BinanceDataDownloader
from main import run_enhanced_downloads


def test_resume_functionality():
    """Test enhanced resume functionality"""
    try:
        config = load_config('test_enhanced_config.yaml')
        logger = setup_logger(config)
        logger.info("=== Enhanced Resume Test Started ===")
        
        ensure_directory_exists(config['output_directory'])
        ensure_directory_exists(config['log_directory'])
        
        tracker = TaskTracker(config)
        
        # First run
        logger.info("=== First Download Run ===")
        tasks = generate_file_level_tasks(config, tracker)
        logger.info(f"Generated {len(tasks)} tasks for first run")
        
        if tasks:
            run_enhanced_downloads(config, tasks, tracker)
        
        stats1 = tracker.get_statistics()
        logger.info(f"First run statistics: {stats1}")
        
        # Second run (should skip completed files)
        logger.info("=== Second Download Run (Resume Test) ===")
        tasks2 = generate_file_level_tasks(config, tracker)
        logger.info(f"Generated {len(tasks2)} tasks for second run")
        
        if tasks2:
            run_enhanced_downloads(config, tasks2, tracker)
        
        stats2 = tracker.get_statistics()
        logger.info(f"Final statistics: {stats2}")
        
        # Test should show improvement in resume
        if len(tasks2) < len(tasks) or len(tasks2) == 0:
            logger.info("✓ Resume functionality test PASSED")
            return 0
        else:
            logger.warning("⚠ Resume test completed but files were not properly skipped")
            return 1
            
    except Exception as e:
        if 'logger' in locals():
            logger.error(f"Test FAILED with error: {e}")
        else:
            print(f"Critical test error: {e}")
        return 1


def main():
    print("Enhanced Binance Data Downloader - Resume Test")
    print("=" * 50)
    return test_resume_functionality()


if __name__ == "__main__":
    sys.exit(main())