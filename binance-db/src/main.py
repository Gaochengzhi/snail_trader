#!/usr/bin/env python3
"""
Enhanced main entry point with file-level tasks and improved progress tracking

Copyright (c) 2025 Gaochengzhi
Licensed under MIT License with Commercial Use Restriction

This software is free for personal and research use.
Commercial use by organizations requires explicit permission.
See LICENSE file for details.
"""

import sys
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any
from tqdm import tqdm

# Add src directory to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from logger_setup import setup_logger, get_logger
from utils import load_config, ensure_directory_exists
from task_tracker import TaskTracker, TaskStatus
from task_generator import generate_file_level_tasks
from downloader import BinanceDataDownloader


def execute_download_task(downloader: BinanceDataDownloader, 
                         task: Dict[str, Any], 
                         tracker: TaskTracker, 
                         pbar: tqdm,
                         stats: Dict[str, Any]) -> TaskStatus:
    """
    Execute a single download task and update progress with enhanced stats
    """
    try:
        start_time = time.time()
        result = downloader.download_file_task(task, tracker)
        end_time = time.time()
        
        # Update speed statistics
        with stats['lock']:
            stats['completed_count'] += 1
            stats['total_time'] += (end_time - start_time)
            
            # Calculate current speed (files per second)
            elapsed_total = time.time() - stats['start_time']
            current_speed = stats['completed_count'] / elapsed_total if elapsed_total > 0 else 0
            
            # Calculate ETA
            remaining = pbar.total - stats['completed_count']
            eta_seconds = remaining / current_speed if current_speed > 0 else 0
            eta_str = f"{int(eta_seconds//3600):02d}:{int((eta_seconds%3600)//60):02d}:{int(eta_seconds%60):02d}"
            
            # Update progress bar with enhanced info
            pbar.set_postfix_str(f"Speed: {current_speed:.1f} files/s | ETA: {eta_str}")
        
        pbar.update(1)
        return result
        
    except Exception as e:
        logger = get_logger()
        logger.error(f"Task execution error: {e}")
        pbar.update(1)
        return TaskStatus.FAILED


def run_enhanced_downloads(config: Dict[str, Any], tasks: List[Dict[str, Any]], tracker: TaskTracker):
    """
    Run downloads with enhanced tracking and progress display
    """
    logger = get_logger()
    
    if not tasks:
        logger.info("No tasks to download")
        return
    
    # Create output directories
    ensure_directory_exists(config["output_directory"])
    ensure_directory_exists(config["log_directory"])
    
    # Initialize downloader
    downloader = BinanceDataDownloader(config)
    
    # Get concurrency settings
    max_workers = config.get("download", {}).get("max_concurrent_downloads", 3)
    
    # Enhanced statistics with threading support
    import threading
    stats = {status: 0 for status in TaskStatus}
    progress_stats = {
        'completed_count': 0,
        'total_time': 0.0,
        'start_time': time.time(),
        'lock': threading.Lock()
    }
    
    logger.info(f"Starting {len(tasks)} download tasks with {max_workers} concurrent workers")
    logger.info(f"Target QPS: {config.get('download', {}).get('max_requests_per_second', 'unlimited')}")
    
    try:
        # Get UI settings
        ui_config = config.get('ui', {})
        progress_bar_width = ui_config.get('progress_bar_width', 100)
        
        # Create enhanced progress bar
        with tqdm(
            total=len(tasks),
            desc="Downloading",
            unit="files",
            ncols=progress_bar_width,
            leave=True,
            bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {postfix}]'
        ) as pbar:
            
            # Execute downloads with thread pool
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit all tasks with enhanced stats
                future_to_task = {
                    executor.submit(execute_download_task, downloader, task, tracker, pbar, progress_stats): task 
                    for task in tasks
                }
                
                # Process completed tasks
                completed_count = 0
                for future in as_completed(future_to_task):
                    task = future_to_task[future]
                    try:
                        result = future.result()
                        stats[result] += 1
                        completed_count += 1
                        
                        # Save progress periodically (every 100 tasks) instead of every task
                        if completed_count % 100 == 0:
                            tracker.save_progress()
                            logger.debug(f"Progress saved after {completed_count} completed tasks")
                            
                    except Exception as e:
                        logger.error(f"Task future error: {e}")
                        stats[TaskStatus.FAILED] += 1
    
    finally:
        downloader.close()
        # Final save
        tracker.save_progress()
    
    # Final summary
    total_processed = sum(stats.values())
    logger.info("=== Download Session Summary ===")
    logger.info(f"Total files processed: {total_processed}")
    logger.info(f"Completed: {stats[TaskStatus.COMPLETED]} ({stats[TaskStatus.COMPLETED]/total_processed*100:.1f}%)")
    logger.info(f"Failed: {stats[TaskStatus.FAILED]} ({stats[TaskStatus.FAILED]/total_processed*100:.1f}%)")
    
    # Show overall statistics including skipped files
    tracker.print_summary()


def main():
    """
    Enhanced main entry point
    """
    try:
        # Load configuration
        config = load_config()
        
        # Setup logging
        logger = setup_logger(config)
        logger.info("Enhanced Binance Public Data Downloader started")
        logger.info(f"Configuration loaded from config.yaml")
        
        # Initialize task tracker
        tracker = TaskTracker(config)
        
        # Generate file-level tasks with pre-filtering
        logger.info("Generating and pre-filtering tasks...")
        tasks = generate_file_level_tasks(config, tracker)
        
        if not tasks:
            logger.info("No tasks need to be downloaded. All files are up to date!")
            tracker.print_summary()
            return 0
        
        logger.info(f"Found {len(tasks)} files that need to be downloaded")
        
        # Run enhanced downloads
        run_enhanced_downloads(config, tasks, tracker)
        
        logger.info("Enhanced Binance Public Data Downloader finished")
        return 0
        
    except KeyboardInterrupt:
        logger = get_logger()
        logger.info("Download interrupted by user")
        return 1
    except Exception as e:
        if "logger" in locals():
            logger.error(f"Unexpected error: {e}")
        else:
            print(f"Critical error: {e}")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)