#!/usr/bin/env python3
"""
Enhanced task generator with file-level granularity and pre-filtering
"""

import os
from pathlib import Path
from typing import List, Dict, Any, Set
from concurrent.futures import ThreadPoolExecutor
from logger_setup import get_logger
from utils import (
    generate_date_range,
    get_all_trading_pairs,
    build_download_url,
    get_output_filename,
    get_file_directory
)
from task_tracker import TaskTracker, TaskStatus


def batch_check_files_exist(file_paths: List[str], max_workers: int = 8) -> Set[str]:
    """
    Batch check file existence using thread pool for I/O parallelization
    Returns set of existing file paths
    """
    existing_files = set()
    
    def check_file(path: str) -> str:
        try:
            if Path(path).is_file():
                return path
        except:
            pass
        return None
    
    # Use thread pool for I/O bound file checking
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = executor.map(check_file, file_paths)
        existing_files = {path for path in results if path is not None}
    
    return existing_files


def build_task_fast(symbol: str, data_type: str, date: str, interval: str, 
                   base_url: str, output_dir: str) -> Dict[str, Any]:
    """
    Fast task building with correct filename logic (matching utils.py)
    """
    # Build paths inline for better performance - CORRECTED LOGIC
    types_with_datatype = ["metrics", "bookDepth"]
    
    if interval:
        # K线类型的URL构建: {data_type}/{symbol}/{interval}/{symbol}-{interval}-{date}.zip
        url_part = f"{data_type}/{symbol}/{interval}/{symbol}-{interval}-{date}.zip"
        file_subdir = f"{data_type}/{symbol}/{interval}"
        # K线类型文件名: symbol-interval-date (不包含data_type)
        filename_base = f"{symbol}-{interval}-{date}"
    else:
        # 非K线类型URL构建: {data_type}/{symbol}/{symbol}-{data_type}-{date}.zip  
        url_part = f"{data_type}/{symbol}/{symbol}-{data_type}-{date}.zip"
        file_subdir = f"{data_type}/{symbol}"
        # 根据类型决定文件名格式
        if data_type in types_with_datatype:
            filename_base = f"{symbol}-{data_type}-{date}"
        else:
            filename_base = f"{symbol}-{data_type}-{date}"
    
    url = f"{base_url.rstrip('/')}/{url_part}"
    file_dir = os.path.join(output_dir, file_subdir)
    zip_filename = f"{filename_base}.zip"
    csv_filename = f"{filename_base}.csv"
    zip_path = os.path.join(file_dir, zip_filename)
    csv_path = os.path.join(file_dir, csv_filename)
    
    return {
        'symbol': symbol, 'data_type': data_type, 'date': date, 'interval': interval,
        'url': url, 'file_dir': file_dir, 'zip_filename': zip_filename,
        'csv_filename': csv_filename, 'zip_path': zip_path, 'csv_path': csv_path
    }


def generate_file_level_tasks(config: Dict[str, Any], tracker: TaskTracker) -> List[Dict[str, Any]]:
    """
    High-performance task generation with batch file checking and optimized path building
    """
    logger = get_logger()
    
    # Get time range
    time_range = config["time_range"]
    dates = generate_date_range(time_range["start_date"], time_range["end_date"])
    logger.info(f"Date range: {len(dates)} days from {dates[0]} to {dates[-1]}")

    # Get trading pairs
    trading_pairs = config.get("trading_pairs", [])
    if not trading_pairs:
        logger.info("No specific trading pairs configured, fetching all from Binance API...")
        try:
            trading_pairs = get_all_trading_pairs()
            logger.info(f"Found {len(trading_pairs)} trading pairs from API")
        except Exception as e:
            logger.error(f"Failed to fetch trading pairs: {e}")
            return []
    else:
        logger.info(f"Using configured trading pairs: {len(trading_pairs)} symbols")

    # Get data types and intervals
    data_types = config["data_types"]
    kline_intervals = config.get("kline_intervals", [])
    base_url = config['base_url']
    output_dir = config['output_directory']

    # High-performance task generation using fast path building
    all_tasks = []
    logger.info("Generating file-level tasks (optimized)...")
    
    # Generate tasks with optimized loops
    for date in dates:
        for symbol in trading_pairs:
            for data_type, enabled in data_types.items():
                if not enabled:
                    continue
                
                if data_type in ['indexPriceKlines', 'klines', 'markPriceKlines', 'premiumIndexKlines']:
                    if kline_intervals:
                        for interval in kline_intervals:
                            task = build_task_fast(symbol, data_type, date, interval, base_url, output_dir)
                            all_tasks.append(task)
                    else:
                        logger.warning(f"No intervals specified for {data_type}, skipping")
                else:
                    task = build_task_fast(symbol, data_type, date, None, base_url, output_dir)
                    all_tasks.append(task)
    
    logger.info(f"Generated {len(all_tasks)} potential file tasks")
    
    # Batch file existence check for maximum performance
    logger.info("Batch checking file existence...")
    all_csv_paths = [task['csv_path'] for task in all_tasks]
    existing_files = batch_check_files_exist(all_csv_paths, max_workers=16)
    logger.info(f"Found {len(existing_files)} existing files")
    
    # Fast pre-filtering using set lookup
    logger.info("Pre-filtering tasks (optimized)...")
    pre_filtered_tasks = []
    skipped_count = 0
    failed_retry_count = 0
    
    for task in all_tasks:
        csv_path = task['csv_path']
        
        # Fast set lookup instead of individual file checks
        if csv_path in existing_files:
            skipped_count += 1
            continue
        
        # Only check task status for files that don't exist
        symbol = task['symbol']
        data_type = task['data_type']
        date = task['date']
        interval = task.get('interval')
        
        current_status = tracker.get_task_status(symbol, data_type, date, interval)
        if current_status == TaskStatus.FAILED:
            failed_retry_count += 1
        
        pre_filtered_tasks.append(task)
    
    logger.info(f"Pre-filtering results:")
    logger.info(f"  - Skipped (already complete): {skipped_count}")
    logger.info(f"  - Failed tasks for retry: {failed_retry_count}")
    logger.info(f"  - Tasks needing download: {len(pre_filtered_tasks)}")
    
    # Only save if there are failed tasks to track
    if failed_retry_count > 0:
        tracker.save_progress()
    
    return pre_filtered_tasks


