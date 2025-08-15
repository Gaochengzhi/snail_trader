#!/usr/bin/env python3
"""
Enhanced downloader with improved task tracking and status management
"""

import os
import time
import zipfile
import requests
import threading
from typing import Dict, Any
from logger_setup import get_logger
from utils import (
    ensure_directory_exists,
    format_file_size
)
from task_tracker import TaskTracker, TaskStatus


class QPSController:
    """
    Token bucket based QPS controller for high-concurrency scenarios
    Allows bursts while maintaining average rate limit
    """
    
    def __init__(self, max_qps: float):
        self.max_qps = max_qps
        self.bucket_size = max(max_qps * 2, 10)  # Allow burst up to 2 seconds worth
        self.tokens = self.bucket_size
        self.last_refill = time.time()
        self._lock = threading.Lock()
        
    def _refill_tokens(self):
        """Refill tokens based on elapsed time"""
        now = time.time()
        elapsed = now - self.last_refill
        tokens_to_add = elapsed * self.max_qps
        self.tokens = min(self.bucket_size, self.tokens + tokens_to_add)
        self.last_refill = now
    
    def wait_if_needed(self):
        """
        Wait if necessary to maintain the configured QPS (non-blocking for available tokens)
        """
        if self.max_qps <= 0:
            return
            
        with self._lock:
            self._refill_tokens()
            
            if self.tokens >= 1:
                self.tokens -= 1
                return  # Token available, proceed immediately
            
            # No tokens available, calculate wait time
            wait_time = (1 - self.tokens) / self.max_qps
            
        # Sleep outside the lock to avoid blocking other threads
        time.sleep(wait_time)
        
        # Try to consume token after waiting
        with self._lock:
            self._refill_tokens()
            if self.tokens >= 1:
                self.tokens -= 1


class BinanceDataDownloader:
    """
    Enhanced downloader with file-level task tracking and three-state status management
    """
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = get_logger()
        self.base_url = config['base_url']
        self.output_dir = config['output_directory']
        
        # Download settings
        download_config = config.get('download', {})
        self.retry_attempts = download_config.get('retry_attempts', 3)
        self.retry_delay = download_config.get('retry_delay', 5)
        
        # QPS control - support both old and new config format
        if 'max_requests_per_second' in download_config:
            max_qps = download_config.get('max_requests_per_second', 10.0)
        else:
            # Legacy support: convert rate_limit_delay to QPS
            rate_limit_delay = download_config.get('rate_limit_delay', 0.1)
            max_qps = 1.0 / rate_limit_delay if rate_limit_delay > 0 else 10.0
        
        self.qps_controller = QPSController(max_qps)
        self.chunk_size = download_config.get('chunk_size', 8192)
        
        # File processing settings
        file_config = config.get('file_processing', {})
        self.auto_extract = file_config.get('auto_extract', True)
        self.delete_zip = file_config.get('delete_zip_after_extract', True)
        
        # Session for connection pooling
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Binance-Data-Downloader/2.0'
        })
    
    def download_file_task(self, task: Dict[str, Any], tracker: TaskTracker) -> TaskStatus:
        """
        Download a single file task with improved error handling and status tracking
        """
        symbol = task['symbol']
        data_type = task['data_type']
        date = task['date']
        interval = task.get('interval')
        url = task['url']
        file_dir = task['file_dir']
        zip_path = task['zip_path']
        csv_path = task['csv_path']
        zip_filename = task['zip_filename']
        csv_filename = task['csv_filename']
        
        try:
            # Ensure directory exists
            ensure_directory_exists(file_dir)
            
            # Download with retry mechanism
            for attempt in range(self.retry_attempts):
                try:
                    self.logger.debug(f"Downloading [{attempt + 1}/{self.retry_attempts}]: {zip_filename}")
                    
                    # Apply QPS control
                    self.qps_controller.wait_if_needed()
                    
                    response = self.session.get(url, stream=True, timeout=30)
                    
                    # Handle 404 errors (file doesn't exist for this date/symbol)
                    if response.status_code == 404:
                        error_msg = f"Data not available (404): {url}"
                        self.logger.warning(error_msg)
                        tracker.set_task_status(
                            symbol, data_type, date, TaskStatus.FAILED, 
                            interval, error_msg
                        )
                        return TaskStatus.FAILED  # Don't retry for 404
                    
                    response.raise_for_status()
                    
                    # Download file
                    total_size = int(response.headers.get('content-length', 0))
                    downloaded_size = 0
                    
                    with open(zip_path, 'wb') as f:
                        for chunk in response.iter_content(chunk_size=self.chunk_size):
                            if chunk:
                                f.write(chunk)
                                downloaded_size += len(chunk)
                    
                    self.logger.debug(f"Downloaded {format_file_size(downloaded_size)}: {zip_filename}")
                    
                    # Extract if enabled - 确保原子性操作
                    if self.auto_extract:
                        # 尝试解压，如果失败就清理不完整的文件
                        try:
                            if self._extract_zip_file(zip_path, file_dir):
                                self.logger.debug(f"Extracted: {csv_filename}")
                                
                                # 验证解压后的文件确实存在
                                if os.path.exists(csv_path):
                                    # Delete ZIP file if configured
                                    if self.delete_zip:
                                        os.remove(zip_path)
                                        self.logger.debug(f"Deleted ZIP file: {zip_filename}")
                                    
                                    # 解压成功，不需要保存状态（依赖文件存在即可）
                                    return TaskStatus.COMPLETED
                                else:
                                    # ZIP解压说成功但文件不存在，清理并标记失败
                                    if os.path.exists(zip_path):
                                        os.remove(zip_path)
                                    error_msg = f"Extraction successful but CSV file missing: {csv_filename}"
                                    self.logger.error(error_msg)
                                    tracker.set_task_status(
                                        symbol, data_type, date, TaskStatus.FAILED,
                                        interval, error_msg
                                    )
                                    return TaskStatus.FAILED
                            else:
                                # 解压失败，清理不完整的文件
                                if os.path.exists(zip_path):
                                    os.remove(zip_path)
                                if os.path.exists(csv_path):
                                    os.remove(csv_path)  # 清理可能损坏的CSV
                                error_msg = f"Failed to extract: {zip_filename}"
                                self.logger.error(error_msg)
                                tracker.set_task_status(
                                    symbol, data_type, date, TaskStatus.FAILED,
                                    interval, error_msg
                                )
                                return TaskStatus.FAILED
                        except Exception as e:
                            # 解压过程中出现异常，清理所有相关文件
                            try:
                                if os.path.exists(zip_path):
                                    os.remove(zip_path)
                                if os.path.exists(csv_path):
                                    os.remove(csv_path)
                            except:
                                pass  # 清理失败也不影响
                            error_msg = f"Exception during extraction: {e}"
                            self.logger.error(error_msg)
                            tracker.set_task_status(
                                symbol, data_type, date, TaskStatus.FAILED,
                                interval, error_msg
                            )
                            return TaskStatus.FAILED
                    else:
                        # No extraction, just mark download as completed
                        file_size = os.path.getsize(zip_path) if os.path.exists(zip_path) else 0
                        tracker.set_task_status(
                            symbol, data_type, date, TaskStatus.COMPLETED,
                            interval, file_size=file_size
                        )
                        return TaskStatus.COMPLETED
                    
                except requests.exceptions.RequestException as e:
                    error_msg = f"Download attempt {attempt + 1} failed: {e}"
                    self.logger.warning(error_msg)
                    if attempt < self.retry_attempts - 1:
                        self.logger.debug(f"Retrying in {self.retry_delay} seconds...")
                        time.sleep(self.retry_delay)
                    else:
                        final_error = f"All download attempts failed for: {url}"
                        self.logger.error(final_error)
                        tracker.set_task_status(
                            symbol, data_type, date, TaskStatus.FAILED,
                            interval, final_error
                        )
                        return TaskStatus.FAILED
                        
                except Exception as e:
                    error_msg = f"Unexpected error during download: {e}"
                    self.logger.error(error_msg)
                    tracker.set_task_status(
                        symbol, data_type, date, TaskStatus.FAILED,
                        interval, error_msg
                    )
                    return TaskStatus.FAILED
            
            # Should not reach here
            tracker.set_task_status(
                symbol, data_type, date, TaskStatus.FAILED,
                interval, "Unknown download failure"
            )
            return TaskStatus.FAILED
            
        except Exception as e:
            error_msg = f"Error in download_file_task: {e}"
            self.logger.error(error_msg)
            tracker.set_task_status(
                symbol, data_type, date, TaskStatus.FAILED,
                interval, error_msg
            )
            return TaskStatus.FAILED
    
    def _extract_zip_file(self, zip_path: str, extract_dir: str) -> bool:
        """
        Extract ZIP file and handle potential errors
        """
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                # 调试：查看ZIP文件内容
                file_list = zip_ref.namelist()
                self.logger.debug(f"ZIP contains files: {file_list}")
                
                zip_ref.extractall(extract_dir)
                
                # 调试：查看解压后的文件
                extracted_files = []
                for root, dirs, files in os.walk(extract_dir):
                    for file in files:
                        if file.endswith('.csv'):
                            extracted_files.append(os.path.join(root, file))
                
                self.logger.debug(f"Extracted CSV files: {extracted_files}")
                
            return True
        except zipfile.BadZipFile:
            self.logger.error(f"Corrupted ZIP file: {zip_path}")
            return False
        except Exception as e:
            self.logger.error(f"Failed to extract {zip_path}: {e}")
            return False
    
    def close(self):
        """
        Close the session
        """
        self.session.close()