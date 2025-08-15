#!/usr/bin/env python3
"""
Task tracking system for resume functionality
"""

import os
import json
import hashlib
import threading
from datetime import datetime
from typing import Dict, Any, List, Optional, Set
from enum import Enum
from logger_setup import get_logger


class TaskStatus(Enum):
    """Task status enumeration"""
    PENDING = "pending"          # 需要下载
    SKIPPED = "skipped"          # 已跳过（文件已存在且完整）
    COMPLETED = "completed"      # 下载完成
    FAILED = "failed"           # 下载失败


class TaskTracker:
    """
    Track download task status with persistent storage
    """
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = get_logger()
        
        # Thread lock for file operations
        self._file_lock = threading.Lock()
        
        # Generate unique tracking file name based on config
        self.tracking_file = self._get_tracking_file_path()
        
        # Load existing task status
        self.task_status: Dict[str, Dict[str, Any]] = self._load_task_status()
        
        # Statistics
        self.stats = {
            TaskStatus.PENDING: 0,
            TaskStatus.SKIPPED: 0,
            TaskStatus.COMPLETED: 0,
            TaskStatus.FAILED: 0
        }
    
    def _get_tracking_file_path(self) -> str:
        """
        Generate simple tracking file path (configuration-independent)
        """
        log_dir = self.config.get('log_directory', './logs')
        os.makedirs(log_dir, exist_ok=True)
        
        # Use a simple filename since we now rely primarily on file existence
        return os.path.join(log_dir, "download_failures.json")
    
    def _load_task_status(self) -> Dict[str, Dict[str, Any]]:
        """
        Load task status from tracking file (only failed tasks)
        """
        if not os.path.exists(self.tracking_file):
            self.logger.info(f"No existing failure tracking file found: {self.tracking_file}")
            return {}
        
        try:
            with open(self.tracking_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                self.logger.info(f"Loaded failure tracking from: {self.tracking_file}")
                return data.get('failed_tasks', {})
        except Exception as e:
            self.logger.warning(f"Failed to load failure tracking file: {e}")
            return {}
    
    def _save_task_status(self):
        """
        Save task status to tracking file (thread-safe) - only failed tasks matter
        """
        with self._file_lock:
            try:
                # Create a snapshot of the dictionary to avoid concurrent modification
                task_status_snapshot = dict(self.task_status)
                
                # Only save failed tasks since we primarily rely on file existence
                failed_tasks = {
                    key: task_info for key, task_info in task_status_snapshot.items()
                    if task_info.get('status') == TaskStatus.FAILED.value
                }
                
                data = {
                    'last_updated': datetime.now().isoformat(),
                    'failed_tasks': failed_tasks,
                    'note': 'This file tracks download failures for retry. Completed files are detected by file existence.'
                }
                
                with open(self.tracking_file, 'w', encoding='utf-8') as f:
                    json.dump(data, f, indent=2, ensure_ascii=False)
                    
            except Exception as e:
                self.logger.error(f"Failed to save task tracking file: {e}")
    
    def _generate_task_key(self, symbol: str, data_type: str, date: str, interval: str = None) -> str:
        """
        Generate unique task key for a file download task
        """
        if interval:
            return f"{symbol}_{data_type}_{interval}_{date}"
        else:
            return f"{symbol}_{data_type}_{date}"
    
    def get_task_status(self, symbol: str, data_type: str, date: str, interval: str = None) -> TaskStatus:
        """
        Get status of a specific task
        """
        task_key = self._generate_task_key(symbol, data_type, date, interval)
        task_info = self.task_status.get(task_key, {})
        status_str = task_info.get('status', TaskStatus.PENDING.value)
        
        try:
            return TaskStatus(status_str)
        except ValueError:
            return TaskStatus.PENDING
    
    def set_task_status(self, symbol: str, data_type: str, date: str, 
                       status: TaskStatus, interval: str = None, 
                       error_msg: str = None, file_size: int = None):
        """
        Set status of a specific task (in-memory only, no immediate disk write)
        """
        task_key = self._generate_task_key(symbol, data_type, date, interval)
        
        task_info = {
            'symbol': symbol,
            'data_type': data_type,  
            'date': date,
            'interval': interval,
            'status': status.value,
            'last_updated': datetime.now().isoformat()
        }
        
        if error_msg:
            task_info['error_msg'] = error_msg
        if file_size is not None:
            task_info['file_size'] = file_size
            
        self.task_status[task_key] = task_info
        # Note: No automatic save - call save_progress() manually when needed
    
    def get_pending_tasks(self) -> List[Dict[str, Any]]:
        """
        Get list of tasks that need to be processed (PENDING or FAILED)
        """
        pending_tasks = []
        
        # Create snapshot to avoid concurrent modification
        task_status_snapshot = dict(self.task_status)
        for task_key, task_info in task_status_snapshot.items():
            status = TaskStatus(task_info.get('status', TaskStatus.PENDING.value))
            if status in [TaskStatus.PENDING, TaskStatus.FAILED]:
                pending_tasks.append(task_info)
        
        return pending_tasks
    
    def update_statistics(self):
        """
        Update task statistics
        """
        self.stats = {status: 0 for status in TaskStatus}
        
        # Create snapshot to avoid concurrent modification
        task_status_snapshot = dict(self.task_status)
        for task_info in task_status_snapshot.values():
            status = TaskStatus(task_info.get('status', TaskStatus.PENDING.value))
            self.stats[status] += 1
    
    def get_statistics(self) -> Dict[TaskStatus, int]:
        """
        Get current task statistics
        """
        self.update_statistics()
        return self.stats.copy()
    
    def save_progress(self):
        """
        Save current progress to file
        """
        self._save_task_status()
    
    def reset_failed_tasks(self):
        """
        Reset all failed tasks to pending for retry
        """
        reset_count = 0
        # Create snapshot to avoid concurrent modification
        task_status_snapshot = dict(self.task_status)
        
        for task_key, task_info in task_status_snapshot.items():
            if task_info.get('status') == TaskStatus.FAILED.value:
                # Update the original dictionary
                self.task_status[task_key]['status'] = TaskStatus.PENDING.value
                self.task_status[task_key]['last_updated'] = datetime.now().isoformat()
                if 'error_msg' in self.task_status[task_key]:
                    del self.task_status[task_key]['error_msg']
                reset_count += 1
        
        if reset_count > 0:
            self.logger.info(f"Reset {reset_count} failed tasks to pending")
            self._save_task_status()
    
    def print_summary(self):
        """
        Print task status summary
        """
        stats = self.get_statistics()
        total = sum(stats.values())
        
        if total > 0:
            self.logger.info("=== Task Status Summary ===")
            self.logger.info(f"Total tasks: {total}")
            self.logger.info(f"Pending: {stats[TaskStatus.PENDING]} ({stats[TaskStatus.PENDING]/total*100:.1f}%)")
            self.logger.info(f"Skipped: {stats[TaskStatus.SKIPPED]} ({stats[TaskStatus.SKIPPED]/total*100:.1f}%)")  
            self.logger.info(f"Completed: {stats[TaskStatus.COMPLETED]} ({stats[TaskStatus.COMPLETED]/total*100:.1f}%)")
            self.logger.info(f"Failed: {stats[TaskStatus.FAILED]} ({stats[TaskStatus.FAILED]/total*100:.1f}%)")
            self.logger.info(f"Tracking file: {self.tracking_file}")