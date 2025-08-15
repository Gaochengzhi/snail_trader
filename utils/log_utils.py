"""
Log utilities: Centralized logging and console output management

Features:
- File-based logging with rotation
- Real-time console output via background thread
- Singleton pattern for global access
"""

import asyncio
import threading
import logging
import os
from datetime import datetime
from typing import Dict, Any, Set, Optional
from queue import Queue, Empty
import sys
from logging.handlers import RotatingFileHandler

class LogUtils:
    """
    Centralized logging service that collects and displays logs from all services.

    Uses a background thread for console output to avoid blocking async operations.
    """

    _instance = None
    _lock = threading.Lock()

    def __new__(cls, config: Dict[str, Any] = None):
        """Singleton pattern to ensure only one LogService instance"""
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
            return cls._instance

    def __init__(self, config: Dict[str, Any] = None):
        if hasattr(self, "_initialized"):
            return

        self.name = "log_utils"
        self.config = config or {}

        # Configuration - 使用点号访问，符合CLAUDE.md规范
        log_config = getattr(self.config, "log_service", None)
        if log_config:
            self.log_directory = getattr(log_config, "directory", "logs")
            self.console_output = getattr(log_config, "console_output", True)
            self.file_output = getattr(log_config, "file_output", True)
            self.max_file_size = getattr(log_config, "max_file_size_mb", 10) * 1024 * 1024
        else:
            self.log_directory = "logs"
            self.console_output = True
            self.file_output = True
            self.max_file_size = 10 * 1024 * 1024
        if log_config:
            self.backup_count = getattr(log_config, "backup_count", 5)
        else:
            self.backup_count = 5

        # Thread-safe queue for log messages
        self.log_queue = Queue()
        self.console_thread = None
        self.thread_running = False

        # Single file logger
        self.file_logger: Optional[logging.Logger] = None
        self._setup_file_logger()

        self._initialized = True

    def _setup_file_logger(self):
        """Setup single file logger."""
        if not self.file_output:
            return

        os.makedirs(self.log_directory, exist_ok=True)

        self.file_logger = logging.getLogger("snail_trader_file")
        self.file_logger.setLevel(logging.DEBUG)

        # Remove existing handlers to avoid duplication
        self.file_logger.handlers = []

        # Rotating file handler
        handler = RotatingFileHandler(
            f"{self.log_directory}/snail_trader.log",
            maxBytes=self.max_file_size,
            backupCount=self.backup_count,
        )

        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        handler.setFormatter(formatter)
        self.file_logger.addHandler(handler)

    async def initialize(self):
        """Initialize log utils"""
        # Start console output thread
        self.thread_running = True
        self.console_thread = threading.Thread(target=self._console_worker, daemon=True)
        self.console_thread.start()

    def _console_worker(self):
        """Background thread worker for console output"""
        while self.thread_running:
            try:
                # Get log message from queue with timeout
                message = self.log_queue.get(timeout=0.1)

                # Print to console immediately
                print(message, flush=True)

                self.log_queue.task_done()

            except Empty:
                continue
            except Exception as e:
                # Print error directly to avoid recursion
                print(f"LogService error: {e}", file=sys.stderr, flush=True)
                break  # Exit thread on error to avoid infinite loop

    def log_message(self, service_name: str, level: str, message: str):
        """
        Thread-safe method to log a message

        Args:
            service_name: Name of the service sending the log
            level: Log level (INFO, DEBUG, ERROR, etc.)
            message: The log message
        """
        timestamp = datetime.now().strftime("%H:%M:%S")
        formatted_message = f"[{timestamp}] {service_name}: {message}"

        # Console output
        if self.console_output:
            try:
                self.log_queue.put_nowait(formatted_message)
            except:
                # If queue is full, print directly to avoid blocking
                print(formatted_message, flush=True)

        # File output
        if self.file_output and self.file_logger:
            log_method = getattr(self.file_logger, level.lower(), None)
            if log_method:
                log_method(f"{service_name}: {message}")



    async def cleanup(self):
        """Clean up log service resources"""

        # Stop console thread
        self.thread_running = False

        if self.console_thread and self.console_thread.is_alive():
            self.console_thread.join(timeout=2)

        # Drain remaining messages
        while not self.log_queue.empty():
            try:
                message = self.log_queue.get_nowait()
                print(message, flush=True)
            except Empty:
                break



# Global singleton instance
_log_utils_instance = None


def get_log_utils(config: Dict[str, Any] = None) -> LogUtils:
    """Get or create the singleton LogUtils instance"""
    global _log_utils_instance
    if _log_utils_instance is None:
        _log_utils_instance = LogUtils(config)
    return _log_utils_instance
