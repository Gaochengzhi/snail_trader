import logging
import os
from logging.handlers import RotatingFileHandler
from datetime import datetime

# Global logger instance to prevent duplicate initialization
_logger_initialized = False
_logger_instance = None


def setup_logger(config):
    """
    Setup logging configuration based on config settings (singleton pattern)
    """
    global _logger_initialized, _logger_instance
    
    # If logger already initialized, return existing instance
    if _logger_initialized and _logger_instance:
        return _logger_instance
    
    # Create logs directory if it doesn't exist
    log_dir = config.get('log_directory', './logs')
    os.makedirs(log_dir, exist_ok=True)
    
    # Get logging configuration
    logging_config = config.get('logging', {})
    log_level = getattr(logging, logging_config.get('level', 'INFO').upper())
    console_output = logging_config.get('console_output', True)
    file_output = logging_config.get('file_output', True)
    max_file_size = logging_config.get('max_log_file_size', '10MB')
    backup_count = logging_config.get('backup_count', 5)
    
    # Convert max_file_size to bytes
    size_multiplier = {'KB': 1024, 'MB': 1024*1024, 'GB': 1024*1024*1024}
    size_unit = max_file_size[-2:].upper()
    size_value = int(max_file_size[:-2])
    max_bytes = size_value * size_multiplier.get(size_unit, 1024*1024)
    
    # Create logger
    logger = logging.getLogger('binance_downloader')
    logger.setLevel(log_level)
    
    # Clear existing handlers to prevent duplicates
    logger.handlers.clear()
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Console handler - only show INFO and above, suppress WARNING
    if console_output:
        console_handler = logging.StreamHandler()
        # Set console to only show ERROR and CRITICAL (suppress WARNING)
        console_handler.setLevel(logging.ERROR)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
        # Separate console handler for INFO messages only
        info_console_handler = logging.StreamHandler()
        info_console_handler.setLevel(logging.INFO)
        info_console_handler.addFilter(lambda record: record.levelno == logging.INFO)
        info_console_handler.setFormatter(formatter)
        logger.addHandler(info_console_handler)
    
    # Main log file handler with rotation (fixed filename)
    if file_output:
        log_filename = os.path.join(log_dir, 'binance_downloader.log')
        file_handler = RotatingFileHandler(
            log_filename,
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding='utf-8'
        )
        file_handler.setLevel(log_level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        
        # Separate error log handler (fixed filename)
        error_log_filename = os.path.join(log_dir, 'binance_downloader_errors.log')
        error_handler = RotatingFileHandler(
            error_log_filename,
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding='utf-8'
        )
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(formatter)
        logger.addHandler(error_handler)
    
    # Mark as initialized and store instance
    _logger_initialized = True
    _logger_instance = logger
    
    return logger


def get_logger():
    """
    Get the configured logger instance
    """
    global _logger_instance
    if _logger_instance:
        return _logger_instance
    else:
        # If not initialized, return a basic logger
        return logging.getLogger('binance_downloader')


def reset_logger():
    """
    Reset logger for testing purposes
    """
    global _logger_initialized, _logger_instance
    _logger_initialized = False
    _logger_instance = None