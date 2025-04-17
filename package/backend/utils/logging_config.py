"""
Centralized logging configuration for the application.
"""

import logging
import os
from datetime import datetime
from typing import Optional

# Log format with timestamp, level, module, and message
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

def setup_logging(
    log_level: int = logging.INFO,
    log_file: Optional[str] = None,
    log_format: str = LOG_FORMAT,
    date_format: str = DATE_FORMAT
) -> None:
    """
    Set up logging configuration for the application.
    
    Args:
        log_level: The logging level (default: INFO)
        log_file: Optional path to log file. If None, logs only to console
        log_format: Format string for log messages
        date_format: Format string for timestamps
    """
    # Create logs directory if it doesn't exist
    if log_file:
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
    
    # Create formatter
    formatter = logging.Formatter(fmt=log_format, datefmt=date_format)
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    
    # Clear any existing handlers
    root_logger.handlers.clear()
    
    # Add console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)
    
    # Add file handler if log file is specified
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)
    
    # Set up AWS Lambda logger
    lambda_logger = logging.getLogger("lambda")
    lambda_logger.setLevel(log_level)
    
    # Set up scraper logger
    scraper_logger = logging.getLogger("scraper")
    scraper_logger.setLevel(log_level)
    
    # Set up analysis logger
    analysis_logger = logging.getLogger("analysis")
    analysis_logger.setLevel(log_level)
    
    # Set up report logger
    report_logger = logging.getLogger("report")
    report_logger.setLevel(log_level)

def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance with the given name.
    
    Args:
        name: Name of the logger
        
    Returns:
        logging.Logger: Configured logger instance
    """
    return logging.getLogger(name) 