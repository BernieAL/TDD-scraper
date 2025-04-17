import logging
import time
from typing import Optional, Callable, Any
from functools import wraps

class ErrorHandler:
    """Centralized error handling system for scrapers."""
    
    def __init__(self, max_retries: int = 3, retry_delay: int = 5):
        """
        Initialize the error handler.
        
        Args:
            max_retries (int): Maximum number of retry attempts
            retry_delay (int): Delay between retries in seconds
        """
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.logger = logging.getLogger(__name__)

    def handle_error(self, error: Exception, context: str, source: str) -> None:
        """
        Handle and log errors consistently.
        
        Args:
            error (Exception): The exception that occurred
            context (str): Context where the error occurred
            source (str): Source of the error (e.g., scraper name)
        """
        error_msg = f"Error in {context} for {source}: {str(error)}"
        self.logger.error(error_msg, exc_info=True)

    def retry_on_failure(self, func: Callable) -> Callable:
        """
        Decorator to retry a function on failure.
        
        Args:
            func (Callable): Function to retry
            
        Returns:
            Callable: Wrapped function with retry logic
        """
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            last_exception = None
            for attempt in range(self.max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt < self.max_retries - 1:
                        self.logger.warning(
                            f"Attempt {attempt + 1} failed. Retrying in {self.retry_delay} seconds..."
                        )
                        time.sleep(self.retry_delay)
                    else:
                        self.logger.error(
                            f"All {self.max_retries} attempts failed. Giving up."
                        )
                        raise last_exception
            return None
        return wrapper

    def validate_data(self, data: list, required_fields: list) -> bool:
        """
        Validate scraped data.
        
        Args:
            data (list): Data to validate
            required_fields (list): List of required fields
            
        Returns:
            bool: True if data is valid, False otherwise
        """
        if not data:
            return False
            
        for item in data:
            if not all(item.get(field) for field in required_fields):
                return False
                
        return True

    def handle_empty_data(self, source: str) -> None:
        """
        Handle cases where no data was extracted.
        
        Args:
            source (str): Source of the error
        """
        error_msg = "No valid data extracted from listings"
        self.handle_error(Exception(error_msg), "data extraction", source) 