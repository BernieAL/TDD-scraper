import logging
import time
from functools import wraps

class ErrorHandler:
    """Handles errors and retries for scraper operations."""

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

    def retry_on_failure(self, func):
        """
        Decorator that implements retry logic for functions.
        
        Args:
            func: Function to wrap with retry logic
            
        Returns:
            wrapper: Wrapped function with retry logic
        """
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(self.max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    self.logger.warning(f"Attempt {attempt + 1} failed: {str(e)}")
                    if attempt < self.max_retries - 1:
                        time.sleep(self.retry_delay)
            
            raise last_exception
        return wrapper

    def handle_error(self, error: Exception, operation: str, source: str):
        """
        Handle an error by logging it appropriately.
        
        Args:
            error (Exception): The error that occurred
            operation (str): The operation that failed
            source (str): The source where the error occurred
        """
        self.logger.error(f"Error in {source} during {operation}: {str(error)}")

    def handle_empty_data(self, source: str):
        """
        Handle the case where no data was extracted.
        
        Args:
            source (str): The source that produced no data
        """
        self.logger.error(f"No data extracted from {source}")

    def validate_data(self, data: list, required_fields: list) -> bool:
        """
        Validate that the extracted data contains all required fields.
        
        Args:
            data (list): List of dictionaries containing scraped data
            required_fields (list): List of required field names
            
        Returns:
            bool: True if data is valid, False otherwise
        """
        if not data:
            return False
            
        for item in data:
            if not all(field in item for field in required_fields):
                return False
                
        return True 