"""
utilities to log runtimes for functions to a file for later analysis
"""
from functools import wraps
import csv
import time
import os
import threading

class BufferedCsvLogger:
    """Buffered, thread-safe logger for function calls."""
    def __init__(self, filename='function_log.csv', buffer_size=10):
        self.filename = filename
        self.buffer_size = buffer_size
        self.buffer = []
        self.lock = threading.Lock()

    def log(self, func_name, elapsed_time, iteration, flush_on_return):
        """Log function metrics."""
        with self.lock:
            self.buffer.append({'function_name': func_name, 'elapsed_time': elapsed_time, 'iteration': iteration})
            
            if len(self.buffer) >= self.buffer_size or flush_on_return:
                self.flush()

    def flush(self):
        """Flush the buffer to disk."""
        file_exists = os.path.exists(self.filename)
        with open(self.filename, 'a', newline='') as csv_file:
            fieldnames = ['function_name', 'elapsed_time', 'iteration']
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            
            if not file_exists:
                writer.writeheader()
            
            writer.writerows(self.buffer)
            self.buffer.clear()

def log_to_csv(filename='function_log.csv', buffer_size=10, iteration=0):
    """Decorator factory for logging function metrics."""
    logger = BufferedCsvLogger(filename, buffer_size)
    
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = func(*args, **kwargs)
            finally:
                elapsed_time = time.time() - start_time
                logger.log(func.__name__, elapsed_time, iteration, flush_on_return=True)
            return result
        return wrapper
    return decorator
