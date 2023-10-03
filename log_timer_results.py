from functools import wraps
import csv
import time
import os
import threading

class BufferedCsvLogger:
    """Buffered logger for function calls."""
    def __init__(self, filename='function_log.csv', buffer_size=10):
        self.filename = filename
        self.buffer_size = buffer_size
        self.buffer = []
        self.lock = threading.Lock()

    def log(self, func_name, elapsed_time):
        """Log function execution time and name."""
        with self.lock:
            self.buffer.append({'function_name': func_name, 'elapsed_time': elapsed_time})
            if len(self.buffer) >= self.buffer_size:
                self.flush()

    def flush(self):
        """Flush the buffer to disk."""
        file_exists = os.path.isfile(self.filename)
        with open(self.filename, mode='a', newline='') as csv_file:
            fieldnames = ['function_name', 'elapsed_time']
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            if not file_exists:
                writer.writeheader()
            writer.writerows(self.buffer)
        self.buffer = []

def log_to_csv(filename='function_log.csv', buffer_size=10, flush_on_return=True):
    """Decorator factory for logging function metrics."""
    logger = BufferedCsvLogger(filename, buffer_size)

    def decorator(func):
        """Actual decorator function."""
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            result = func(*args, **kwargs)
            end_time = time.time()
            elapsed_time = end_time - start_time
            logger.log(func.__name__, elapsed_time)
            
            if flush_on_return:
                logger.flush()
            
            return result
        return wrapper
    return decorator
