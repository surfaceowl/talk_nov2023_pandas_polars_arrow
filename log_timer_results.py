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

    def __init__(self, log_filename="function_log.csv", buffer_size=10):
        self.filename = log_filename
        self.buffer_size = buffer_size
        self.buffer = []
        self.lock = threading.Lock()

    def log(self, func_name, elapsed_time, iteration, flush_on_return):
        """Log function metrics."""
        with self.lock:
            print(f"saving results: {func_name}")
            self.buffer.append(
                {
                    "function_name": func_name,
                    "elapsed_time": elapsed_time,
                    "iteration": iteration,
                }
            )

            if len(self.buffer) >= self.buffer_size or flush_on_return:
                self.flush()

    def flush(self):
        """Flush the buffer to disk."""
        file_exists = os.path.exists(self.filename)
        with open(self.filename, "a", newline="") as csv_file:
            fieldnames = ["function_name", "elapsed_time", "iteration"]
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

            if not file_exists:
                writer.writeheader()

            writer.writerows(self.buffer)
            self.buffer.clear()


def log_to_csv(log_filename="function_log.csv", buffer_size=10, flush_on_return=True):
    """Decorator factory for logging function metrics."""
    logger = BufferedCsvLogger(log_filename, buffer_size)

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            iteration = kwargs.pop(
                "iteration", 0
            )  # Get the iteration and remove it from kwargs FIRST

            start_time = time.time()
            try:
                result = func(*args, **kwargs)
            finally:
                elapsed_time = time.time() - start_time
                logger.log(func.__name__, elapsed_time, iteration, flush_on_return=True)
            return result

        return wrapper

    return decorator
