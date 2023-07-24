import time
import functools
import logging
import os


def create_logger(log_filename: str, log_folder: str, dir_path: str, turn_on_console=True):
    # config
    logging.captureWarnings(True)  # 捕捉 py waring message
    # formatter = logging.Formatter("%(asctime)s  %(message)s")
    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s %(message)s", "%Y-%m-%d %H:%M:%S")
    my_logger = logging.getLogger("py.warnings")  # 捕捉 py waring message
    my_logger.setLevel(logging.INFO)

    # mkdir
    dir_log_path = os.path.join(dir_path, log_folder)
    if not os.path.exists(dir_log_path):
        os.makedirs(dir_log_path)

    file_dir_log_path = os.path.join(dir_log_path, log_filename)
    # file handler
    fileHandler = logging.FileHandler(
        file_dir_log_path, "a+", "utf-8"
    )
    fileHandler.setFormatter(formatter)
    my_logger.addHandler(fileHandler)
    # console handler
    if turn_on_console:
        consoleHandler = logging.StreamHandler()
        consoleHandler.setLevel(logging.DEBUG)
        consoleHandler.setFormatter(formatter)
        my_logger.addHandler(consoleHandler)
    return my_logger


def log_info(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        execution_time = end_time - start_time
        logger = logging.getLogger(__name__)
        logger.info(
            f"!!!Function '{func.__qualname__}' executed in {execution_time:.4f} seconds.")
        return result
    return wrapper


def logger_decorator(logger):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            result = func(*args, **kwargs)
            end_time = time.time()
            execution_time = end_time - start_time

            logger.info(
                f"Function '{func.__qualname__}' executed in {execution_time:.4f} seconds.")
            return result
        return wrapper
    return decorator
