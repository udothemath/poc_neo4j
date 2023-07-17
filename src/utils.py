import math
import random
import csv
import time
import functools
import logging
import os

 
def create_logger(log_filename: str, log_folder: str, dir_path: str, turn_on_console=True):
    ## config
    logging.captureWarnings(True)  # 捕捉 py waring message
    ## formatter = logging.Formatter("%(asctime)s  %(message)s")
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s", "%Y-%m-%d %H:%M:%S")
    my_logger = logging.getLogger("py.warnings")  # 捕捉 py waring message
    my_logger.setLevel(logging.INFO)

    ## mkdir
    if not os.path.exists(dir_path + log_folder):
        os.makedirs(dir_path + log_folder)

    ## file handler
    fileHandler = logging.FileHandler(
        dir_path + log_folder + "/" + log_filename, "a+", "utf-8"
    )
    fileHandler.setFormatter(formatter)
    my_logger.addHandler(fileHandler)

    ## console handler
    if turn_on_console:
        consoleHandler = logging.StreamHandler()
        consoleHandler.setLevel(logging.DEBUG)
        consoleHandler.setFormatter(formatter)
        my_logger.addHandler(consoleHandler)
    my_logger.info("You have created a customized logger.")
    return my_logger


def log_info(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        execution_time = end_time - start_time
        logger = logging.getLogger(__name__)
        logger.info(f"!!!Function '{func.__qualname__}' executed in {execution_time:.4f} seconds.")
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

            logger.info(f"Function '{func.__qualname__}' executed in {execution_time:.4f} seconds.")
            return result
        return wrapper
    return decorator

def create_csv_file(file_info):
    file_prefix = file_info.file_prefix 
    num_rows = file_info.num_rows 
    type_size = file_info.type_size
    file_path = file_info.file_path
    # Define column names
    fieldnames = ['from', 'to', 'from_type', 'to_type', 'relation']
    rand_size = int(num_rows/3)
    paddle_zero_size = int(math.log(num_rows, 10))+1

    relation_type_list  =  [f'type_{i:02}' for i in range(1,  type_size+1) ]
    # Generate data for each row
    rows = []
    for i in range(num_rows):
        rand_value_from  = random.randint(1, rand_size)
        rand_value_to  = random.randint(1, rand_size)

        from_info = random.choice([('a', 'account'), 
                                    ('c', 'company'), 
                                    ('u', 'user')])        
        to_info = random.choice([('a', 'account'), 
                                    ('c', 'company'), 
                                    ('u', 'user')])          

        relation = random.choice(relation_type_list)
        rows.append({'from': f'{from_info[0]}_{rand_value_from:0{paddle_zero_size}}', 
                     'to': f'{to_info[0]}_{rand_value_to:0{paddle_zero_size}}', 
                     'from_type': from_info[1], 
                     'to_type': to_info[1], 
                     'relation': relation})
    
    # Write data to CSV file
    with open(file_info.filename_path, mode='w', newline='') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
