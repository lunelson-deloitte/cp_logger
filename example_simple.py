import functools
import time
import sys
from pprint import pprint

# PATH_LOG_FILE = 'path/to/log.log'
# logger.add(PATH_LOG_FILE, format="{time:HH:mm:ss.SS zz} | {level:<8} | {message}")

@cp_log()
def read_data(path):
    return f'Loaded dataframe from {path}'

@cp_log(log_type='dataframe', perf=False)
def clean_data(data):
    return "Cleansed dataset"

@cp_log(log_type='write', perf=True, eval=False)
def very_long_process():
    time.sleep(11.0799)
    return "Processed value"

@cp_log(log_type='default', msg='Separate strings by commas')
def custom_function(*args):
    return ', '.join(args)

@cp_log(log_type='default', eval=False)
def skip_process():
    return "This value will never return because `eval = False`"

@cp_log(log_type='default', eval=False)
def invalid_process():
    value = 2 + 's' + ValueError('cursed code')
    return "Skips invalid code too"

# pprint({
#     'load_data() returns' : load_data(path='random/file/path.csv'),
#     'clean_data() returns' : clean_data(data='random dataset'),
#     'very_long_process() returns' : very_long_process(),
#     'custom_function() returns' : custom_function('this', 'is', 'a', 'demonstration'),
#     'skip_process() returns' : skip_process(),
#     'invalid_process() returns' : invalid_process(),
# })
