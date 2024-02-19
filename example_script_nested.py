import sys
import time
import random
from datetime import datetime
from loguru import logger
from cp_log import cp_log, cp_init_log, cp_summarize_log


def random_time(scale=5):
    return random.random() * scale

@cp_log(message = "Universal prep for {data_name}. Input: {input_path}. Output: {output_path}.")
def prep_dataset(data_name, input_path, output_path, custom_func=False):
    
    @cp_log(message = "Loading in data from '{path}'")
    def load_data(path):
        time.sleep(random_time())
        return None


    @cp_log(message = "Cleaning {data} ...", eval=False)
    def clean_data(data):
        time.sleep(random_time())
        return None


    @cp_log(message = "Removing AR transaction types", eval=False)
    def custom_function(data):
        time.sleep(random_time(scale=20))
        return None


    @cp_log(message = "Writing `{data}` to `{path}`")
    def write_data(data, path):
        time.sleep(random_time())
        return None
    
    raw_data = load_data(path=input_path)
    cleansed_data = clean_data(data=raw_data)
    if custom_func:
        cleansed_data = custom_function(data=cleansed_data)
    write_data(data=cleansed_data, path=output_path)



LOG_FILE = 'nested_file.log'
LOG_FORMAT = "{time:HH:mm:ss.SS zz} | {level:<8} | {message}"
config = {
    "handlers": [
        {"sink": sys.stdout, "format":LOG_FORMAT},
        {"sink": LOG_FILE, 'format':LOG_FORMAT, 'mode':'w'}
    ],
    "extra": {
        "Analytic": "Journal Entry Testing",
        "Reviewer": "First Last",
        "Preparer": "First Last"
    }
}
logger.configure(**config)
cp_init_log(config['extra'])

prep_dataset(data_name='JE', input_path='path/to/je.csv', output_path='path/to/output/je.parquet', custom_func=True)
prep_dataset(data_name='TB', input_path='path/to/tb.csv', output_path='path/to/output/tb.parquet')
prep_dataset(data_name='COA', input_path='path/to/coa.csv', output_path='path/to/output/coa.parquet')


LOG_DELIM = r"\s*\|\s*"
LOG_CASTING_FUNCTIONS = {f:lambda x: x.strip() for f in ('time', 'level', 'message')}
LOG_PATTERN = f"(?P<time>.*){LOG_DELIM}(?P<level>.*){LOG_DELIM}\((?P<function>.*)\)(?P<message>.*)"
parsed_log = list(logger.parse(LOG_FILE, pattern=LOG_PATTERN, cast=LOG_CASTING_FUNCTIONS))
cp_summarize_log(parsed_log)
