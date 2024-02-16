import sys
import time
import random
from datetime import datetime
from loguru import logger
from cp_log import cp_log, cp_init_log, cp_summarize_log


def random_time(scale=5):
    return random.random() * scale


@cp_log(message = "Loading in data from {path}")
def load_data(path):
    time.sleep(random_time())
    return None


@cp_log(message = "Cleaning data: {data}")
def clean_data(data):
    time.sleep(random_time())
    return None


@cp_log(message = "Removing AR transaction types")
def custom_function(data):
    time.sleep(random_time(scale=20))
    return None


@cp_log(message = "Writing {data} to {path}")
def write_data(data, path):
    time.sleep(random_time())
    return None



LOG_FORMAT = "{time:HH:mm:ss.SS zz} | {level:<8} | {message}"
config = {
    "handlers": [
        {"sink": sys.stdout, "format":LOG_FORMAT},
        {"sink": f"file.log", 'format':LOG_FORMAT, 'mode':'w'}
    ],
    "extra": {
        "Date": datetime.today().strftime('%B %d, %Y'),
        "Analytic": "Journal Entry Testing",
        "Preparer": "Lucas Nelson",
        "Reviewer": "Molly Meehan"
    }
}
logger.configure(**config)
cp_init_log(config['extra'])

je = load_data('path/to/je.csv')
tb = load_data('path/to/tb.csv')
coa = load_data('path/to/coa.csv')

je_clean = clean_data(je)
tb_clean = clean_data(tb)
coa_clean = clean_data(coa)

je_custom = custom_function(je_clean)

write_data(je_custom, 'path/to/output/je.parquet')
write_data(tb_clean, 'path/to/output/tb.parquet')
write_data(coa_clean, 'path/to/output/coa.parquet')


LOG_DELIM = r"\s*\|\s*"
LOG_CASTING_FUNCTIONS = {f:lambda x: x.strip() for f in ('time', 'level', 'message')}
LOG_PATTERN = f"(?P<time>.*){LOG_DELIM}(?P<level>.*){LOG_DELIM}\((?P<function>.*)\)(?P<message>.*)"
parsed_log = list(logger.parse('file.log', pattern=LOG_PATTERN, cast=LOG_CASTING_FUNCTIONS))
cp_summarize_log(parsed_log)
