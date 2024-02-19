from typing import Optional
import time
from datetime import datetime
import dateutil
import functools

from attrs import define, field
from loguru import logger


def cp_init_log(config_dict):
    sep='='*50
    logger.info(sep)
    logger.info(f"Date     : {datetime.now().strftime('%B %d, %Y')}")
    logger.info(f"Analytic : {config_dict['Analytic'] if config_dict['Analytic'] is not None else 'Analytic Unknown'}")
    logger.info(f"Reviewer : {config_dict['Reviewer'] if config_dict['Reviewer'] else 'Reviewer Unknown'}")
    logger.info(f"Preparer : {config_dict['Preparer'] if config_dict['Preparer'] else 'Preparer Unknown'}")
    logger.info(sep)


def cp_summarize_log(parsed_log, *args):
    def count_levels(parsed_log, level):
        return sum(map(lambda log: log['level'] == level, parsed_log))

    runtime = cp_timer(
        start=dateutil.parser.parse(parsed_log[0]['time']),
        end=dateutil.parser.parse(parsed_log[-1]['time'])
    )

    level_summary = map(
        lambda level: f"{level.upper()} {count_levels(parsed_log, level)}",
        ('ERROR', 'WARNING', 'SUCCESS')
    )

    sep='='*50
    logger.info(sep)
    logger.info(f"Script completed in {runtime}")
    logger.info('\t'.join(level_summary))
    # allow user to pass notes to log
    logger.info(sep)

    


def cp_log(message=None, eval=True, perf=True, parallel=False, before=None, after=None):
    def decorator(func):
        if not eval:
            return cp_decorator_skip(func, message=message)
        else:
            return cp_decorator_custom(func, message=message, perf=perf)
    return decorator


def cp_decorator_skip(func, message):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        logger.warning(f"({func.__name__}) Skipped {message}")
    return wrapper


def cp_decorator_custom(func, message, perf):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        logger.debug(f"({func.__name__}) {message.format(**kwargs) if message is not None else 'Starting ...'}")
        time_start = time.perf_counter() if perf else 0
        try:
            result = func(*args, **kwargs)
        except Exception as e:
            logger.error(f"({func.__name__}) Error! {e.__class__.__name__}")
            raise Exception(e)
        time_end = time.perf_counter() if perf else 0
        logger.success(f"({func.__name__}) Completed ({cp_timer(time_start, time_end)})")
        return result
    return wrapper



def cp_timer(start=0, end=0):
    def runtime(start, end):
        if not isinstance(start, type(end)):
            raise ValueError('Type mismatch!')
        if isinstance(start, float):
            return end - start
        if isinstance(start, datetime):
            return (end - start).total_seconds()
    def format(runtime, threshold=0.01):
        if runtime < threshold:
            return ""
        return f"{runtime:.2f}s"
    return format(runtime(start, end))


@define
class CortexPyLogger:
    name: str = field(default='Custom')
    message: str = field(default='Custom function')
    on_error: Optional[str] = field(default=None)
    validators: Optional[tuple[callable]] = field(default=None)


# cp_log_custom = CortexPyLogger(name='Custom', msg=('start'='Custom function in progress', 'end'='Custom process complete'))