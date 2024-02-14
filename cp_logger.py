import time
from loguru import logger


CP_LOG_TYPES = ('read', 'dataframe', 'write', 'default', 'reconcile')
def cp_log(log_type, eval=True, perf=True, msg=None):
    if log_type not in CP_LOG_TYPES:
        raise ValueError(f"Unrecognized `log_type` value: '{log_type}'. Please pass one of: {', '.join(CP_LOG_TYPES)}")
    def inner_decorator(func):
        if not eval:
            return cp_log_skip(func)
        if log_type == 'read':
            return cp_log_read(func, perf=perf)
        if log_type == 'dataframe':
            return cp_log_dataframe(func, perf=perf)
        if log_type == 'reconcile':
            return cp_log_reconcile(func, perf=perf)
        if log_type == 'write':
            return cp_log_write(func, perf=perf)
        if log_type == 'default':
            return cp_log_default(func, perf=perf, msg=msg)
    return inner_decorator


def cp_log_skip(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        logger.warning(f'({func.__name__}) Skipping process')
    return wrapper

def cp_log_read(func, perf):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        logger.info(f"({func.__name__})\tReading file")
        time_start = time.perf_counter() if perf else 0
        result = func(*args, **kwargs)
        time_finish = time.perf_counter() if perf else 0
        logger.success(f"({func.__name__})\tFile loaded{cp_perf(start=time_start, end=time_finish) if perf else ''}")
        return result
    return wrapper

def cp_log_dataframe(func, perf):    
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        logger.info(f"({func.__name__})\tStarting dataframe operation")
        time_start = time.perf_counter() if perf else 0
        result = func(*args, **kwargs)
        time_finish = time.perf_counter() if perf else 0
        logger.info(f'({func.__name__})\tRows: {result.shape[0]} Columns: {result.shape[1]}')
        logger.success(f"({func.__name__})\tProcess Complete{cp_perf(start=time_start, end=time_finish) if perf else ''}")
        return result
    return wrapper

def cp_log_reconcile(func, perf):    
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        logger.info(f"({func.__name__})\tStarting dataframe operation")
        time_start = time.perf_counter() if perf else 0
        result = func(*args, **kwargs)
        time_finish = time.perf_counter() if perf else 0
        logger.info(f'({func.__name__})\t{cp_rec(result)}')
        logger.success(f"({func.__name__})\tProcess Complete{cp_perf(start=time_start, end=time_finish) if perf else ''}")
        return result
    return wrapper

def cp_log_write(func, perf):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        logger.info(f"({func.__name__})\tProcess Starting")
        time_start = time.perf_counter() if perf else 0
        result = func(*args, **kwargs)
        time_finish = time.perf_counter() if perf else 0
        logger.success(f"({func.__name__})\tProcess Complete{cp_perf(start=time_start, end=time_finish) if perf else ''}")
        return result
    return wrapper

def cp_log_default(func, perf, msg):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        logger.info(f"({func.__name__})\t{msg if msg is not None else 'Generic process starting'}")
        time_start = time.perf_counter() if perf else 0
        result = func(*args, **kwargs)
        time_finish = time.perf_counter() if perf else 0
        logger.success(f"({func.__name__})\tProcess Complete{cp_perf(start=time_start, end=time_finish) if perf else ''}")
        return result
    return wrapper

def cp_rec(data):
    def n_reconciled(data, threshold=100):
        return data[data['Amount'] < threshold].shape[0]
    def pct_reconciled(data, n_reconciled):
        return n_reconciled / data.shape[0]
    def format_reconciled(data, n_reconciled):
        recon_msg = f"Reconciled: {n_reconciled:,} ({pct_reconciled(data, n_reconciled) * 100}%)"
        unrecon_msg = f"Unreconciled: {data.shape[0] - n_reconciled:,} ({(1 - pct_reconciled(data, n_reconciled)) * 100}%)"
        return f"{recon_msg} {unrecon_msg}"
    return format_reconciled(data=data, n_reconciled=n_reconciled(data=data, threshold=100))

def cp_perf(start, end):
    def runtime(start=0, end=0):
        return end - start
    def format_runtime(runtime):
        return f" ({runtime:.1f}s)"
    return format_runtime(runtime(start, end))
