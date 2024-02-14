import pandas as pd
import random

from cp_logger import cp_log


N_ROWS = random.randint(10e7, 10e10)
N_ACCOUNTS = random.randint(1, 10)
N_TRANSACTION_TYPES = random.randint(1, 10)

@cp_log(log_type='default', msg='Generating sample data')
def generate_sample_data(n_rows, n_accounts, n_transaction_types) -> pd.DataFrame:
    sample_data = pd.DataFrame({
        'Account' : [f'Acct {random.randint(1, n_accounts)}' for _ in range(n_rows)],
        'Transaction Type' : [f'TT {random.randint(1, n_transaction_types)}' for _ in range(n_rows)],
        'Source' : [random.choice(['Good', 'Bad']) for _ in range(n_rows)],
        'Amount' : [random.random() * random.randint(-10e3, 10e3) for _ in range(n_rows)]
    })
    return sample_data

@cp_log(log_type='read')
def read_data(data):
    return data

@cp_log(log_type='dataframe')
def filter_source_column(data):
    return data[data['Source'] == 'Good']

@cp_log(log_type='dataframe')
def summarize_transaction_type(data):
    return data.groupby('Transaction Type').sum()

@cp_log(log_type='reconcile')
def reconcile_data(data):
    return data.groupby('Account').sum()

@cp_log(log_type='write', perf=False, eval=False)
def write_data(data: pd.DataFrame, path: str) -> None:
    # ... write data ...
    print(f'Writing file to {path}')
    return None
