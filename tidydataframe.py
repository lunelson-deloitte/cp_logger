import functools

from attrs import define, field
from loguru import logger

import pandas as pd


@define
class TidyDataFrame:
    data: pd.DataFrame = field(default=None)

    def _tdf_controller(row_count: bool = False, message: str = "count toggled off"):
        def decorator(func):
            @functools.wraps(func)
            def wrapper(self, *args, **kwargs):
                if hasattr(self, func.__name__):
                    result = func(self, *args, **kwargs)
                    self._log_operation(operation=func.__name__, message=message)
                    return result
            return wrapper
        return decorator


    def _log_operation(self, operation, message, level='info'):
        getattr(logger, level)(f"#> {operation}: {message}")


    @_tdf_controller(
        row_count=True,
        message="scaled values by {other}"
    )
    def mul(self, other, axis='columns', level=None, fill_value=None):
        data = self.data.mul(other, axis=axis, level=level, fill_value=fill_value)
        return TidyDataFrame(data=data)
    

    @_tdf_controller(
        row_count=True,
        message="removed {n_post} rows, {n_result} remaining"
    )
    def filter(self, items=None, like=None, regex=None, axis=None):
        data = self.data.filter(items=items, like=like, regex=regex, axis=axis)
        return TidyDataFrame(data=data)


    def __getattr__(self, attr):
        if hasattr(self.data, attr):
            def wrapper(*args, **kwargs):
                result = getattr(self.data, attr)(*args, **kwargs)
                if isinstance(result, pd.DataFrame):
                    result = TidyDataFrame(data=result)
                    self._log_operation(operation=attr, message="not yet implemented", level="warning")
                    return result
                else:
                    return result
            return wrapper
        ### TODO: validate if this logging operation is legit
        # self._log_operation(operation=attr, message="method does not exist", level="error")
        raise AttributeError(f"'{type(self.data).__name__}' object has no attribute '{attr}'")
    