import time
from functools import wraps
from typing import Callable, TypeVar, Any

T = TypeVar('T')


def timer(func: Callable[..., T]) -> Callable[..., T]:

    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> T:
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()

        execution_time_ms = (end_time - start_time) * 1000
        print(f"Function '{func.__name__}' executed in {execution_time_ms} ms")

        return result

    return wrapper