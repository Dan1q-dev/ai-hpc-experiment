import time
from functools import wraps
from typing import Any, Callable, Optional, TypeVar, cast

F = TypeVar("F", bound=Callable[..., Any])


def timed(metric_name: Optional[str] = None):
    def decorator(func: F) -> F:
        @wraps(func)
        def wrapper(*args, **kwargs):
            started = time.perf_counter()
            try:
                return func(*args, **kwargs)
            finally:
                wrapper.last_duration_s = time.perf_counter() - started

        wrapper.metric_name = metric_name or func.__name__
        wrapper.last_duration_s = 0.0
        return cast(F, wrapper)

    return decorator


def get_last_duration_s(func: Callable[..., Any]) -> float:
    return float(getattr(func, "last_duration_s", 0.0))

def mark_submit():
    return time.perf_counter()

def mark_start():
    return time.perf_counter()

def mark_end():
    return time.perf_counter()

def duration(start, end):
    return end - start

def sched_overhead(submit, start):
    return (start - submit) * 1000  # ms
