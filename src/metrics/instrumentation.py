import time

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
