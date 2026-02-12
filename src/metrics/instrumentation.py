import time

def mark_submit():
    return time.time()

def mark_start():
    return time.time()

def mark_end():
    return time.time()

def duration(start, end):
    return end - start

def sched_overhead(submit, start):
    return (start - submit) * 1000  # ms
