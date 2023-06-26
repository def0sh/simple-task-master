from functools import wraps
from time import time


def timing(f):
    @wraps(f)
    def wrap(*args, **kw):
        ts = time()
        result = f(*args, **kw)
        te = time()
        delta = round(te - ts, 2)
        print(f"function {f.__name__} took {delta} sec")
        return result
    return wrap