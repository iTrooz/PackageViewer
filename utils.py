from time import time

def timer(func):
    def wrap_func(*args, **kwargs):
        start = time()
        result = func(*args, **kwargs)
        delta = time()-start
        print(f'Function {func.__name__!r} executed in {delta:.4f}s')
        return result
    return wrap_func
