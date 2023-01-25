import time

def dec(fun):
    def decorator(*args, **kwargs):
        return call(fun, *args, **kwargs)
    return decorator

def call(fun, *args, **kwargs):
    print(f"----- Function '{fun.__name__}' started!")
    start = time.time()
    result = fun(*args, **kwargs)
    end = time.time()
    print(f"----- Function '{fun.__name__}' finished! ({(end-start):.4f}s)")
    return result