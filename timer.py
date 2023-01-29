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

tasks = []

def start(name=None):
    task_name = "anonymous task" if name==None else "task "+name
    print(f"{task_name} started !")

    task = [name, None]
    tasks.append(task)
    task[1] = time.time() # start


def stop(name=None):
    end = time.time()

    task = tasks.pop()

    if name != None: # name verification
        if task[0] != name:
            raise ValueError(f"Invalid name for task : started with '{task[0]}' but stopped with '{name}'")

    task_name = "anonymous task" if task[0]==None else "task "+task[0]

    print(f"{task_name} finished ! ({(end-task[1]):.4f}s)")