import asyncio
import time
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

# def decorator(func):
#     async def wrap_func():
#         print("a")
#         result = await func()
#         print("c")
#         return result
#     return wrap_func

# @decorator
# async def fun():
#     print("b")
#     return "inner"

def cpu_intensive(n):
    result = 0
    for i in range(n):
        result += i * i
    return result


async def inner():
    cpu_intensive(10**7)
    return "42"

async def main():
    loop = asyncio.get_running_loop()
    pool = ProcessPoolExecutor(6)
    L = [
        loop.run_in_executor(pool, inner),
        # loop.run_in_executor(pool, inner),
        # loop.run_in_executor(pool, inner),
        # loop.run_in_executor(pool, inner),
        # loop.run_in_executor(pool, inner),
    ]
    results = await asyncio.gather(*L)
    print(results)
    # results2 = await asyncio.gather(*results)
    # print(results)

asyncio.run(main())