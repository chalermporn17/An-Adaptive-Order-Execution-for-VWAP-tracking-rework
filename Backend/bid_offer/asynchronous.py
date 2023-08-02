from typing import List, Tuple, Callable, Any, Coroutine
import asyncio
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import os

# import config

# this function is deprecated since python 3.9
# https://docs.python.org/release/3.9.1/library/asyncio-task.html#asyncio.to_thread
def in_async(function):
    """
    convert synchronous function to multithread asynchronous function

    function (Function): synchronous function
    """
    async def async_function(*parameters):
        loop = asyncio.get_running_loop()
        with ThreadPoolExecutor() as pool:
            result = await loop.run_in_executor(
                pool,
                function,
                *parameters
            )
            return result
    return async_function

# def in_async_new_in_python_3_9(function):
#     def result(*args, **kwargs):
#         return asyncio.to_thread(function, *args, **kwargs)
#     return result

async def run_function_multiple_times_multiple_threads(
    function,
    parameters:List[list],
    max_workers:int = None,
):
    """
    function (Function): synchronous function
    parameters (List): lists of parameter to run
    """
    loop = asyncio.get_running_loop()
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        result = await asyncio.gather(*[
            loop.run_in_executor(
                executor,
                function,
                *args
            ) for args in parameters
        ])
        return result

def run_execute_function(lists):
    return function(*lists)

def init(*args):
    global function
    function = args[0]

    initialize_function = args[1]
    if callable(initialize_function):
        initialize_function()

    # global function
    # global obj
    # function, obj = args

    # https://pymongo.readthedocs.io/en/stable/faq.html#is-pymongo-fork-safe
    # you have to create new connection per fork
    # mongo_client = MongoClient(config.mongo_db_hostname, config.mongo_db_port)
    # obj.set_client(mongo_client)

"""
https://stackoverflow.com/a/52283968
Pool needs to pickle (serialize) everything it sends to its worker-processes
(IPC). Pickling actually only saves the name of a function and unpickling
requires re-importing the function by name. For that to work, the function
needs to be defined at the top-level, nested functions won't be importable
by the child and already trying to pickle them raises an exception (more).

https://stackoverflow.com/a/56011074
"""
def run_function_multiple_times_multiple_processes(
    function,
    parameters:List[list],
    initialize_function = None,
    max_workers:int = 6,
):
    """
    function (Function): synchronous function
    parameters (List): lists of parameter to run
    """
    with ProcessPoolExecutor(
        max_workers=max_workers,
        initializer=init,
        initargs=(function, initialize_function,)
    ) as executor:
        result = list(executor.map(run_execute_function, parameters))
        return result


def run():
    result = run_function_multiple_times_multiple_processes(
        lambda v: v + 50,
        [
            [i] for i in range(10)
        ],
        initialize_function=lambda : print('init yay!!')
    )

    print(result)

# def function1(arg1):
#     print(f'arg1 = {arg1}')
#     gg_function()
#     return

# def function2(arg1, arg2):
#     print(f'arg1 = {arg1} | arg2 = {arg2}')
#     return arg1 + arg2

# def function3(lists):
#     return function2(*lists)

# def list_to_args(function):
#     def haha(lists):
#         return function(*lists)
#     return haha

# def run_execute_function_2(lists):
#     return function(*lists)

# def init_2(*args):
#     global function
#     function = args[0]

# def run_in_process(function, parameters):
#     with ProcessPoolExecutor(max_workers=4, initializer=init_2, initargs=(function,)) as executor:
#         # executor.map(function1, parameters)
#         result = list(executor.map(run_execute_function_2, parameters))
#         return result
#         # executor.map(list_to_args(function2), parameters)
#         # executor.submit(function, [1, 2])

# async def main():
#     # with ProcessPoolExecutor(max_workers=4) as executor:
#         # pass
#         # executor.map(function1, [1, 2])
#         # executor.map(function1, [[1, 2], [3, 4]])
#         # executor.map(function3, [[1, 2], [3, 4]])

#     # def function2(arg1, arg2):
#     #     print(f'arg1 = {arg1} | arg2 = {arg2}')
#     #     return

#     run_in_process(function2, [[1, 2], [3, 4]])
#     # h = list_to_args(function2)
#     # await run_in_process(h, [[1, 2], [3, 4]])

if __name__ == '__main__':
    # asyncio.run(main())
    run()
    