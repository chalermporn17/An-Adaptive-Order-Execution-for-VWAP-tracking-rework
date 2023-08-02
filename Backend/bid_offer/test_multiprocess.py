import numpy as np
import time
import random
from asynchronous import run_function_multiple_times_multiple_processes


def gen_file(file_name):
    r1 = random.randint(5, 10) 
    time.sleep(r1)
    print(file_name)
    
if __name__ == '__main__':
    name_list = []
    for i in range(1,20):
        name_list.append('file_' + str(i))

    data = run_function_multiple_times_multiple_processes(
                gen_file,
                [
                    [file_name] for file_name in name_list
                ],
            )