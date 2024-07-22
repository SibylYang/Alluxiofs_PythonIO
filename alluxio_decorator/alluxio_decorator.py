from .alluxio_custom_io import alluxio_open, alluxio_ls
import builtins
import os
from functools import wraps
import pandas as pd

def alluxio_io(func):
    replacement_map = {
        'open': (alluxio_open, builtins.open),
        'listdir': (alluxio_ls, os.listdir),
    }
    originals = {}
    @wraps(func)
    def decorator(*args, **kwargs):

        for name, (custom_func, original_func) in replacement_map.items():
            originals[name] = original_func
            if name == 'open':
                builtins.open = custom_func
            elif name == 'listdir':
                os.listdir = custom_func

        try:
            result = func(*args, **kwargs)
        finally:
            for name in replacement_map:
                if name == 'open':
                    builtins.open = originals[name]
                elif name == 'listdir':
                    os.listdir = originals[name]

        return result


    return decorator

# Example usage
@alluxio_io
def example_open(file):
    with open(file, 'r') as f:
        f.seek(5, 0)
        print("current position: ", f.tell())
        # data = f.read()
        # print(data)

    print(os.listdir('s3://sibyltest/IO_test/'))

example_open('s3://sibyltest/IO_test/sent_train_1.csv')

