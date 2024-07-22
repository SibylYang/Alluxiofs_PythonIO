import sys

import pandas as pd

from alluxio_custom_io import alluxio_open, alluxio_ls

def trace_calls(frame, event, arg):
    if event == 'call':
        code = frame.f_code
        func_name = code.co_name
        if func_name == 'open':
            print(f"Intercepted call to {func_name}")
            # Directly call custom_open instead of continuing to the original open
            def trace_lines(frame, event, arg):
                if event == 'line':
                    return trace_lines
                return trace_calls
            frame.f_globals['custom_open'] = alluxio_open
            # Execute the custom_open function with the arguments of the original call
            args = frame.f_locals['args']
            kwargs = frame.f_locals['kwargs']
            alluxio_open(*args, **kwargs)
            return trace_lines
    return

def test_function():
    f = open('s3://sibyltest/IO_test/sent_train_99.csv', 'r')
    # with open('s3://sibyltest/IO_test/sent_train_99.csv', 'r') as f:
    #     data = pd.read_csv(f)
    #     print(data)
    #     f.seek(5, 0)
    #     print("current position: ", f.tell())


# Modify the test_function to capture the args and kwargs for open
import inspect
# def modified_test_function():
#     code = inspect.getsource(test_function)
#     modified_code = code.replace("open(", "custom_open(args=(", 1)
#     modified_code = modified_code.replace(",)", ", kwargs={}))", 1)
#     exec(modified_code)

# Set the trace function
sys.settrace(trace_calls)

# Test the function
test_function()
# Reset the trace function to default
sys.settrace(None)

# mock

