import fsspec
from alluxiofs import AlluxioFileSystem
import inspect
import builtins

# Save the original open function before replacing it
original_open = builtins.open

# Define a global variable for alluxio_fs
file_systems = {}

def initialize_alluxio_file_systems():
    global file_systems
    # Register the Alluxio file system with fsspec for different protocols
    fsspec.register_implementation("alluxiofs", AlluxioFileSystem, clobber=True)
    file_systems['s3'] = fsspec.filesystem("alluxiofs", etcd_hosts="localhost", etcd_port=2379, target_protocol="s3")
    file_systems['hdfs'] = fsspec.filesystem("alluxiofs", etcd_hosts="localhost", etcd_port=2379, target_protocol="hdfs")

def alluxio_open(file, mode='r', buffering=-1, encoding=None, errors=None, newline=None, closefd=True, opener=None):

    if file.startswith('s3://'):

        print(f"Alluxio Opening file: {file} with mode: {mode}, encoding: {encoding}")
        alluxio_fs = file_systems['s3']
        try:
            return alluxio_fs.open(file, mode=mode, buffering=buffering, encoding=encoding, errors=errors,
                                   newline=newline, closefd=closefd, opener=opener)
        except Exception as e:
            print(f"Failed to open file {file} with Alluxio: {e}")
            raise
    elif file.startswith('hdfs://'):
        alluxio_fs = file_systems['hdfs']
        try:
            return alluxio_fs.open(file, mode=mode, buffering=buffering, encoding=encoding, errors=errors,
                                   newline=newline, closefd=closefd, opener=opener)
        except Exception as e:
            print(f"Failed to open file {file} with Alluxio: {e}")
            raise
    else:
        # For non-S3 paths, use the original built-in open function
        return original_open(file, mode=mode, buffering=buffering, encoding=encoding, errors=errors, newline=newline,
                             closefd=closefd, opener=opener)
