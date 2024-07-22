import fsspec
from alluxiofs import AlluxioFileSystem
import inspect
import builtins

# Save the original open function before replacing it
original_open = builtins.open

# Define a global variable for alluxio_fs
file_systems = {}

def initialize_alluxio_file_systems(protocol):
    global file_systems
    # Register the Alluxio file system with fsspec for different protocols
    fsspec.register_implementation("alluxiofs", AlluxioFileSystem, clobber=True)
    file_systems[protocol] = fsspec.filesystem("alluxiofs", etcd_hosts="localhost", etcd_port=2379, target_protocol=protocol)

supported_file_paths = {'s3', 'hdfs'}


def alluxio_open(file, mode='r', buffering=-1, encoding=None, errors=None, newline=None, closefd=True, opener=None):
    file_path_prefix = file.split('://')[0]
    if file_path_prefix in supported_file_paths:
        print(f"Alluxio Opening file: {file} with mode: {mode}, encoding: {encoding}")
        if file_path_prefix not in file_systems:
            initialize_alluxio_file_systems(file_path_prefix)
        alluxio_fs = file_systems[file_path_prefix]

        try:
            return alluxio_fs.open(file, mode=mode, buffering=buffering, encoding=encoding, errors=errors,
                                   newline=newline, closefd=closefd, opener=opener)
        except Exception as e:
            print(f"Failed to open file {file} with Alluxio: {e}")
            raise
    else:
        # For other paths, use the original built-in open function
        return original_open(file, mode=mode, buffering=buffering, encoding=encoding, errors=errors, newline=newline,
                          closefd=closefd, opener=opener)

def alluxio_ls(path):
    file_path_prefix = path.split('://')[0]
    if file_path_prefix in supported_file_paths:
        print(f"Alluxio list file: {path}" )
        if file_path_prefix not in file_systems:
            initialize_alluxio_file_systems(file_path_prefix)
        alluxio_fs = file_systems[file_path_prefix]

        try:
            return alluxio_fs.ls(path)
        except Exception as e:
            print(f"Failed to list path {path} with Alluxio: {e}")
            raise
    else:
        return original_ls(path)