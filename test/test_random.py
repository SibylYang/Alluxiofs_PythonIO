import fsspec
import pandas as pd
from alluxiofs import AlluxioFileSystem, AlluxioClient
#
fsspec.register_implementation("alluxiofs", AlluxioFileSystem, clobber=True)
alluxio_fs = fsspec.filesystem("alluxiofs", etcd_hosts="localhost", etcd_port=2379, target_protocol="s3")
#
files = alluxio_fs.ls('s3://sibyltest/IO_test/caltech-101')
print(files)
alluxio_client = AlluxioClient(etcd_hosts="localhost")
# load_success = alluxio_client.submit_load('s3://sibyltest/IO_test/caltech-101')
# print('Load successful:', load_success)
# progress = alluxio_client.load_progress('s3://sibyltest/IO_test/caltech-101')
# print('Load progress:', progress[1]['jobState'])
# #
with alluxio_fs.open('s3://sibyltest/IO_test/sent_train_99.csv', mode='r') as f:
    # data = pd.read_csv(f)
    f.seek(5, 0)
    print("current position: ", f.tell())
    # data = f.read()
    # print(data)

import s3fs

# Create an S3 filesystem object
# fs = s3fs.S3FileSystem()
# fs = fsspec.filesystem('s3')
# # Specify the file path
# file_path = 's3://sibyltest/IO_test/sent_train_2.csv'
#
# try:
#     # Open the file
#     with fs.open(file_path, 'r') as f:
#         # Read the file content or a specific byte range
#         content = f.read()
#         print(content)
#         data = pd.read_csv(f)
#
# except OSError as e:
#     if 'The requested range is not satisfiable' in str(e):
#         print(f"Error: {e}. The requested byte range is out of bounds.")
#     else:
#         print(f"An unexpected error occurred: {e}")
# file_contents = fs.cat_file(file_path, start=0, end=10)
# print(file_contents)
