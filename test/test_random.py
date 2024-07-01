import fsspec
import pandas as pd
from alluxiofs import AlluxioFileSystem, AlluxioClient

fsspec.register_implementation("alluxiofs", AlluxioFileSystem, clobber=True)
alluxio_fs = fsspec.filesystem("alluxiofs", etcd_hosts="localhost", etcd_port=2379, target_protocol="s3")

files = alluxio_fs.ls('s3://sibyltest/IO_test/sent_train_2.csv')
print(files)
# alluxio_client = AlluxioClient(etcd_hosts="localhost")
# load_success = alluxio_client.submit_load('s3://sibyltest/IO_test/sent_train_1.csv')
# print('Load successful:', load_success)
# progress = alluxio_client.load_progress('s3://sibyltest/IO_test/sent_train_1.csv')
# print('Load progress:', progress[1]['jobState'])
#
with alluxio_fs.open('s3://sibyltest/IO_test/sent_train_1.csv', mode='r') as f:
    data = pd.read_csv(f)
    print(data)