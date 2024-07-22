import alluxioio
import os
import shutil
import pandas as pd
import fsspec
from alluxiofs import AlluxioFileSystem
# from tests3 import BertDataset, BERT, finetune
f = open('s3://sibyltest/IO_test/sent_train_100_rename.csv', 'r')
    # data = pd.read_csv(f)
    # print(data)
# f.seek(5, 0)
# print("current position: ", f.tell())
data_10 = f.read()
# print(data_10)
f.close()

# for d in os.listdir('s3://sibyltest/IO_test/'):
#     print(d)
# os.mkdir('s3://newdir123')
# os.rmdir('s3://newdir123')
# os.rename("s3://sibyltest/IO_test/sent_train_1_loaded.csv", "s3://sibyltest/IO_test/sent_train_1_loaded_rename.csv")
shutil.copy(src="s3://sibyltest/IO_test/sent_train_1_loaded_rename.csv", dst="s3://sibyltest/IO_test/test_posix/sent_train_1_loaded_rename.csv")
# os.remove("s3://sibyltest/IO_test/test_posix/sent_train_1_loaded_rename.csv")
# print(os.stat('s3://sibyltest/IO_test/'))
# print(os.path.isdir('s3://sibyltest/IO_test/'))
# print(os.path.isfile('s3://sibyltest/IO_test/'))
# print(os.path.exists('s3://sibyltest/IO_test/'))
# print(os.walk('s3://sibyltest/IO_test/'))