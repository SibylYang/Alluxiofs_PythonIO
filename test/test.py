import alluxioio
import pandas as pd
with open('s3://sibyltest/IO_test/sent_train_1.csv', 'r') as f:
    data = pd.read_csv(f)
    print(data)
    f.seek(5, 0)
    print("current position: ", f.tell())
    data_10 = f.read(10)
    print(data_10)

# init alluxio system
# cat
# seek to position, read certain bytes
# other mode
# file load into alluxio?