'''
This notebook shows you how to read **partitioned** device data in parquet format from Amazon S3 bucket via interfaces (either Apache Arrow or FastParquet) using python language

Other known methods of parquet format read include: Spark (Scala, Java, Python, SQL), Dask (Python), and Presto/Athena (SQL)

**this notebook will focus on slicing and dicing the data (i.e. select month, day, year, etc.)**

***
***Please note these are only starter examples and additional adjustments may need to be made depending on the structure and locality of your dataset & use case***

Refer to the official documentation to learn more about Arrow, FastParquet, Boto3, S3FS, and Pandas APIs

https://arrow.apache.org/docs/index.html

https://arrow.apache.org/docs/index.html

https://boto3.amazonaws.com/v1/documentation/api/latest/index.html

https://s3fs.readthedocs.io/en/latest/

https://pandas.pydata.org/docs/

***
1. Set Proxies & Bucket Access Keys
2. List Partitions in S3 Buckets
3. Read via Apache Arrow
***
'''

### import libraries
import sys
import os
import os.path

### set proxies
os.environ['HTTP_PROXY']=""
os.environ['HTTPS_PROXY']=""

### set s3 bucket access keys
aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID'] = ""
aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY'] = ""
region_name = os.environ['AWS_DEFAULT_REGION'] = ""

# read data (via apache arrow):
### import libraries
import boto3
import io
import pyarrow
import pandas as pd
import numpy as np
import s3fs
import pyarrow.parquet as pq

### view device data and available years worth of data in s3
! aws s3 ls  s3://bucket/prefix/device=device/

### read a device + year + month + day partition into pandas dataframe
fs = s3fs.S3FileSystem()

bucket = 'bucket'
path = 'prefix/device=device/year=year/month=month/day=day'
bucket_uri = f's3://{bucket}/{path}'

dataset = pq.ParquetDataset(bucket_uri, filesystem=fs)
table = dataset.read()
arrow_df = table.to_pandas()

### read a device + year + month + day (select days) partition into pandas dataframe
#### just add partitions to _filters_ parameter as _tuple_
fs = s3fs.S3FileSystem()

bucket = 'bucket'
path = 'prefix/device=device/year=year/month=month'
bucket_uri = f's3://{bucket}/{path}'

dataset = pq.ParquetDataset(bucket_uri, filesystem=fs, filters=[[('day', '=', 'value')],
                                                                [('day', '=', 'value')]])
table = dataset.read()
arrow_df = table.to_pandas()

# read data (via fastparquet):
### import libraries
import boto3
import io
import fastparquet
import pandas as pd
import numpy as np
import s3fs
import fastparquet as fp

### read a device partition into pandas dataframe
s3 = s3fs.S3FileSystem()
fs = s3fs.core.S3FileSystem()

bucket = 'bucket'
path = 'prefix//device=device'
root_dir_path = f'{bucket}/{path}'
s3_path = f"{root_dir_path}/year=*/month=*/day=*/*.parquet" # map * to num of partitioned columns in s3 path ex: device - YYYY - MM - d
all_paths_from_s3 = fs.glob(path=s3_path)

s3_open = s3.open
fp_obj = fp.ParquetFile(all_paths_from_s3, open_with=s3_open, root=root_dir_path)
fastparquet_df = fp_obj.to_pandas()

# read data (via presto):
# deps
import os
import pandas as pd
from pyhive import presto

import urllib3
urllib3.disable_warnings()

# presto cnx
cnx = presto.connect('host', # hostname
                      protocol='https',
                      port=port,
                      username= os.environ['USER'], # user account env variable
                      password = os.environ['PW'], # user account env variable
                      requests_kwargs={'verify': False})

pd.read_sql('show tables from table', cnx)
pd.read_sql('show columns from database.table', cnx)
pd.set_option('display.max_colwidth', None)
pd.read_sql('show create table database.table', cnx)['Create Table'].astype(str)

sql = """

select col
from database.table

"""

df = pd.read_sql(sql, cnx)