import boto3
import os
import io
import pyarrow
import fastparquet
import pandas as pd
import numpy as np

aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID'] = ""
aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY'] = ""
region_name = os.environ['AWS_DEFAULT_REGION'] = ""

def s3_single_parquet_file(key, bucket, s3_client=None, **kwargs):
    if s3_client is None:
        s3_client = boto3.client('s3',
                                 aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
                                 aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY'],
                                 region_name = os.environ['AWS_DEFAULT_REGION'])
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    return pd.read_parquet(io.BytesIO(obj['Body'].read()), **kwargs)

single_parquet_df = s3_single_parquet_file('datasets/prefix.parquet/key.parquet',\
                                          'my-bucket', engine='pyarrow', columns=['c1', 'c2', 'c3'])

def s3_multi_parquet_files(prefix, bucket, s3=None, s3_client=None, **kwargs):
    if not prefix.endswith('/'):
        prefix = prefix + '/'
    if s3_client is None:
        s3_client = boto3.client('s3',
                                 aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
                                 aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY'],
                                 region_name = os.environ['AWS_DEFAULT_REGION'])
    if s3 is None:
        s3 = boto3.resource('s3',
                            aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
                            aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY'],
                            region_name = os.environ['AWS_DEFAULT_REGION'])
    s3_keys = [item.key for item in s3.Bucket(bucket).objects.filter(Prefix=prefix) if item.key.endswith('.parquet')]
    data = [s3_single_parquet_file(key, bucket=bucket, s3_client=s3_client, **kwargs) 
           for key in s3_keys]
    return pd.concat(data, ignore_index=True)

multi_parquet_df = s3_multi_parquet_files('datasets/prefix.parquet/',\
                                          'my-bucket', engine='pyarrow', columns=['c1', 'c2', 'c3'])
