import boto3
import s3fs
import os
import pandas as pd
import numpy as np

aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID'] = ""
aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY'] = ""
region_name = os.environ['AWS_DEFAULT_REGION'] = ""

def header(bucket, key):
    s3_client = boto3.client('s3',
                             aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
                             aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY'],
                             region_name = os.environ['AWS_DEFAULT_REGION'])
    file = s3_client.get_object(Bucket=bucket, Key=key)
    body = file['Body']
    return body.read().decode('utf-8').split()

lines = header('my-bucket', 'datasets/headers/cols.txt')

def s3_single_csv_file(key, bucket, s3_client=None, **kwargs):
    if s3_client is None:
        s3_client = boto3.client('s3',
                                 aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
                                 aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY'],
                                 region_name = os.environ['AWS_DEFAULT_REGION'])
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(obj["Body"], **kwargs)

single_csv_df = s3_single_csv_file('datasets/prefix.csv/key.csv',\
                            'my-bucket',\
                            delimiter=',',\
                            names=lines,\
                            usecols=['c1', 'c2', 'c3'])

def s3_multi_csv_files(prefix, bucket, s3=None, s3_client=None, **kwargs):
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
    s3_keys = [item.key for item in s3.Bucket(bucket).objects.filter(Prefix=prefix) if item.key.endswith('.csv')]
    data = [s3_single_csv_file(key, bucket=bucket, s3_client=s3_client, **kwargs) 
           for key in s3_keys]
    return pd.concat(data, ignore_index=True)

multi_csv_df = s3_multi_csv_files('datasets/prefix.csv/',\
                            'my-bucket',\
                            delimiter=',',\
                            names=lines,\
                            usecols=['c1', 'c2', 'c3'])
