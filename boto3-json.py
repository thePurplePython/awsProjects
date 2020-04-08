import boto3
import s3fs
import os
import pandas as pd
import numpy as np

aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID'] = ""
aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY'] = ""
region_name = os.environ['AWS_DEFAULT_REGION'] = ""

def s3_single_json_file(key, bucket, s3_client=None, **kwargs):
    if s3_client is None:
        s3_client = boto3.client('s3',
                                 aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
                                 aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY'],
                                 region_name = os.environ['AWS_DEFAULT_REGION'])
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    return pd.read_json(obj["Body"], **kwargs)

single_json_df = s3_single_json_file('datasets/prefix.json/key.json',\
                            'my-bucket', lines=True)

def s3_multi_json_files(prefix, bucket, s3=None, s3_client=None, **kwargs):
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
    s3_keys = [item.key for item in s3.Bucket(bucket).objects.filter(Prefix=prefix) if item.key.endswith('.json')]
    data = [s3_single_json_file(key, bucket=bucket, s3_client=s3_client, **kwargs) 
           for key in s3_keys]
    return pd.concat(data, ignore_index=True)

multi_json_df = s3_multi_json_files('datasets/prefix.json/',\
                            'my-bucket', lines=True)
