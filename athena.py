import os
import pyathena
from pyathena import connect

aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID'] = ""
aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY'] = ""
region_name = os.environ['AWS_DEFAULT_REGION'] = ""

bucket = 'my-bucket'
prefix = 'datasets/athena-queries/'

def athena_python(sql='', s3_staging_dir=''):
    cnx = connect(aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
                   aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
                   region_name=os.environ['AWS_DEFAULT_REGION'],
                   s3_staging_dir=s3_staging_dir)
    athena_df = pd.read_sql(sql, cnx)
    return athena_df
 
 athena_df = athena_python('SELECT * FROM database.table limit 10;', 's3://{}/{}'.format(bucket, prefix))
