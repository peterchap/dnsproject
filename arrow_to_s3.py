import os 
import time
import boto3
import pandas as pd
import pyarrow as pa
from io import BytesIO

def write_dataframe_to_s3_parquet(df, bucket, key, s3_client=None):
    """Write a dataframe to S3 in Parquet format.
    :param df: DataFrame to write.
    :param bucket: S3 bucket name.
    :param key: S3 key for the file (path + filename).
    :param s3_client: Pre-configured boto3 S3 client.
    """
    if s3_client is None:
        s3_client = boto3.client("s3")

    # Convert DataFrame to Parquet
    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer, index=False, engine="pyarrow")

    # Reset buffer cursor
    parquet_buffer.seek(0)

    # Upload the Parquet file to S3
    s3_client.put_object(Bucket=bucket, Key=key, Body=parquet_buffer.getvalue())
    print(f"File uploaded to S3 bucket {bucket} at {key}")


directory = "/root/dnsproject/"
session = boto3.session.Session()
client = session.client("s3")



s3_bucket = "domain-monitor-results"
host = os.uname()[1]
hostname = host.split(".")[0]
timestamp = time.time()
s3_filename = f"dns_result_{hostname}_{timestamp:0.0f}.parquet"
s3_key = s3_filename
with pa.ipc.open_stream(directory + "domains_all.arrow") as reader:
	print(reader.schema)
	df = reader.read_pandas()
print(df.shape)
write_dataframe_to_s3_parquet(df, s3_bucket, s3_key)
