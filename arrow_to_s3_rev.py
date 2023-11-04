import boto3
import os
import time
import pyarrow as pa
import pyarrow.parquet as pq


# Function to convert Arrow file to Parquet and upload to S3
def arrow_to_s3_parquet(arrow_file_path, s3_bucket_name, s3_parquet_key):
    # Read the Arrow file from disk
    with pa.memory_map(arrow_file_path, "rb") as f:
        table = pa.ipc.open_file(f).read_all()
        # table = arrow_file.read_all()

    # Convert Arrow Table to Parquet
    parquet_buffer = pa.BufferOutputStream()
    pq.write_table(table, parquet_buffer)
    parquet_data = parquet_buffer.getvalue()

    # Get S3 client
    s3_client = boto3.client("s3")

    # Upload the Parquet file to S3
    s3_client.put_object(Bucket=s3_bucket_name, Key=s3_parquet_key, Body=parquet_data)


directory = "/root/dnsproject/"
file = "domains_all.arrow"
session = boto3.session.Session()
client = session.client("s3")
s3_bucket = "domain-monitor-results"
host = os.uname()[1]
hostname = host.split(".")[0]
timestamp = time.time()
s3_filename = f"dns_result_{hostname}_{timestamp:0.0f}.parquet"
s3_key = s3_filename

# Execute the function
arrow_to_s3_parquet(directory + file, s3_bucket, s3_key)

print(f"File uploaded to S3 bucket {s3_bucket} at {s3_key}")
