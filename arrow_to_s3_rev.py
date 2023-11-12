import boto3
import os
import time
import pyarrow as pa
import pyarrow.parquet as pq
import s3fs

# Function to convert Arrow file to Parquet and upload to S3
def arrow_to_s3_parquet(arrow_file_path, s3_bucket_name, s3_parquet_key):
    # Read the Arrow file from disk
    with pa.ipc.open_stream(arrow_file_path) as f:
        table = f.read_all()

    fs =s3fs.S3FileSystem(anon=False)
    s3_path = f"s3://{s3_bucket_name}/{s3_parquet_key}"
    pq.write_table(table, fs.open(s3_path, "wb"))

directory = "/root/dnsproject/"
file = "domains_all.arrow"

s3_bucket = "domain-monitor-results"
host = os.uname()[1]
hostname = host.split(".")[0]
timestamp = time.time()
s3_filename = f"dns_result_{hostname}_{timestamp:0.0f}.parquet"
s3_key = s3_filename

# Execute the function
arrow_to_s3_parquet(directory + file, s3_bucket, s3_key)

print(f"File uploaded to S3 bucket {s3_bucket} at {s3_key}")
