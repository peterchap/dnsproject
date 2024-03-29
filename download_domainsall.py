import requests
import os
import pyarrow.csv as csv
import pyarrow.parquet as pq
import pyarrow as pa
import zipfile
from zipfile import ZipFile

token = '5123dedd3e04a07c6380b8aec0ba30b2'
zone = 'full'
format = 'zip'
cols = ["domain", "ns", "ip", "country-dm", "web_server", "email", "Alexa_rank", "phone"]
api = f'https://domains-monitor.com/api/v1/{token}/get-detailed/{zone}/list/{format}/'
schema = pa.schema(
    [
        pa.field("domain", pa.string()),
        pa.field("ns", pa.string()),
        pa.field("ip", pa.string()),
        pa.field("country-dm", pa.string()),
    ]
)


zip_file_path = '/root/dnsall/'
input_file = 'domainsall.zip'
with requests.get(api, stream=True) as response:
    response.raise_for_status()  # This will raise an error for non-200 responses
    with open(zip_file_path + input_file, 'wb') as file:
        for chunk in response.iter_content(chunk_size=8192): 
            file.write(chunk)
print('Downloaded zip file')


zip_folder = "domainsall.zip"
file = "domains-detailed.csv"
directory = "/root/dnsall/"

# Extract the CSV file from the ZIP
with zipfile.ZipFile(directory + zip_folder, 'r') as zip_ref:
    zip_ref.extract(file, directory)

print("file unzipped")

os.remove(directory + zip_folder)
print("zip removed")

# Perform the split and conversion
print("start")
number_of_files = 10
small_chunk_size = 1000000  # Adjust based on your system's memory capacity

# Initialize variables
rows_processed = 0
accumulated_batches = []
file_counter = 1

# Read and process CSV in chunks
def skip_bad_rows(row):
   return "skip"

read_options = csv.ReadOptions(
        use_threads=True, 
        block_size=small_chunk_size,
        column_names=cols,
        encoding="utf-8"
    )
parse_options = csv.ParseOptions(
        delimiter=";",
        quote_char='"',
        ignore_empty_lines=True,
        invalid_row_handler=skip_bad_rows
    )
reader = csv.open_csv(directory + file, read_options= read_options, parse_options= parse_options)
while True:
    try:
       data  = next(reader)
       # Create a new RecordBatch with selected columns
       batch = data.select(["domain", "ns", "ip", "country-dm"])
       # Accumulate data
       accumulated_batches.append(batch)

       rows_processed += batch.num_rows

       # Write to Parquet file if the chunk limit is reached or end of file is reached
       if rows_processed >= (270000000 // number_of_files):
           accumulated_table = pa.Table.from_batches(accumulated_batches)
           file_name = f'{directory}dma_{file_counter}.parquet'
           writer = pq.ParquetWriter(file_name, schema)
           writer.write_table(accumulated_table)
           writer.close()
           file_counter += 1
           accumulated_batches = []
           rows_processed = 0
    except StopIteration:
        # Write any remaining data to a Parquet file
        if accumulated_batches:
            accumulated_table = pa.Table.from_batches(accumulated_batches)
            file_name = f'{directory}dma_{file_counter}.parquet'
            writer = pq.ParquetWriter(file_name, schema)
            writer.write_table(accumulated_table)
            writer.close()
        break
print("Conversion completed.")
