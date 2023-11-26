import requests
import polars as pl
from zipfile import ZipFile

from io import StringIO

token = '5123dedd3e04a07c6380b8aec0ba30b2'
zone = 'full'
format = 'zip'
cols = ["domain", "ns", "ip", "country", "web_server", "email", "Alexa_rank", "phone"]
api = f'https://domains-monitor.com/api/v1/{token}/get-detailed/{zone}/list/{format}/'
#print(api)


zip_file_path = '/root/dnsproject/'
input_file = 'domainsall.zip'

'''
with requests.get(api, stream=True) as response:
    response.raise_for_status()  # This will raise an error for non-200 responses
    with open(zip_file_path + input_file, 'wb') as file:
        for chunk in response.iter_content(chunk_size=8192):
            file.write(chunk)
print('Downloaded zip file')
'''
with requests.get(api, stream=True) as response:
    response.raise_for_status()  # This will raise an error for non-200 responses
    with open(zip_file_path + input_file, 'wb') as file:
        for chunk in response.iter_content(chunk_size=8192): 
            file.write(chunk)
print('Downloaded zip file')

# Open csv file in zip file, split into slices using polars and save as parquet files
zip_folder = "domains-detailed.zip"
file = "domains-detailed.csv"
directory = "/root/dnsproject"

cols = ["domain", "ns", "ip", "country", "web_server", "Alexa_rank"]
pl.read_csv(
    ZipFile(zip_file_path + zip_folder).open(file).read(),
    quote_char='"',
    columns=[0, 1, 2, 3, 4, 6],
    separator=";",
    low_memory=False,
    new_columns=cols,
    ignore_errors=False,
).write_parquet(directory + "domains_all.parquet")

print(pl.read_parquet_schema(directory + "domains_all.parquet"))

df = pl.read_parquet(directory + "domains_all.parquet", columns=["domain"])

# Calculate the number of rows in each subfile
num_rows = len(df)
print(len(df), num_rows)
CHUNK_SIZE = 27000000

start = 0
num_chunks = (
    num_rows - start + CHUNK_SIZE
) // CHUNK_SIZE  # This formula gives the ceiling of the division
print(num_chunks)

for i in range(num_chunks):
    # Calculate end for each chunk
    subfile = df.slice(start, CHUNK_SIZE)
    subfile.write_parquet(f"E:/domains-monitor/dma_{i}.parquet")

    print(i, start, CHUNK_SIZE)

    # Update the start for next chunk
    CHUNK_SIZE = min(CHUNK_SIZE, num_rows - start + CHUNK_SIZE)
    start = start + CHUNK_SIZE

# Extract the CSV file from the ZIP
with zipfile.ZipFile(directory + zip_folder, 'r') as zip_ref:
    zip_ref.extract(file, directory)

print("file unzipped")

reader = pl.read_csv_batched(directory + file)
batches = reader.next_batches(27000000)
i = 0
while batches: 
    print(i)
    batches.write_parquet(f"E:/domains-monitor/dma_{i}.parquet")
    batches = reader.next_batches(27000000)
    i += 1