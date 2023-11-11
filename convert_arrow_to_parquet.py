import pyarrow as pa
import pyarrow.parquet as pq

directory = '/root/dnsproject/'
file = 'domains_all.arrow'
output = 'domains_all.parquet'
# Read arrow file from disk
with pa.ipc.open_stream(directory+file) as f:
    table = f.read_all()

# Write arrow table to parquet file
pq.write_table(table, directory + output)
