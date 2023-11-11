import pyarrow as pa
import pyarrow.parquet as pq

directory = '/root/dnsproject/'
file = 'domains_all.arrow'
output = 'domains_all.parquet'
# Read arrow file from disk
table = pa.Table.from_batches(pa.ipc.open_file(directory + file).read_all())

# Write arrow table to parquet file
pq.write_table(table, directory + output)
