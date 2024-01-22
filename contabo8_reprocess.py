import pyarrow.parquet as pq
import pyarrow as pa


file = "refresh.parquet"
directory = "/root/"
outputdir = "/root/dnsall/"

#perform the split and conversion
print("start")
number_of_files = 10

pf  = pq.read_table(directory + file, columns=['domain'])

total_rows  = len(pf)
rows_per_split = total_rows // number_of_files

# Split and save
for i in range(number_of_files):
    start_row = i * rows_per_split
    end_row = rows_per_split if i != 9 else total_rows
    split_pf = pf.slice(offset=start_row, length = end_row)
    print(f'file: {i} size: {len(split_pf)}')
    pq.write_table(split_pf, f'{outputdir}dma_{i+1}.parquet')

print("Conversion completed.")
