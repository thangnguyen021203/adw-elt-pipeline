import pyarrow.parquet as pq
table = pq.read_table("parquet_files/Sales_SpecialOffer.parquet")
print(table.schema)
print(table.to_pandas().head())
