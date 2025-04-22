import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd


class ParquetSource:
    def __init__(self, source_path: str):
        self.source_path = source_path
        self.parquet_file = pq.ParquetFile(source_path)
        self.schema = self.parquet_file.schema

        pd.set_option('display.max_rows', 40)
        pd.set_option('display.max_columns', None)
        pd.set_option('display.max_colwidth', None)

    def read_parquet(self):
        column_names = self.schema.names
        print("Selected columns:", column_names)

        # Read the first batch of 10 rows
        batch = next(self.parquet_file.iter_batches(batch_size=10))

        # Non pandas method
        # rows = [dict(zip(batch.schema.names, row)) for row in zip(*[column.to_pylist() for column in batch.columns])]
        # for row in rows:
        #     print(row)


        table = pa.Table.from_batches([batch])
        df = table.to_pandas()
        print(df.to_string())