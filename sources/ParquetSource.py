import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd

from python_data.data_row import DataRow


class ParquetSource:
    def __init__(self, source_path: str):
        self.source_path = source_path
        self.parquet_file = pq.ParquetFile(source_path)
        self.schema = self.parquet_file.schema

        pd.set_option('display.max_rows', 40)
        pd.set_option('display.max_columns', None)
        pd.set_option('display.max_colwidth', None)

    def read_parquet_row(self) -> DataRow:
        column_names = self.schema.names
        print("Selected columns:", column_names)

        batch = next(self.parquet_file.iter_batches(batch_size=10))

        table = pa.Table.from_batches([batch])
        df = table.to_pandas()
        print(df.to_string())

        first_row = df.iloc[0].to_dict()
        decoded_binary_row = {k: (v.decode('utf-8') if isinstance(v, bytes) else v) for k, v in first_row.items()}
        event = DataRow(**decoded_binary_row)

        print(event)

        # Non pandas method
        # rows = [dict(zip(batch.schema.names, row)) for row in zip(*[column.to_pylist() for column in batch.columns])]
        # for row in rows:
        #     print(row)
        return event
