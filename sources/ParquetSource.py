import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd

from python_data.data_row import DataRow
from sources import ChdbSource


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
        # print("Selected columns:", column_names)

        batch = next(self.parquet_file.iter_batches(batch_size=10))

        table = pa.Table.from_batches([batch])
        df = table.to_pandas()
        # print(df.to_string())

        first_row = df.iloc[0].to_dict()
        decoded_binary_row = {k: (v.decode('utf-8') if isinstance(v, bytes) else v) for k, v in first_row.items()}
        event = DataRow(**decoded_binary_row)

        # Non pandas method
        # rows = [dict(zip(batch.schema.names, row)) for row in zip(*[column.to_pylist() for column in batch.columns])]
        # for row in rows:
        #     print(row)
        return event

    def read_parquet_records(self) -> list[DataRow]:
        records = []
        for batch in self.parquet_file.iter_batches(batch_size=100000):
            df = batch.to_pandas()
            for column in df.columns:
                if df[column].dtype == object:
                    df[column] = df[column].apply(lambda x: x.decode('utf-8') if isinstance(x, bytes) else x)
            chunk_records = df.to_records(index=False).tolist()
            records.extend(chunk_records)

        print(f"Finished reading {len(records)} records")
        return records

    def read_write_chdb(self, chdb_client: ChdbSource):
        for batch in self.parquet_file.iter_batches(batch_size=100000):
            records = []
            df = batch.to_pandas()
            for column in df.columns:
                if df[column].dtype == object:
                    df[column] = df[column].apply(lambda x: x.decode('utf-8') if isinstance(x, bytes) else x)
            chunk_records = df.to_records(index=False).tolist()
            records.extend(chunk_records)
            chdb_client.insert_records_into_chdb(records)
            print(f"Finished writing {len(records)} records")
        print("Finished writing records")
