import os

import clickhouse_connect
import time

from python_data.data_row import testRow, DataRow
from sources.ChdbSource import ChdbSource
from sources.ClickhouseRemoteSource import ClickhouseRemoteSource
from util.load_env_file import load_env_file
from sources.ParquetSource import ParquetSource

load_env_file()
start_time = time.perf_counter()

# client = clickhouse_connect.get_client(
#     host=os.getenv("CLICKHOUSE_HOST"),
#     port=int(os.getenv("CLICKHOUSE_PORT")),
#     user=os.getenv("CLICKHOUSE_USERNAME"),
#     password=os.getenv("CLICKHOUSE_PASSWORD"),
#     database=os.getenv("CLICKHOUSE_DATABASE"),
# )

# remote clickhouse
# clickhouseClient = ClickhouseRemoteSource(client, "default", "events")
# clickhouseClient.read_clickhouse_db()
# clickhouseClient.test_get_events_number_by_day()
# clickhouseClient.test_get_events_sum()
# clickhouseClient.test_select_date()
# client.close()
# Program finished execution in 591.1539990011079

# local parquet from remote chdb
# parquet_source = ParquetSource("./parquet_data/events.parquet")
# row = parquet_source.read_parquet_row()
# records = parquet_source.read_parquet_records()


# local chdb
# "local", "events" is full parquet file
# chdb_client = ChdbSource("local", "events")
chdb_client = ChdbSource("testline", "events")
# chdb_client = ChdbSource("mytestdb", "testevents")
chdb_client.create_table()
# chdb_client.write_parquet("./parquet_data/events.parquet")
# chdb_client.drop_table()

# parquet_source.read_write_chdb(chdb_client)

# chdb_client.read_table()
# chdb_client.test_get_events_number_by_day()
# chdb_client.test_get_events_sum()
# chdb_client.test_select_date()
chdb_client.insert_row(testRow)
chdb_client.test_get_line()

# Insert rows by row
# chdb_client.insert_row(row)
# for record in records:
#     chdb_client.insert_row(DataRow.from_tuple(record))


# Insert batches
# chdb_client.insert_records_into_chdb(records)


# chdb_client.read_table()


end_time = time.perf_counter()
execution_time_ms = (end_time - start_time) * 1000
print(f"Program finished execution in {execution_time_ms}")
# client.close()
