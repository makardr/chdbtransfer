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

client = clickhouse_connect.get_client(
    host=os.getenv("CLICKHOUSE_HOST"),
    port=int(os.getenv("CLICKHOUSE_PORT")),
    user=os.getenv("CLICKHOUSE_USERNAME"),
    password=os.getenv("CLICKHOUSE_PASSWORD"),
    database=os.getenv("CLICKHOUSE_DATABASE"),
)

# remote clickhouse
clickhouseClient = ClickhouseRemoteSource(client, "default", "events")
# clickhouseClient.read_clickhouse_db()
clickhouseClient.test_get_events_number_by_day()
clickhouseClient.test_get_events_sum()
clickhouseClient.test_select_date()
# client.close()
# Program finished execution in 591.1539990011079

# local parquet from remote chdb
# parquet_source = ParquetSource("./parquet_data/events.parquet")
# row = parquet_source.read_parquet_row()
# records = parquet_source.read_parquet_records()


# local chdb
chdb_client = ChdbSource("local", "events")
# chdb_client = ChdbSource("mytestdb", "testevents")
# chdb_client.create_table()
# chdb_client.write_parquet("./parquet_data/events.parquet")
# chdb_client.drop_table()
# Program finished execution in 422.26462500002526

#
# parquet_source.read_write_chdb(chdb_client)

# chdb_client.read_table()
chdb_client.test_get_events_number_by_day()
chdb_client.test_get_events_sum()
chdb_client.test_select_date()

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

# pycharm-python-interpreter  |   File "/opt/project/main.py", line 36, in <module>
# pycharm-python-interpreter  |     parquet_source.read_write_chdb(chdb_client)
# pycharm-python-interpreter  |   File "/opt/project/sources/ParquetSource.py", line 61, in read_write_chdb
# pycharm-python-interpreter  |     chdb_client.insert_records_into_chdb(records)
# pycharm-python-interpreter  |   File "/opt/project/sources/ChdbSource.py", line 180, in insert_records_into_chdb
# pycharm-python-interpreter  |     self.session.query(sql_query)
# pycharm-python-interpreter  |   File "/usr/local/lib/python3.12/site-packages/chdb/session/state.py", line 119, in query
# pycharm-python-interpreter  |     return self._conn.query(sql, fmt)
# pycharm-python-interpreter  |            ^^^^^^^^^^^^^^^^^^^^^^^^^^
# pycharm-python-interpreter  |   File "/usr/local/lib/python3.12/site-packages/chdb/state/sqlitelike.py", line 59, in query
# pycharm-python-interpreter  |     result = self._conn.query(query, format)
# pycharm-python-interpreter  |              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
# pycharm-python-interpreter  | RuntimeError: Code: 62. DB::Exception: Code: 62. DB::Exception: Cannot parse expression of type String here: 'Status(StatusCode="Internal", Detail="Error starting gRPC call. MethodAccessException: Attempt to access method 'System.IDisposa', '', '', '', '', '', '', '', : While executing ValuesBlockInputFormat: data for INSERT was parsed from query. (SYNTAX_ERROR) (version 24.8.4.1). (SYNTAX_ERROR)
# pycharm-python-interpreter exited with code 1

client.close()