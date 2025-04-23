import clickhouse_connect
import time

from python_data.data_row import testRow
from sources.ChdbSource import ChdbSource
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
# clickhouseClient = ClickhouseRemoteSource(client)
# clickhouseClient.read_clickhouse_db()
# client.close()


# local parquet from remote chdb
parquet_source = ParquetSource("./parquet_data/events.parquet")
row = parquet_source.read_parquet_row()

# local chdb
chdb_client = ChdbSource("default_test", "events")
chdb_client.create_table()
chdb_client.insert_row(row)
chdb_client.read_table()


end_time = time.perf_counter()
execution_time_ms = (end_time - start_time) * 1000
print(execution_time_ms)
