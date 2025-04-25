import chdb
from chdb.session import Session
import pandas as pd
from python_data.data_row import DataRow
import chdb.dataframe as cdf

from util.wrap_timer import timer


# noinspection SqlNoDataSourceInspection
class ChdbSource:

    def __init__(self, db_name: str, table_name: str):
        self.db_name = db_name
        self.table_name = table_name
        self.session = Session("/opt/chdb_data")
        # self.connection = chdb.connect("/opt/chdb_data")

    def create_table(self):
        self.session.query(f"CREATE DATABASE IF NOT EXISTS {self.db_name}")
        self.session.query(f"""
        CREATE TABLE IF NOT EXISTS {self.db_name}.{self.table_name}(
            `event_app` String,
            `event` String,
            `event_date` Date DEFAULT toDate(event_time),
            `event_time` DateTime,
            `event_id` UInt32,
            `string_00` String,
            `string_01` String,
            `string_02` String,
            `string_03` String,
            `string_04` String,
            `string_05` String,
            `string_06` String,
            `string_07` String,
            `string_08` String,
            `string_09` String,
            `string_10` String,
            `string_11` String,
            `string_12` String,
            `string_13` String,
            `string_14` String,
            `string_15` String,
            `string_16` String,
            `string_17` String,
            `string_18` String,
            `string_19` String,
            `float_00` Float64,
            `float_01` Float64,
            `float_02` Float64,
            `float_03` Float64,
            `float_04` Float64,
            `float_05` Float64,
            `float_06` Float64,
            `float_07` Float64,
            `float_08` Float64,
            `float_09` Float64,
            `float_10` Float64,
            `float_11` Float64,
            `float_12` Float64,
            `float_13` Float64,
            `float_14` Float64,
            `float_15` Float64,
            `float_16` Float64,
            `float_17` Float64,
            `float_18` Float64,
            `float_19` Float64,
            `string_20` String,
            `string_21` String,
            `string_22` String,
            `string_23` String,
            `string_24` String,
            `string_25` String,
            `string_26` String,
            `string_27` String,
            `string_28` String,
            `string_29` String,
            `float_20` Float64,
            `float_21` Float64,
            `float_22` Float64,
            `float_23` Float64,
            `float_24` Float64,
            `float_25` Float64,
            `float_26` Float64,
            `float_27` Float64,
            `float_28` Float64,
            `float_29` Float64,
            `event_context` String,
            `event_tick` UInt32
        )
        ENGINE = ReplacingMergeTree(event_tick)
        PARTITION BY toYYYYMM(event_date)
        PRIMARY KEY (event_app, event, event_date, event_time)
        ORDER BY (event_app, event, event_date, event_time, event_id)
        SETTINGS index_granularity = 8192
        """)

    def insert_row(self, row: DataRow):
        sql_query = f"""
            INSERT INTO {self.db_name}.{self.table_name} (*) VALUES (
            '{row.event_app}',
            '{row.event}',
            '{row.event_date}',
            '{row.event_time}',
            '{row.event_id}',
            '{row.string_00}',
            '{row.string_01}',
            '{row.string_02}',
            '{row.string_03}',
            '{row.string_04}',
            '{row.string_05}',
            '{row.string_06}',
            '{row.string_07}',
            '{row.string_08}',
            '{row.string_09}',
            '{row.string_10}',
            '{row.string_11}',
            '{row.string_12}',
            '{row.string_13}',
            '{row.string_14}',
            '{row.string_15}',
            '{row.string_16}',
            '{row.string_17}',
            '{row.string_18}',
            '{row.string_19}',
            '{row.float_00}',
            '{row.float_01}',
            '{row.float_02}',
            '{row.float_03}',
            '{row.float_04}',
            '{row.float_05}',
            '{row.float_06}',
            '{row.float_07}',
            '{row.float_08}',
            '{row.float_09}',
            '{row.float_10}',
            '{row.float_11}',
            '{row.float_12}',
            '{row.float_13}',
            '{row.float_14}',
            '{row.float_15}',
            '{row.float_16}',
            '{row.float_17}',
            '{row.float_18}',
            '{row.float_19}',
            '{row.string_20}',
            '{row.string_21}',
            '{row.string_22}',
            '{row.string_23}',
            '{row.string_24}',
            '{row.string_25}',
            '{row.string_26}',
            '{row.string_27}',
            '{row.string_28}',
            '{row.string_29}',
            '{row.float_20}',
            '{row.float_21}',
            '{row.float_22}',
            '{row.float_23}',
            '{row.float_24}',
            '{row.float_25}',
            '{row.float_26}',
            '{row.float_27}',
            '{row.float_28}',
            '{row.float_29}',
            '{row.event_context}',
            '{row.event_tick}')
            ;
        """
        self.session.query(sql_query)

    def insert_records_into_chdb(self, records):
        print("Inserting records into ChDB")
        sql_query = f"""INSERT INTO {self.db_name}.{self.table_name} (*) VALUES"""

        values_str = ",".join([
            "(" + ", ".join(
                f"'{str(item)}'" if isinstance(item, str) else str(item)
                for item in record
            ) + ")"
            for record in records
        ])
        sql_query += values_str

        self.session.query(sql_query)
        print(f"Inserted {len(records)} records into ClickHouse")

    def read_table(self):
        result = self.session.query(f"SELECT * FROM {self.db_name}.{self.table_name} LIMIT 10000;")
        print(f"Table {self.table_name} contents: \n{result}")

    def drop_table(self):
        self.session.query(f"DROP TABLE IF EXISTS {self.db_name}.{self.table_name};")
        print(f"Table {self.table_name} dropped")

    def write_parquet(self, parquet_path: str):
        self.session.query(f"""
                           INSERT INTO {self.db_name}.{self.table_name}
                           SELECT *
                           FROM file('{parquet_path}', Parquet)
                           """)

    @timer
    def test_get_events_number_by_day(self):
        result = self.session.query(f"""SELECT
            toDate(event_time) AS day,
            count(*) AS event_count
            FROM {self.db_name}.{self.table_name}
            GROUP BY day
            ORDER BY day;""")
        # print(f"Table {self.table_name} contents: \n{result}")

    @timer
    def test_get_events_sum(self):
        result = self.session.query(f"""SELECT
                event_date,
                SUM(float_00) AS sum_float_00,
                SUM(float_01) AS sum_float_01,
                SUM(float_02) AS sum_float_02,
                SUM(float_03) AS sum_float_03,
                SUM(float_04) AS sum_float_04,
                SUM(float_05) AS sum_float_05,
                SUM(float_06) AS sum_float_06,
                SUM(float_07) AS sum_float_07,
                SUM(float_08) AS sum_float_08,
                SUM(float_09) AS sum_float_09,
                SUM(float_10) AS sum_float_10,
                SUM(float_11) AS sum_float_11,
                SUM(float_12) AS sum_float_12,
                SUM(float_13) AS sum_float_13,
                SUM(float_14) AS sum_float_14,
                SUM(float_15) AS sum_float_15,
                SUM(float_16) AS sum_float_16,
                SUM(float_17) AS sum_float_17,
                SUM(float_18) AS sum_float_18,
                SUM(float_19) AS sum_float_19,
                SUM(float_20) AS sum_float_20,
                SUM(float_21) AS sum_float_21,
                SUM(float_22) AS sum_float_22,
                SUM(float_23) AS sum_float_23,
                SUM(float_24) AS sum_float_24,
                SUM(float_25) AS sum_float_25,
                SUM(float_26) AS sum_float_26,
                SUM(float_27) AS sum_float_27,
                SUM(float_28) AS sum_float_28,
                SUM(float_29) AS sum_float_29
            FROM {self.db_name}.{self.table_name}
            GROUP BY event_date
            ORDER BY event_date""")
        # print(f"Table {self.table_name} contents: \n{result}")

    @timer
    def test_select_date(self):
        query_result = self.session.query(f"""SELECT *
                        FROM {self.db_name}.{self.table_name}
                        WHERE event_date = '2024-12-31'
                        ORDER BY event_time;""")
