from util.wrap_timer import timer


# noinspection SqlNoDataSourceInspection
class ClickhouseRemoteSource:

    def __init__(self, session, db_name, table_name):
        self.session = session
        self.db_name = db_name
        self.table_name = table_name

    @timer
    def read_clickhouse_db(self):
        query_result = self.session.query(f"SELECT * FROM {self.db_name}.{self.table_name} LIMIT 10000")

        results = []
        column_names = query_result.column_names

        for row in query_result.result_rows:
            row_dict = {column_names[i]: value for i, value in enumerate(row)}
            results.append(row_dict)

        # print(results)
        return results

    @timer
    def test_get_events_number_by_day(self):
        query_result = self.session.query(f"""SELECT
            toDate(event_time) AS day,
            count(*) AS event_count
            FROM {self.db_name}.{self.table_name}
            GROUP BY day
            ORDER BY day;""")
        #
        # results = []
        # column_names = query_result.column_names
        #
        # for row in query_result.result_rows:
        #     row_dict = {column_names[i]: value for i, value in enumerate(row)}
        #     results.append(row_dict)
        #
        # for event in results:
        #     print(f"{event}\n")
        # print(results)

    @timer
    def test_select_date(self):
        query_result = self.session.query(f"""SELECT *
            FROM {self.db_name}.{self.table_name}
            WHERE event_date = '2024-12-31'
            ORDER BY event_time;""")

        # results = []
        # column_names = query_result.column_names
        #
        # for row in query_result.result_rows:
        #     row_dict = {column_names[i]: value for i, value in enumerate(row)}
        #     results.append(row_dict)
        #
        # for event in results:
        #     print(f"{event}\n")


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