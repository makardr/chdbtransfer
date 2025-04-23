from util.wrap_timer import timer


class ClickhouseRemoteSource:

    def __init__(self, source):
        self.source = source

    @timer
    def read_clickhouse_db(self):
        query_result = self.source.query('SELECT * FROM default.events LIMIT 10')

        results = []
        column_names = query_result.column_names

        for row in query_result.result_rows:
            row_dict = {column_names[i]: value for i, value in enumerate(row)}
            results.append(row_dict)

        print(results)
        return results
