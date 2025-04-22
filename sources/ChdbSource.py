from chdb.session import Session


class ChdbSource:

    def __init__(self, table_name: str):
        self.db_name = "mydb"
        self.table_name = table_name
        self.session = Session("/opt/chdb_data")

    def create_table(self):
        self.session.query(f"CREATE DATABASE IF NOT EXISTS mydb ENGINE=Atomic")
        self.session.query(f"USE {self.db_name}")
        self.session.query(f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            id UInt32,
            name String
            ) ENGINE = MergeTree()
            ORDER BY id
        """)

    def write_string(self):
        print("Writing string")
        self.session.query(f"""
            INSERT INTO {self.table_name} (*) VALUES
                (1, 'string1'),
                (2, 'string2'),
                (3, 'string3')
        """)
        print("Finished writing string")

    def select_one_test(self):
        self.session.query("SELECT 1;")

    def read_table(self):
        result = self.session.query(f"SELECT * FROM {self.table_name};")
        print(f"Table {self.table_name} contents: \n{result}")
