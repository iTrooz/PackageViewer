import itertools

class SQLTable:

    def __init__(self, conn, table_name: str, create_query: str) -> None:
        self.conn = conn
        self.table_name = table_name
        self.conn.execute(create_query)

    def add_rows(self, rows: list):
        first_row = next(iter(rows))
        fields = ", ".join(first_row.keys())
        values = ", ".join("?"*len(first_row))
        sql = f"INSERT INTO {self.table_name} ({fields}) VALUES ({values})"

        insert_data = (list(row.values()) for row in itertools.chain((first_row,), rows))
        self.conn.executemany(sql, insert_data)
