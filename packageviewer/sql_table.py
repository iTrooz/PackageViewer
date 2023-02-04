import itertools

class SQLTable:

    def __init__(self, conn, table_name: str, create_query: str) -> None:
        self.conn = conn
        self.table_name = table_name
        self.conn.execute(create_query)

    def _get_sql_query_(self, keys):
        fields = ", ".join(keys)
        values = ", ".join("?"*len(keys))
        return f"INSERT INTO {self.table_name} ({fields}) VALUES ({values})"


    def add_row(self, row: dict):
        sql = self._get_sql_query_(row.keys())
        self.conn.execute(sql, list(row.values()))


    def add_rows(self, rows): # 'rows' is iterable of dict
        first_row = next(iter(rows), None)
        
        if first_row == None: # empty iterator
            return

        sql = self._get_sql_query_(first_row.keys())

        insert_data = (list(row.values()) for row in itertools.chain((first_row,), rows))
        self.conn.executemany(sql, insert_data)
