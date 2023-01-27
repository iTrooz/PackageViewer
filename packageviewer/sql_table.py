class SQLTable:

    def __init__(self, conn, table_name: str, create_query: str) -> None:
        self.conn = conn
        self.table_name = table_name
        self.conn.execute(create_query)

    def add_rows(self, rows: list):
        fields = ", ".join(rows[0].keys())
        values = ", ".join("?"*len(rows[0]))
        sql = f"INSERT INTO {self.table_name} ({fields}) VALUES ({values})"
        
        insert_data = (list(row.values()) for row in rows)
        self.conn.executemany(sql, insert_data)
        self.conn.commit()
