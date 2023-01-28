import sqlite3

from packageviewer.sql_table import SQLTable
import timer

class DnfInserter:

    def __init__(self, db_path) -> None:
        self.db_path = db_path
        self.conn = sqlite3.connect(self.db_path)
        print("Opening db "+self.db_path)

        self.table_tmp_package = SQLTable(conn=self.conn, table_name="tmp_package", create_query='''
            CREATE TABLE IF NOT EXISTS tmp_package (pkgId, name, version)
        ''')
    