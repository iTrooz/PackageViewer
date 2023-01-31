import sqlite3

from packageviewer.sql_table import SQLTable
from packageviewer.inserters.inserter import Inserter
import timer

class DnfInserter(Inserter):

    def __init__(self, conn) -> None:
        super().__init__(conn)

        self.table_tmp_package = SQLTable(conn=self.conn, table_name="tmp_package", create_query='''
            CREATE TEMPORARY TABLE IF NOT EXISTS tmp_package (pkgId, name, version, repo)
        ''')

        self.table_tmp_file = SQLTable(conn=self.conn, table_name="tmp_file", create_query='''
            CREATE TEMPORARY TABLE IF NOT EXISTS tmp_file (pkgId, dirname, filename, repo)
        ''')

        self.table_tmp_repo = SQLTable(conn=self.conn, table_name="tmp_repo", create_query='''
            CREATE TEMPORARY TABLE IF NOT EXISTS tmp_repo(repo_id INTEGER, name TEXT)
        ''')

    @timer.dec
    def insert_file_table(self):
        self.conn.execute('''
            INSERT INTO file (package_id, dirname_id, filename_id)
            SELECT package_id, dirname_id, filename_id FROM tmp_file
            JOIN tmp_package ON tmp_package.pkgId = tmp_file.pkgId
            JOIN package ON package.name = tmp_package.name
            JOIN tmp_repo ON tmp_repo.repo_id = package.repo_id
            JOIN dirname ON dirname.dirname = tmp_file.dirname
            JOIN filename ON filename.filename = tmp_file.filename
            WHERE tmp_file.repo = tmp_repo.name
        ''')
