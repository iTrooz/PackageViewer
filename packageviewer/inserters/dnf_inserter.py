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
    def insert_distro(self, distro_name, distro_version):
        cursor = self.conn.execute('''
            INSERT INTO distro (name, version) VALUES (?, ?)
        ''', (distro_name, distro_version))
        self.conn.commit()
        self.cache_distro_id = cursor.lastrowid

    @timer.dec
    def insert_repo_table(self):
        self.conn.execute('''
            INSERT INTO repo(distro_id, name)
            SELECT DISTINCT ?, tmp_package.repo FROM tmp_package
        ''', (self.cache_distro_id,))

        self.conn.execute('''
            INSERT INTO tmp_repo(repo_id, name)
            SELECT DISTINCT repo.repo_id, repo.name FROM repo
            JOIN distro ON distro.distro_id = ?
        ''', (self.cache_distro_id,))
        self.conn.commit()

    @timer.dec
    def insert_package_table(self):
        self.conn.execute('''
            INSERT INTO package (name, repo_id)
            SELECT tmp_package.name, tmp_repo.repo_id FROM tmp_package
            JOIN tmp_repo
            ON tmp_repo.name = repo
        ''')
        self.conn.commit()

    @timer.dec
    def insert_dirname_table(self):
        self.conn.execute('''
            INSERT INTO dirname (dirname)
            SELECT DISTINCT dirname FROM tmp_file
        ''')
        self.conn.commit()

    @timer.dec
    def insert_filename_table(self):
        self.conn.execute('''
            INSERT INTO filename (filename)
            SELECT DISTINCT filename FROM tmp_file
        ''')
        self.conn.commit()

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
        self.conn.commit()

    def normalize(self, distro_name, distro_version):
        self.create_tables()

        self.insert_distro(distro_name, distro_version)

        self.insert_repo_table()

        self.insert_package_table()

        self.insert_dirname_table()
        self.insert_filename_table()

        self.insert_file_table()
