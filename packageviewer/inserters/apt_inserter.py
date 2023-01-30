import sqlite3

from packageviewer.sql_table import SQLTable
from packageviewer.inserters.inserter import Inserter
import timer

class AptInserter(Inserter):
    
    def __init__(self, db_path) -> None:
        super().__init__(db_path)

        self.table_tmp_package = SQLTable(conn=self.conn, table_name="tmp_package", create_query='''
            CREATE TEMPORARY TABLE IF NOT EXISTS tmp_package (distro_name, distro_version, distro_repo, name, arch, version, others)
        ''')

        self.table_tmp_file = SQLTable(conn=self.conn, table_name="tmp_file", create_query='''
            CREATE TEMPORARY TABLE IF NOT EXISTS tmp_file (package, repo, dirname, filename)
        ''')
        self.table_tmp_repo = SQLTable(conn=self.conn, table_name="tmp_repo", create_query='''
            CREATE TEMPORARY TABLE IF NOT EXISTS tmp_repo(repo_id INTEGER, name TEXT)
        ''')

    def normalize(self):
        self.create_tables()

        self.insert_distro_table()

        self.insert_repo_table()

        self.insert_package_table()

        self.insert_dirname_table()
        self.insert_filename_table()

        self.insert_file_table()

        self.add_indexes()

    @timer.dec
    def create_tmp_tables(self):
        
        TMP_PACKAGE_TABLE_FIELDS = "distro_name, distro_version, distro_repo, name, arch, version, others"
        
        STMTS = f'''
        CREATE TEMPORARY TABLE IF NOT EXISTS tmp_package(
            {TMP_PACKAGE_TABLE_FIELDS}
        );
        CREATE TEMPORARY TABLE IF NOT EXISTS tmp_file(
            package, repo, dirname, filename
        );
        CREATE TEMPORARY TABLE IF NOT EXISTS tmp_repo(
            repo_id INTEGER PRIMARY KEY,
            name TEXT
        );
        '''

        for stmt in STMTS.split(";"):
            self.conn.execute(stmt)

    @timer.dec
    def insert_distro_table(self):
        self.conn.execute('''
            INSERT INTO distro (name, version)
            SELECT DISTINCT distro_name, distro_version FROM tmp_package
        ''')
        self.conn.commit()

    @timer.dec
    def insert_repo_table(self):
        self.conn.execute('''
           INSERT INTO repo(distro_id, name)
            SELECT DISTINCT distro.distro_id, distro_repo FROM tmp_package
            JOIN distro
            ON distro.name = tmp_package.distro_name
            AND distro.version = tmp_package.distro_version
        ''')

        self.conn.execute('''
            INSERT INTO tmp_repo(repo_id, name)
            SELECT DISTINCT repo.repo_id, repo.name FROM repo
            JOIN distro ON distro.distro_id = repo.distro_id
            JOIN tmp_package ON
            tmp_package.distro_name = distro.name
            AND tmp_package.distro_version = distro.version
        ''')
        self.conn.commit()

    @timer.dec
    def insert_package_table(self):
        self.conn.execute('''
            INSERT INTO package (name, repo_id)
            SELECT tmp_package.name, tmp_repo.repo_id FROM tmp_package
            JOIN tmp_repo
            ON tmp_repo.name = distro_repo
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
            JOIN package ON package.name = tmp_file.package
            JOIN tmp_repo ON tmp_repo.repo_id = package.repo_id
            JOIN dirname ON dirname.dirname = tmp_file.dirname
            JOIN filename ON filename.filename = tmp_file.filename
            WHERE tmp_file.repo = tmp_repo.name
        ''')
        self.conn.commit()

    @timer.dec
    def vacuum_db(self):
        self.conn.execute("VACUUM")
        self.conn.commit()

    @timer.dec
    def add_indexes(self):
        self.conn.execute('''CREATE INDEX "index-dirname-dirname" ON "dirname" ("dirname")''')
        self.conn.execute('''CREATE INDEX "index-filename-filename" ON "filename" ("filename")''')
        self.conn.execute('''CREATE INDEX "index-file-dirname_id" ON "file" ("dirname_id")''')
        self.conn.execute('''CREATE INDEX "index-file-filename_id" ON "file" ("filename_id")''')
        self.conn.execute('''CREATE INDEX "index-file-package_id" ON "file" ("package_id")''')
        self.conn.execute('''CREATE INDEX "index-package-name" ON "package" ("name")''')

