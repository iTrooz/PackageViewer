import sqlite3

from packageviewer.sql_table import SQLTable
import timer

class AptInserter:
    
    def __init__(self, db_path) -> None:
        self.db_path = db_path
        self.conn = sqlite3.connect(self.db_path)

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
    def create_tables(self):

        STMTS = f'''
        CREATE TABLE distro(
            distro_id INTEGER PRIMARY KEY,
            name TEXT,
            version TEXT
        );
        CREATE TABLE repo(
            repo_id INTEGER PRIMARY KEY AUTOINCREMENT,
            distro_id INTEGER,
            name TEXT
        );
        CREATE TABLE package(
            package_id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            repo_id INTEGER
        );
        CREATE TABLE file(
            package_id INTEGER,
            dirname_id INTEGER,
            filename_id INTEGER
        );
        CREATE TABLE dirname(
            dirname_id INTEGER PRIMARY KEY AUTOINCREMENT,
            dirname
        );
        CREATE TABLE filename(
            filename_id INTEGER PRIMARY KEY AUTOINCREMENT,
            filename TEXT
        );
        '''

        for stmt in STMTS.split(";"):
            self.conn.execute(stmt)

    @timer.dec
    def insert_distro(self, distro_name, distro_version):

        cursor = self.conn.execute("INSERT INTO distro(name, version) VALUES (?, ?)", (distro_name, distro_version))
        self.conn.commit()
        self.cache_distro_id = cursor.lastrowid

    @timer.dec
    def insert_repos(self, repo_list):
        self.cache_repo_ids = {}

        for repo in repo_list:
            cursor = self.conn.execute("INSERT INTO repo(name, distro_id) VALUES (?, ?)", (repo, self.cache_distro_id))
            self.cache_repo_ids[repo] = cursor.lastrowid
        self.conn.commit()

    @timer.dec
    def insert_packages(self, package_list):
        self.cache_package_ids = {}
        for package in package_list:
            cursor = self.conn.execute(
                "INSERT INTO package(name, repo_id) VALUES (?, ?)",
                (package["name"], self.cache_repo_ids[package["distro_repo"]])
            )
            self.cache_package_ids[package["name"]] = cursor.lastrowid
        self.conn.commit()

    @timer.dec
    def insert_dirnames(self, dirnames_list):
        self.cache_dirname_ids = {}
        for dirname in dirnames_list:
            cursor = self.conn.execute("INSERT INTO dirname(dirname) VALUES (?)", (dirname,))
            self.cache_dirname_ids[dirname] = cursor.lastrowid
        self.conn.commit()

    @timer.dec
    def insert_filenames(self, filenames_list):
        self.cache_filename_ids = {}
        for filename in filenames_list:
            cursor = self.conn.execute("INSERT INTO filename(filename) VALUES (?)", (filename,))
            self.cache_filename_ids[filename] = cursor.lastrowid
        self.conn.commit()

    @timer.dec
    def insert_files(self, files_list):
        self.cache_filename_ids = {}
        for file in files_list:
            dirname = file["dirname"]
            filename = file["filename"]
            self.conn.execute(
                "INSERT INTO file(dirname_id, filename_id) VALUES (?)", (
                self.cache_dirname_ids[dirname],
                self.cache_filename_ids[filename]
            ))
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

