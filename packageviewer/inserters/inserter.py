import sqlite3

import timer

class Inserter:
    
    def __init__(self, conn) -> None:
        self.conn = conn
        
    @timer.dec
    def create_tables(self):
        self.conn.executescript('''
        CREATE TABLE IF NOT EXISTS distro(
            distro_id INTEGER PRIMARY KEY,
            name TEXT,
            version TEXT
        );
        CREATE TABLE IF NOT EXISTS repo(
            repo_id INTEGER PRIMARY KEY AUTOINCREMENT,
            distro_id INTEGER,
            name TEXT
        );
        CREATE TABLE IF NOT EXISTS package(
            package_id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            repo_id INTEGER
        );
        CREATE TABLE IF NOT EXISTS file(
            package_id INTEGER,
            dirname_id INTEGER,
            filename_id INTEGER
        );
        CREATE TABLE IF NOT EXISTS dirname(
            dirname_id INTEGER PRIMARY KEY AUTOINCREMENT,
            dirname
        );
        CREATE TABLE IF NOT EXISTS filename(
            filename_id INTEGER PRIMARY KEY AUTOINCREMENT,
            filename TEXT
        );
        ''')
    
    @timer.dec
    def insert_distro(self, distro_name, distro_version):
        cursor = self.conn.execute('''
            INSERT INTO distro (name, version) VALUES (?, ?)
        ''', (distro_name, distro_version))
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

    @timer.dec
    def insert_package_table(self):
        self.conn.execute('''
            INSERT INTO package (name, repo_id)
            SELECT tmp_package.name, tmp_repo.repo_id FROM tmp_package
            JOIN tmp_repo
            ON tmp_repo.name = repo
        ''')

    @timer.dec
    def insert_dirname_table(self):
        self.conn.execute('''
            INSERT INTO dirname (dirname)
            SELECT DISTINCT tmp_file.dirname FROM tmp_file
            LEFT JOIN dirname ON dirname.dirname = tmp_file.dirname
            WHERE dirname.dirname_id is null
        ''')

    @timer.dec
    def insert_filename_table(self):
        self.conn.execute('''
            INSERT INTO filename (filename)
            SELECT DISTINCT tmp_file.filename FROM tmp_file
            LEFT JOIN filename ON filename.filename = tmp_file.filename
            WHERE filename.filename_id is null
        ''')

    @timer.dec
    def insert_file_table(self):
        self.conn.execute('''
            INSERT INTO file (package_id, dirname_id, filename_id)
            SELECT package_id, dirname_id, filename_id FROM tmp_file
            -- join package
            JOIN package ON package.name = tmp_file.package
            -- filter to keep our packages only
            JOIN tmp_repo ON tmp_repo.repo_id = package.repo_id
            JOIN distro ON tmp_repo.repo_id = package.repo_id
            -- join dirname
            JOIN dirname ON dirname.dirname = tmp_file.dirname
            -- join filename
            JOIN filename ON filename.filename = tmp_file.filename
        ''')

    @timer.dec
    def commit(self):
        self.conn.commit()

    def normalize(self, distro_name, distro_version):
        self.create_tables()

        self.insert_distro(distro_name, distro_version)

        self.insert_repo_table()

        self.insert_package_table()

        self.insert_dirname_table()
        self.insert_filename_table()

        self.insert_file_table()

        self.commit()
