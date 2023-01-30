import sqlite3

import timer

class Inserter:
    
    def __init__(self, db_path) -> None:
        self.db_path = db_path
        self.conn = sqlite3.connect(self.db_path)
    
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
