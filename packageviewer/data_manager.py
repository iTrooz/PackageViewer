import os
import sqlite3

from packageviewer.processors.apt_processor import AptProcessor
from packageviewer.processors.dnf_processor import DnfProcessor
from packageviewer.processors.pacman_processor import PacmanProcessor
import timer


class DataManager:

    def __init__(self, db_filepath):
        self.db_filepath = db_filepath
        self.conn = sqlite3.connect(db_filepath)
        
    def __get_processor_class__(self, distro_name):
        match distro_name:
            case "debian": return AptProcessor
            case "ubuntu": return AptProcessor
            case "fedora": return DnfProcessor
            case "archlinux": return PacmanProcessor
            case _: raise ValueError("Unknown distribution: "+distro_name)

    def process_data_point(self, distro_name, distro_version, dir_path):
        ProcessorClass = self.__get_processor_class__(distro_name)
        print(f"Using processor class '{ProcessorClass.__name__}' for {distro_name}:{distro_version}")
        
        parser = ProcessorClass(distro_name, distro_version, dir_path, self.conn)
        parser.process()

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
    def add_indexes(self):
        self.conn.execute('''CREATE INDEX "index-dirname-dirname" ON "dirname" ("dirname")''')
        self.conn.execute('''CREATE INDEX "index-filename-filename" ON "filename" ("filename")''')
        self.conn.execute('''CREATE INDEX "index-file-dirname_id" ON "file" ("dirname_id")''')
        self.conn.execute('''CREATE INDEX "index-file-filename_id" ON "file" ("filename_id")''')
        self.conn.execute('''CREATE INDEX "index-file-package_id" ON "file" ("package_id")''')
        self.conn.execute('''CREATE INDEX "index-package-name" ON "package" ("name")''')
