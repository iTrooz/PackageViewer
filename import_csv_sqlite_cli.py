import argparse
import os
import sqlite3
import csv
import time

from tqdm import tqdm

from packageparse.distro_data import DistroData
from packageparse.data_outputs.csv_output import CSVOutput

SCRIPT_VERSION = "v1.0 beta"

class CSVImporter:

    def __init__(self, conn):
        self.conn = conn
        self.cursor = conn.cursor()
        

    def import_csv(self, filepath, table):
        file = open(filepath, "r")
        csv_file = csv.reader(file)

        header = next(csv_file)

        fields_str = ', '.join(header)
        values_str = ', '.join("?"*len(header))
        insert_query = f"INSERT INTO {table} ({fields_str}) VALUES ({values_str})"

        self.cursor.executemany(insert_query, csv_file)

        file.close()

    def read_csv(self, filepath):
        file = open(filepath, "r")
        csv_file = csv.reader(file)

        header = next(csv_file)

        return header, csv_file


    def create_db(self):
        print("Creating DB..")

        STMTS = f'''
        CREATE TABLE distro(
            distro_id INTEGER PRIMARY KEY,
            repo_id INTEGER,
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
        CREATE TABLE path(
            dirname TEXT PRIMARY KEY
        );
        '''

        for stmt in STMTS.split(";"):
            self.cursor.execute(stmt)

        self.import_csv("path.csv", "path")

        print("Created DB !")


    def create_tmp_tables(self):
        
        TMP_PACKAGE_TABLE_FIELDS = "distro_name, distro_version, distro_repo, name, arch, version, others"
        
        STMTS = f'''
        CREATE TABLE IF NOT EXISTS tmp_package(
            {TMP_PACKAGE_TABLE_FIELDS}
        );
        CREATE TABLE IF NOT EXISTS tmp_file(
            package, repo, dirname, filename
        );
        CREATE TABLE tmp_repo(
            repo_id INTEGER PRIMARY KEY,
            name TEXT
        );
        '''

        for stmt in STMTS.split(";"):
            self.cursor.execute(stmt)


    def import_csvs_in_tmp_tables(self, input_folder, content_filter):
        for filename in os.listdir(input_folder):
            base, ext = os.path.splitext(filename)
            if ext == ".csv":
                filepath = os.path.join(input_folder, filename)
                splitted_base = base.split("-")
                content = splitted_base[-1]
                repo = splitted_base[-2]

                if content_filter and args.content != content_filter:
                    print(f"Skipping file {filepath}")
                    continue

                print(f"Processing file {filepath} with repo {repo} and content {content}")

                fields, data = self.read_csv(filepath)
                fields_str = ', '.join(fields)
                placeholders_str= ', '.join("?"*len(fields))
                if content == "sums":
                    insert_query = f'''INSERT INTO tmp_package ({fields_str}) VALUES ({placeholders_str})'''
                elif content == "files":
                    insert_query = f'''INSERT INTO tmp_file ({fields_str}, repo) VALUES ({placeholders_str}, '{repo}')'''
                else:
                    raise ValueError(f"Invalid content type: {type}")

                self.cursor.executemany(insert_query, tqdm(data))

        self.conn.commit()


    def insert_distro_table(self):
        self.cursor.execute('''
            INSERT INTO distro (name, version)
            SELECT DISTINCT distro_name, distro_version FROM tmp_package
        ''')
        self.conn.commit()

    def insert_repo_table(self):
        self.cursor.execute('''
           INSERT INTO repo(distro_id, name)
            SELECT DISTINCT distro.distro_id, SUBSTR(distro_repo, 0, INSTR(distro_repo, '/')) FROM tmp_package
            JOIN distro
            ON distro.name = tmp_package.distro_name
            AND distro.version = tmp_package.distro_version
        ''')

        self.cursor.execute('''
            INSERT INTO tmp_repo(repo_id, name)
            SELECT DISTINCT repo.repo_id, repo.name FROM repo
            JOIN distro ON distro.distro_id = repo.distro_id
            JOIN tmp_package ON
            tmp_package.distro_name = distro.name
            AND tmp_package.distro_version = distro.version
        ''')
        self.conn.commit()

    def insert_package_table(self):
        self.cursor.execute('''
            INSERT INTO package (name, repo_id)
            SELECT tmp_package.name, tmp_repo.repo_id FROM tmp_package
            JOIN tmp_repo
            ON tmp_repo.name = SUBSTR(distro_repo, 0, INSTR(distro_repo, '/'))
        ''')
        self.conn.commit()

    def insert_dirname_table(self):
        self.cursor.execute('''
            INSERT INTO dirname (dirname)
            SELECT DISTINCT dirname FROM tmp_file
        ''')
        self.conn.commit()

    def insert_filename_table(self):
        self.cursor.execute('''
            INSERT INTO filename (filename)
            SELECT DISTINCT filename FROM tmp_file
        ''')
        self.conn.commit()

    def insert_file_table(self):
        self.cursor.execute('''
            -- Insert file table
            INSERT INTO file (package_id, dirname_id, filename_id)
            SELECT package_id, dirname_id, filename_id FROM tmp_file
            -- get all packages
            JOIN package ON package.name = tmp_file.package
            -- filter by our repos
            JOIN tmp_repo ON tmp_repo.repo_id = package.repo_id
            -- join dirname
            JOIN dirname ON dirname.dirname = tmp_file.dirname
            -- join filename
            JOIN filename ON filename.filename = tmp_file.filename
        ''')
        self.conn.commit()

    def delete_tmp_tables(self):
        self.cursor.execute("DELETE TABLE tmp_package")
        self.cursor.execute("DELETE TABLE tmp_file")
        self.cursor.execute("DELETE TABLE tmp_repo")
        self.conn.commit()

    def vacuum_db(self):
        self.cursor.execute("VACUUM")
        self.conn.commit()







# ---------- CODE

parser = argparse.ArgumentParser(
    prog = "importcsv",
    description = "import CSV files in the database",
    epilog=f"script version {SCRIPT_VERSION}",
)

parser.add_argument("--recreate-db", required=False, action="store_true",
    help = "Recreate the database before importing")


parser.add_argument("-c", "--content", required=False, default=None, choices=("sums", "files"),
    help = "")

parser.add_argument("-i", "--input-folder", required=False, default="output-csv",
    help = "Input folder to read the CSV files from")
parser.add_argument("-o", "--output-file", required=False, default="output.db",
    help = "Output SQLite db file")

args = parser.parse_args()



need_create_db = False
if os.path.exists(args.output_file):
    if args.recreate_db:
        os.remove(args.output_file)
        need_create_db = True
else:
    need_create_db = True

conn = sqlite3.connect(args.output_file)
importer = CSVImporter(conn)

if need_create_db:
    importer.create_db()

importer.create_tmp_tables()

importer.import_csvs_in_tmp_tables(args.input_folder, args.content)

importer.insert_distro_table()

importer.insert_repo_table()

importer.insert_package_table()

importer.insert_dirname_table()
importer.insert_filename_table()

importer.insert_file_table()

importer.delete_tmp_tables()

importer.vacuum_db()
