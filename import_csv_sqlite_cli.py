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
            name TEXT,
            version TEXT
        );
        CREATE TABLE repo_tree(
            repo_parent_id INTEGER,
            repo_id INTEGER
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
        '''

        for stmt in STMTS.split(";"):
            self.cursor.execute(stmt)


    def read_csvs_in_tmp_tables(self, input_folder, content_filter):
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


    def insert_repo_tree_table(self):
        self.cursor.execute('''
            WITH split(parent_word, word, csv) AS (
            SELECT 
                null,
                null,
                distro_repo||'/'
                FROM (
                    SELECT DISTINCT distro_repo FROM tmp_package
                )
            UNION ALL SELECT
                word,
                substr(csv, 0, instr(csv, '/')),
                substr(csv, instr(csv, '/') + 1)
            FROM split
            WHERE csv != ''
            )
            INSERT INTO repo_tree
            SELECT DISTINCT parent_word, word FROM split 
            WHERE parent_word is not null
        ''')
        self.conn.commit()

    def insert_distro_table(self):
        self.cursor.execute('''
            INSERT INTO distro (name, version)
            SELECT DISTINCT distro_name, distro_version FROM tmp_package
        ''')
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

importer.read_csvs_in_tmp_tables(args.input_folder, args.content)

importer.insert_repo_tree_table()

importer.insert_distro_table()
