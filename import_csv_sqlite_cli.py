import argparse
import os
import sqlite3
import csv

from tqdm import tqdm
from pydpkg import Dpkg

from packageparse.distro_data import DistroData
from packageparse.data_outputs.csv_output import CSVOutput
import timer

SCRIPT_VERSION = "v1.0 beta"

class CSVImporter:

    def __init__(self, db_filepath):
        self.db_filepath = db_filepath
        self.connect_db()

    def connect_db(self):
        self.conn = sqlite3.connect(self.db_filepath)
        self.cursor = self.conn.cursor()

    def disconnect_db(self):
        self.conn.close()
        self.conn = None
        self.cursor = None

        

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


    @timer.dec
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
            self.cursor.execute(stmt)

    @timer.dec
    def import_csvs_in_tmp_tables(self, input_folder, content_filter=None, dedup_sums=True):
        sums_files = []
        files_files = []
        for filename in os.listdir(input_folder):
            base, ext = os.path.splitext(filename)
            if ext == ".csv":
                splitted_base = base.split("-")
                content = splitted_base[-1]
                repo = splitted_base[-2]

                if content_filter and args.content != content_filter:
                    print(f"Skipping file {filename}")
                    continue

                if content == "sums":
                    l = sums_files
                elif content == "files":
                    l = files_files
                else:
                    raise ValueError(f"Invalid content type: {content}")
                l.append((os.path.join(input_folder, filename), repo))

        self.import_sums_csvs_in_tmp_table(sums_files, dedup_sums)
        self.import_files_csvs_in_tmp_table(files_files)
      

    def import_sums_csvs_in_tmp_table(self, sums_files, dedup_sums):
        sums_data = []

        for filepath, repo in sums_files:
            print(f"Processing file {filepath} with repo {repo} and content sums")

            f = open(filepath)
            sums_data.extend(csv.DictReader(f))
            f.close()
        
        sums_data = sorted(sums_data, key=lambda d: d["name"])

        if dedup_sums:
            dedup_sums_data = []
            pkg_sum = {"name": None}
            sums_data.append({"name": None})
            for loop_sum in sums_data:
                if loop_sum["name"] == pkg_sum["name"]:
                    if Dpkg.compare_versions(loop_sum["version"], pkg_sum["version"]) == 1:
                        pkg_sum = loop_sum
                else:
                    if pkg_sum["name"] != None:
                        dedup_sums_data.append(pkg_sum)
                    pkg_sum = loop_sum
            sums_data = dedup_sums_data

        fields = sums_data[0].keys()
        fields_str = ', '.join(fields)
        placeholders_str= ', '.join("?"*len(fields))

        insert_query = f'''INSERT INTO tmp_package ({fields_str}) VALUES ({placeholders_str})'''
        
        insert_data = (list(sum.values()) for sum in sums_data)
        self.cursor.executemany(insert_query, tqdm(insert_data))
        self.conn.commit()

    def import_files_csvs_in_tmp_table(self, files_files):
        for filepath, repo in files_files:
            print(f"Processing file {filepath} with repo {repo} and content files")
            
            fields, data = self.read_csv(filepath)
            fields_str = ', '.join(fields)
            placeholders_str= ', '.join("?"*len(fields))

            insert_query = f'''INSERT INTO tmp_file ({fields_str}, repo) VALUES ({placeholders_str}, '{repo}')'''

            self.cursor.executemany(insert_query, tqdm(data))

        self.conn.commit()



    @timer.dec
    def insert_distro_table(self):
        self.cursor.execute('''
            INSERT INTO distro (name, version)
            SELECT DISTINCT distro_name, distro_version FROM tmp_package
        ''')
        self.conn.commit()

    @timer.dec
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

    @timer.dec
    def insert_package_table(self):
        self.cursor.execute('''
            INSERT INTO package (name, repo_id)
            SELECT tmp_package.name, tmp_repo.repo_id FROM tmp_package
            JOIN tmp_repo
            ON tmp_repo.name = SUBSTR(distro_repo, 0, INSTR(distro_repo, '/'))
        ''')
        self.conn.commit()

    @timer.dec
    def insert_dirname_table(self):
        self.cursor.execute('''
            INSERT INTO dirname (dirname)
            SELECT DISTINCT dirname FROM tmp_file
        ''')
        self.conn.commit()

    @timer.dec
    def insert_filename_table(self):
        self.cursor.execute('''
            INSERT INTO filename (filename)
            SELECT DISTINCT filename FROM tmp_file
        ''')
        self.conn.commit()

    @timer.dec
    def insert_file_table(self):
        self.cursor.execute('''
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
        self.cursor.execute("VACUUM")
        self.conn.commit()

    @timer.dec
    def add_indexes(self):
        self.cursor.execute('''CREATE INDEX "index-dirname-dirname" ON "dirname" ("dirname")''')
        self.cursor.execute('''CREATE INDEX "index-filename-filename" ON "filename" ("filename")''')
        self.cursor.execute('''CREATE INDEX "index-file-dirname_id" ON "file" ("dirname_id")''')
        self.cursor.execute('''CREATE INDEX "index-file-filename_id" ON "file" ("filename_id")''')
        self.cursor.execute('''CREATE INDEX "index-file-package_id" ON "file" ("package_id")''')
        self.cursor.execute('''CREATE INDEX "index-package-name" ON "package" ("name")''')







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

importer = CSVImporter(args.output_file)

if need_create_db:
    importer.create_db()

importer.create_tmp_tables()

importer.import_csvs_in_tmp_tables(args.input_folder, args.content)

importer.insert_distro_table()

importer.insert_repo_table()

importer.insert_package_table()

importer.insert_dirname_table()
importer.insert_filename_table()

importer.add_indexes()

importer.insert_file_table()
