import argparse
import os
import sqlite3
import csv
import time

from tqdm import tqdm

from packageparse.distro_data import DistroData
from packageparse.data_outputs.csv_output import CSVOutput

SCRIPT_VERSION = "v1.0 beta"


# ----- FUNCTIONS


def import_csv(filepath, table, cursor):
    file = open(filepath, "r")
    csv_file = csv.reader(file)

    header = next(csv_file)

    fields_str = ', '.join(header)
    values_str = ', '.join("?"*len(header))
    insert_query = f"INSERT INTO {table} ({fields_str}) VALUES ({values_str})"

    cursor.executemany(insert_query, csv_file)

    file.close()

def read_csv(filepath):
    file = open(filepath, "r")
    csv_file = csv.reader(file)

    header = next(csv_file)

    return header, csv_file


def create_db(cursor):
    print("Creating DB..")

    STMTS = f'''
    CREATE TABLE distro(
        distro_id INT PRIMARY KEY,
        name TEXT
    );
    CREATE TABLE repo_tree(
        repo_parent_id INT,
        repo_id INT
    );
    CREATE TABLE package(
        package_id INT PRIMARY KEY,
        name TEXT,
        repo_id INT
    );
    CREATE TABLE file(
        package_id INT,
        dirname_id INT,
        filename_id INT
    );
    CREATE TABLE dirname(
        dirname_id INT PRIMARY KEY,
        dirname
    );
    CREATE TABLE filename(
        filename_id INT PRIMARY KEY,
        filename TEXT
    );
    CREATE TABLE path(
        dirname TEXT PRIMARY KEY
    );
    '''

    for stmt in STMTS.split(";"):
        cursor.execute(stmt)

    import_csv("path.csv", "path", cursor)

    print("Created DB !")

TMP_PACKAGE_TABLE_FIELDS = "distro_name, distro_version, distro_repo, name, arch, version, others"

def create_tmp_tables(cursor):
    STMTS = f'''
    CREATE TABLE IF NOT EXISTS tmp_package(
        {TMP_PACKAGE_TABLE_FIELDS}
    );
    CREATE TABLE IF NOT EXISTS tmp_file(
        package, repo, dirname, filename
    );
    '''

    for stmt in STMTS.split(";"):
        cursor.execute(stmt)


# ----- CODE


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


fields_package = "name, distro_name, distro_version, distro_repo, others, arch, version"
fields_package_file = "package, dirname, filename"

insert_package = f'''INSERT INTO package ({fields_package}) VALUES(?, ?, ?, ?, ?, ?, ?)'''
insert_package_file = f'''INSERT INTO package_file ({fields_package_file}) VALUES(?, ?, ?)'''

need_create_db = False
if os.path.exists(args.output_file):
    if args.recreate_db:
        os.remove(args.output_file)
        need_create_db = True
else:
    need_create_db = True

conn = sqlite3.connect(args.output_file)
cursor = conn.cursor()

if need_create_db:
    create_db(cursor)
    conn.commit() # Should not be needed because DDL but still

create_tmp_tables(cursor)

for filename in os.listdir(args.input_folder):
    base, ext = os.path.splitext(filename)
    if ext == ".csv":
        filepath = os.path.join(args.input_folder, filename)
        splitted_base = base.split("-")
        content = splitted_base[-1]
        repo = splitted_base[-2]

        if args.content and args.content != content:
            print(f"Skipping file {filepath}")
            continue

        print(f"Processing file {filepath} with repo {repo} and content {content}")

        fields, data = read_csv(filepath)
        fields_str = ', '.join(fields)
        placeholders_str= ', '.join("?"*len(fields))
        if content == "sums":
            insert_query = f'''INSERT INTO tmp_package ({fields_str}) VALUES ({placeholders_str})'''
        elif content == "files":
            insert_query = f'''INSERT INTO tmp_file ({fields_str}, repo) VALUES ({placeholders_str}, '{repo}')'''
        else:
            raise ValueError(f"Invalid content type: {type}")

        cursor.executemany(insert_query, tqdm(data))

conn.commit()


# fill 'repo_tree' table

cursor.execute('''
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
