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


def __import_csv__timer__(func):
    def decorator(filepath, table, cursor):
        print(f"Importing file '{filepath}' in table '{table}'..")
        start = time.time()
        func(filepath, table, cursor)
        end = time.time()
        print(f"Finished importing file ! ({(end-start):.4f}s)")
    return decorator

@__import_csv__timer__
def import_csv(filepath, table, cursor):
    file = open(filepath, "r")
    csv_file = csv.reader(file)

    header = next(csv_file)

    fields_str = ', '.join(header)
    values_str = ', '.join("?"*len(header))
    insert_query = f"INSERT INTO {table} ({fields_str}) VALUES ({values_str})"

    cursor.executemany(insert_query, csv_file)

    file.close()


def create_db(cursor):
    print("Creating DB..")

    cursor.execute(f'''CREATE TABLE path(dirname)''')
    import_csv("path.csv", "path", cursor)

    cursor.execute(f'''CREATE TABLE package({fields_package})''')
    cursor.execute(f'''CREATE TABLE package_file({fields_package_file})''')

    print("Created DB !")


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


for filename in os.listdir(args.input_folder):
    base, ext = os.path.splitext(filename)
    if ext == ".csv":
        filepath = os.path.join(args.input_folder, filename)
        content = base.split("-")[-1]

        if args.content and args.content != content:
            print(f"Skipping file {filepath}")
            continue

        if content == "sums":
            table = "package"
        elif content == "files":
            table = "package_file"
        else:
            raise ValueError(f"Invalid content type: {type}")

        import_csv(filepath, table, cursor)

conn.commit()