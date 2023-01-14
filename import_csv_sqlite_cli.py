import argparse
import os
import sqlite3
import csv
import time

from tqdm import tqdm

from packageparse.distro_data import DistroData
from packageparse.data_outputs.csv_output import CSVOutput

SCRIPT_VERSION = "v1.0 beta"

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

if args.recreate_db:
    if os.path.exists(args.output_file):
        os.remove(args.output_file)

conn = sqlite3.connect(args.output_file)
cursor = conn.cursor()

fields_package = "name, distro_name, distro_version, distro_repo, others, arch, version"
fields_package_file = "package, dirname, filename"

cursor.execute(f'''CREATE TABLE IF NOT EXISTS package({fields_package})''')
cursor.execute(f'''CREATE TABLE IF NOT EXISTS package_file({fields_package_file})''')

insert_package = f'''INSERT INTO package ({fields_package}) VALUES(?, ?, ?, ?, ?, ?, ?)'''
insert_package_file = f'''INSERT INTO package_file ({fields_package_file}) VALUES(?, ?, ?)'''

def get_type(filename):
    return os.filename

def __import_csv__timer__(func):
    def decorator(dirname, filename, type):
        print(f"Importing file {filename}..")
        start = time.time()
        func(dirname, filename, type)
        end = time.time()
        print(f"Finished ! ({(end-start):.4f}s)")
    return decorator

@__import_csv__timer__
def import_csv(dirname, filename, type):
    file = open(os.path.join(dirname, filename), "r")
    csv_file = csv.reader(file)

    next(csv_file) # Skip header
    
    if args.content and args.content != type:
        return
    if type == "sums":
        cursor.executemany(insert_package, tqdm(csv_file))
    elif type == "files":
        cursor.executemany(insert_package_file, tqdm(csv_file))
    else:
        raise ValueError(f"Invalid type: {type}")


    file.close()

for filename in os.listdir(args.input_folder):
    base, ext = os.path.splitext(filename)
    if ext == ".csv":
        type = base.split("-")[-1]
        import_csv(args.input_folder, filename, type)

conn.commit()