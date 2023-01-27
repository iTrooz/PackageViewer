import argparse
import os

from packageparse.data_manager import DataManager
from packageparse.distro_data import DistroData
import timer

SCRIPT_VERSION = "v1.0 beta"
data_manager = DataManager()

parser = argparse.ArgumentParser(
    prog = "processdata",
    description = "Process data",
    epilog=f"script version {SCRIPT_VERSION}",
)

parser.add_argument("-d", "--distro", required=False,
    help = "Set the distribution")
parser.add_argument("-v", "--version", required=False,
    help = "Set the distribution version")
parser.add_argument("-o", "--output-file", required=False, default="out.db",
    help = "Set the output folder for the CSV files")
parser.add_argument("--reset-db", required=False, default=False, action="store_true",
    help = "Reset database if already existing")

args = parser.parse_args()

if args.reset_db:
    print("Resetting DB")
    if os.path.exists(args.output_file):
        os.remove(args.output_file)

print(f"Operation requested : process '{args.distro}/{args.version}'")

timer.call(
    data_manager.process_data_point, args.distro, args.version, "archives/ubuntu/22", args.output_file
)

print("Finished !")
