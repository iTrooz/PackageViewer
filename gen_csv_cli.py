import argparse
import os

from data_manager import DataManager
from distro_data import DistroData
from data_outputs.csv_output import CSVOutput

SCRIPT_VERSION = "v1.0 beta"
data_manager = DataManager()

parser = argparse.ArgumentParser(
    prog = "gencsv",
    description = "generate CSV",
    epilog=f"script version {SCRIPT_VERSION}",
)

parser.add_argument("-d", "--distro", required=False,
    help = "Set the distribution")
parser.add_argument("-v", "--version", required=False,
    help = "Set the distribution version")
parser.add_argument("-r", "--repo", required=False,
    help = "Set the distribution repository. Separate sub-repositories with '/'. Starting with '/' means using the default repository, if there is one")
parser.add_argument("-c", "--content", required=False, default='all', choices=("sums","files","all"),
    help = "What content to parse. Accepted values : sums|files|all")
parser.add_argument("-o", "--output-folder", required=False, default="output-csv",
    help = "Set the output folder for the CSV files")

args = parser.parse_args()

# TODO maybe deal with that better ?
if args.repo and args.repo[0] == "/":
    args.repo = "DEFREPO"+args.repo

print(f"Operation requested : parse content '{args.content}' from repository '{args.distro}/{args.version}/{args.repo}'")

def create_output(distro_data: DistroData):
    filename = "-".join((
        distro_data.name,
        distro_data.version,
        distro_data.version,
        distro_data.repo.replace("/", "_"),
        distro_data.content
        )) + ".csv"
    return CSVOutput(os.path.join(args.output_folder, filename))

data_manager.parse_multiple_data(DistroData(
    name=args.distro, version=args.version, repo=args.repo, content=args.content
), create_output)

print("Finished !")
