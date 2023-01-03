import argparse
import sys

from data_manager import DataManager
import database

SCRIPT_VERSION = "v1.0 beta"
data_manager = DataManager()

parser = argparse.ArgumentParser(
                    prog = "ProgramName",
                    description = "add Archive data to the database",
                    epilog=f"script version {SCRIPT_VERSION}",
                    )

parser.add_argument("--recreate-db", required=False, action="store_true",
    help = "Recreate the DB structure using the ORM model in this script. THIS MAY NOT BE THE SAME MODEL AS CURRENTLY IN THE DATABASE")
parser.add_argument("--clear-db", required=False, action="store_true",
    help = "Clears all of tables of the database before inserting")
parser.add_argument("--clear-table", required=False, action="store_true",
    help = "Clear the target table (assumed from --content) before inserting")
content_arg = parser.add_argument("-c", "--content", required=False, choices=("sums","files","all"),
    help = "What to add to the database. Accepted values : sums|files|all")

args, unknown_args = parser.parse_known_args()

if args.recreate_db:
    data_manager.recreate_db(input("Warning: destructive operation. Please enter 'Yes I am sure': "))
    
if args.clear_db:
    print("Clearing database..")
    data_manager.clear_tables(content="all")
    print("Cleared database!")
if args.clear_table:
    if not args.content:
        print("Use -c/--content to specify a table")
        sys.exit(1)

    data_manager.clear_tables(content=args.content)

if len(unknown_args) == 0:
    exit(0)

parser.add_argument("-d", "--distro", required=False,
    help = "Set the distribution")
parser.add_argument("-v", "--version", required=False,
    help = "Set the distribution version")
parser.add_argument("-r", "--repo", required=False,
    help = "Set the distribution repository. Separate sub-repositories with '/'. Starting with '/' means using the default repository, if there is one")
content_arg.required = False

parser.add_argument("-a", "--add-multiple", required=False, action="store_true",
    help = "Confirm you will insert from multiple data sources (if you do not use all flags -d -v and -r)")
parser.add_argument("--file", required=False,
    help = "Manually set the file path instead of deriving it from other flags. The file content will be assumed from the other flags")

args = parser.parse_args()

if args.file:
    raise NotImplementedError("--file")
else:
    if not all((args.distro, args.version, args.repo, args.content)) and not args.add_multiple:
        print("not all flags (distro, version, repo and content) have been specified, large amount of data could be inserted. please use option -l/--large to confirm")
        exit(1)

    # TODO maybe deal with that better ?
    if args.repo and args.repo[0] == "/":
        args.repo = "DEFREPO"+args.repo

    print(f"Operation requested : add content '{args.content}' from repository '{args.distro}/{args.version}/{args.repo}'")

    data_manager.add_data(args.distro, args.version, args.repo, args.content)

    # data_manager.commit()
