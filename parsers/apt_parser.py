import os
import gzip
from time import time
import sys

from sqlalchemy import insert
from tqdm import tqdm

import database

class AptParser:

    def __init__(self, db_session, distro_name, distro_version) -> None:
        self.db_session = db_session
        self.distro_name = distro_name
        self.distro_version = distro_version

    
    def __gen_package(self, distro_repo):
        return database.Package(
            distro_name=self.distro_name,
            distro_version=self.distro_version,
            distro_repo=distro_repo,
        )

    def __parse_sum_file_timer(func):
        def wrap_func(self, filepath, distro_repo):
            print(f'Starting parsing summary for {filepath}')
            start = time()
            result = func(self, filepath, distro_repo)
            end = time()
            print(f'Finished parsing summary for {filepath} ({(end-start):.4f}s)')
            return result
        return wrap_func

    @__parse_sum_file_timer
    def _parse_sum_file(self, filepath, distro_repo):
        def gen_row():
            return {
                "distro_name":self.distro_name,
                "distro_version":self.distro_version,
                "distro_repo":distro_repo,
                "others":{},
            }

        row = gen_row()

        file = gzip.open(filepath, "rb")
        for line in tqdm(file):
            line = line.decode().strip()

            if line == "":
                if "name" in row:
                    yield row

                row = gen_row()
                continue

            index = line.find(":")
            key = line[:index]
            value = line[index+2:]
            
            match key:
                case "Package":
                    row["name"] = value
                case "Architecture":
                    row["arch"] = value
                case "Version":
                    row["version"] = value
                case _:
                    row["others"][key] = value

        file.close()

    def __parse_files_file_timer(func):
        def wrap_func(self, filepath):
            print(f'Starting parsing files for {filepath}')
            start = time()
            result = func(self, filepath)
            end = time()
            print(f'Finished parsing files for {filepath} ({(end-start):.4f}s)')
            return result
        return wrap_func

    @__parse_files_file_timer
    def _parse_files_file(self, filepath):
        file = gzip.open(filepath, "rb")

        filepaths = []

        for line in tqdm(file):
            line = line.decode()

            # EOF
            if line == "":
                break

            split_line = line.split("\t")
            package_file, package_loc = split_line[0], split_line[-1].strip()
            package_name = package_loc.split("/")[-1]

            yield {"package_name": package_name, "filepath": package_file}

        file.close()

    def parse_all(self, dir):

        print(f"Starting to parse all summaries for {dir}")
        start = time()
        self.parse_sums(dir)
        end = time()
        print(f"Finished parsing all summaries for {dir} ({(end-start):.4f}s)")

        print(f"Starting to parse all files for {dir}")
        start = time()
        self.parse_files(dir)
        end = time()
        print(f"Finished parsing all files for {dir} ({(end-start):.4f}s)")

    def parse_sums(self, dir):
        # get summaries
        tasks = []

        for subdir in tqdm(list(os.listdir(dir))):
            distro_codename, repo = (*subdir.split("-"),"")[0:2]

            for subrepo in ("main", "universe", "restricted", "multiverse"):

                print("Inserting sums mapping..")
                database.bulk_insert_chunked(
                    self.db_session,
                    database.Package,
                    self._parse_sum_file(os.path.join(dir, subdir, subrepo, "Packages.gz"), repo+"-"+subrepo)
                )
                print("Inserted sums mapping!")



    def parse_files(self, dir):
        tasks = []

        for subdir in tqdm(list(os.listdir(dir))):
            
            distro_codename, repo = (*subdir.split("-"),"")[0:2]

            print(f"Inserting files for {subdir}..")
            database.bulk_insert_chunked(
                self.db_session,
                database.PackageFile,
                self._parse_files_file(os.path.join(dir, subdir, "Contents-amd64.gz"))
            )
