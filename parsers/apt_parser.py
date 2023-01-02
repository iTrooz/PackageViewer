import os
import gzip
from time import time
import sys

from sqlalchemy import insert
from tqdm import tqdm

import database

class AptParser:

    def __init__(self, db_session, distro_name, distro_version, distro_repo, archives_dir) -> None:
        self.db_session = db_session
        self.distro_name = distro_name
        self.distro_version = distro_version
        self.distro_repo = distro_repo
        self.repo_path = os.path.join(archives_dir, distro_name, distro_version, distro_repo)

    
    def __parse_sum_file_timer__(func):
        def wrap_func(self, filepath, subrepo):
            print(f'Starting parsing summary for {filepath}')
            start = time()
            result = func(self, filepath, subrepo)
            end = time()
            print(f'Finished parsing summary for {filepath} ({(end-start):.4f}s)')
            return result
        return wrap_func

    @__parse_sum_file_timer__
    def _parse_sum_file_(self, filepath, subrepo):
        def gen_row():
            return {
                "distro_name":self.distro_name,
                "distro_version":self.distro_version,
                "distro_repo":self.distro_repo+"/"+subrepo,
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

    def _parse_files_filetimer__(func):
        def wrap_func(self, filepath):
            print(f'Starting parsing files for {filepath}')
            start = time()
            result = func(self, filepath)
            end = time()
            print(f'Finished parsing files for {filepath} ({(end-start):.4f}s)')
            return result
        return wrap_func

    @_parse_files_filetimer__
    def _parse_files_file_(self, filepath):
        file = gzip.open(filepath, "rb")

        filepaths = []

        for line in tqdm(file):
            line = line.decode()

            # EOF
            if line == "":
                break

            split_line = line.split("\t")
            package_file, package_loc = split_line[0].strip(), split_line[-1].strip()
            package_name = package_loc.split("/")[-1]

            yield {"package": package_name, "filepath": package_file, "filename": package_file.split("/")[-1]}

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

    def parse_sums(self):

        for subrepo in os.listdir(self.repo_path):
            if os.path.isdir(os.path.join(self.repo_path, subrepo)):
                print(f"Inserting sums mapping for {subrepo}..")
                database.bulk_insert_chunked(
                    self.db_session,
                    database.Package,
                    self._parse_sum_file_(os.path.join(self.repo_path, subrepo, "Packages.gz"), subrepo)
                )
                print(f"Inserted sums mapping for {subrepo}!")



    def parse_files(self):
        print(f"Inserting files for {self.repo_path}..")
        database.bulk_insert_chunked(
            self.db_session,
            database.PackageFile,
            self._parse_files_file_(os.path.join(self.repo_path, "Contents-amd64.gz"))
        )
