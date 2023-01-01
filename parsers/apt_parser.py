import os
import asyncio
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
        async def wrap_func(self, filepath, distro_repo):
            print(f'Starting parsing summary for {filepath}')
            start = time()
            result = await func(self, filepath, distro_repo)
            end = time()
            print(f'Finished parsing summary for {filepath} ({(end-start):.4f}s)')
            return result
        return wrap_func

    @__parse_sum_file_timer
    async def _parse_sum_file(self, filepath, distro_repo):
        def gen_row():
            return {
                "distro_name":self.distro_name,
                "distro_version":self.distro_version,
                "distro_repo":distro_repo,
                "others":{},
            }

        row = gen_row()
        rows = []

        file = gzip.open(filepath, "rb")
        for line in tqdm(file):
            line = line.decode()

            if line == "":

                if "name" in row:
                    rows.append(row)

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

        return rows

    def __parse_files_file_timer(func):
        async def wrap_func(self, filepath):
            print(f'Starting parsing files for {filepath}')
            start = time()
            result = await func(self, filepath)
            end = time()
            print(f'Finished parsing files for {filepath} ({(end-start):.4f}s)')
            return result
        return wrap_func

    @__parse_files_file_timer
    async def _parse_files_file(self, filepath):
        file = gzip.open(filepath, "rb")

        filepaths = []

        for line in tqdm(file):
            line = line.decode()

            # EOF
            if line == "":
                break

            split_line = line.replace("\t", "").split(" ")
            package_file, package_loc = split_line[0], split_line[-1]
            package_name = package_loc.split("/")[-1]

            filepaths.append({"package_name": package_name, "filepath": package_file})

        file.close()

        return filepaths

    async def parse_async(self, dir):

        # input()
        # 
        # print(f"Starting to parse all summaries for {dir}")
        # start = time()
        # await self.parse_sums(dir)
        # end = time()
        # print(f"Finished parsing all summaries for {dir} ({(end-start):.4f}s)")

        input()
        
        print(f"Starting to parse all summaries for {dir}")
        start = time()
        await self.parse_files(dir)
        end = time()
        print(f"Finished parsing all summaries for {dir} ({(end-start):.4f}s)")

        input()
        
    async def parse_sums(self, dir):
        # get summaries
        tasks = []

        for subdir in os.listdir(dir):
            distro_codename, repo = (*subdir.split("-"),"")[0:2]

            for subrepo in ("main", "universe", "restricted", "multiverse"):

                tasks.append(self._parse_sum_file(os.path.join(dir, subdir, subrepo, "Packages.gz"), repo+"-"+subrepo))


        print("Inserting sums mapping..")
        for rows in tqdm(await asyncio.gather(*tasks)):
            self.db_session.bulk_insert_mappings(database.Package, rows)
        print("Inserted sums mapping!")

    async def parse_files(self, dir):
        tasks = []

        for subdir in os.listdir(dir):
            
            distro_codename, repo = (*subdir.split("-"),"")[0:2]

            tasks.append(
                self._parse_files_file(os.path.join(dir, subdir, "Contents-amd64.gz"))
            )

        print("Inserting files mapping..")
        for filepaths in tqdm(await asyncio.gather(*tasks)):
            self.db_session.bulk_insert_mappings(database.PackageFile, filepaths)
        print("Inserted files mapping!")