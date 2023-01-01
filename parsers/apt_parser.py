import os
import asyncio
import gzip
from time import time
import sys

from tqdm import tqdm

import database

class AptParser:

    def __init__(self, db, distro_name, distro_version) -> None:
        self.db = db
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
        package = self.__gen_package(distro_repo)
        packages = []

        file = gzip.open(filepath, "rb")
        for line in file.read().decode().split("\n"):
            if line == "":
                if package.name:
                    packages.append(package)
                package = self.__gen_package(distro_repo)
                continue

            index = line.find(":")
            key = line[:index]
            value = line[index+2:]
            
            match key:
                case "Package":
                    package.name = value
                case "Architecture":
                    package.arch = value
                case "Version":
                    package.version = value
                case _:
                    package.add_other(key, value)
        return packages

    def __parse_files_file_timer(func):
        async def wrap_func(self, filepath, repo_packages):
            print(f'Starting parsing files for {filepath}')
            start = time()
            result = await func(self, filepath, repo_packages)
            end = time()
            print(f'Finished parsing files for {filepath} ({(end-start):.4f}s)')
            return result
        return wrap_func

    @__parse_files_file_timer
    async def _parse_files_file(self, filepath, repo_packages):
        file = gzip.open(filepath, "rb")

        packagefiles = []

        for line in tqdm(file.read().decode().split("\n")):
            # EOF
            if line == "":
                break

            split_line = line.replace("\t", "").split(" ")
            package_file, package_loc = split_line[0], split_line[-1]
            package_name = package_loc.split("/")[-1]

            # a = database.PackageFile(package_name=package_name, filepath=package_file)
            # packagefiles.append()

        file.close()

        return packagefiles

    async def parse_async(self, dir):

        print(f"Starting to parse all summaries for {dir}")
        start = time()
        visited_dirs, packages = await self.parse_sums(dir)
        end = time()
        print(f"Finished parsing all summaries for {dir} ({(end-start):.4f}s)")
        

        print(f"Starting to parse all summaries for {dir}")
        start = time()
        package_files = await self.parse_files(dir, visited_dirs, packages)
        end = time()
        print(f"Finished parsing all summaries for {dir} ({(end-start):.4f}s)")
        

        packages_flat = []
        for repo_packages in packages.values():
            for subrepo_packages in repo_packages.values():
                packages_flat = packages_flat + subrepo_packages
        
        packages_files_flat = []
        for sub_package_files in package_files:
            packages_files_flat = packages_files_flat + sub_package_files
        
        return packages_flat, packages_files_flat

    async def parse_sums(self, dir):
        # get summaries
        packages = {}
        tasks = {}
        visited_dirs = []

        for subdir in os.listdir(dir):
            distro_codename, repo = (*subdir.split("-"),"")[0:2]

            visited_dirs.append(subdir)

            for subrepo in ("main", "universe", "restricted", "multiverse"):

                task = self._parse_sum_file(os.path.join(dir, subdir, subrepo, "Packages.gz"), repo+"-"+subrepo)
                tasks[(repo, subrepo)] = task


        # await all
        for key, value in zip(tasks.keys(), await asyncio.gather(*tasks.values())):
            if not key[0] in packages:
                packages[key[0]] = {}
            packages[key[0]][key[1]] = value


        return visited_dirs, packages

    async def parse_files(self, dir, visited_dirs, packages):
        tasks = []

        for subdir, repo_packages in zip(visited_dirs, packages.values()):
            
            distro_codename, repo = (*subdir.split("-"),"")[0:2]

            tasks.append(
                self._parse_files_file(os.path.join(dir, subdir, "Contents-amd64.gz"), repo_packages)
            )

        return await asyncio.gather(*tasks)
