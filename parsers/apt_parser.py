import os
import asyncio
import gzip
from time import time
from utils import timer

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

    def __read_sum_file_timer(func):
        async def wrap_func(self, filepath, distro_repo):
            print(f'Starting parsing summary for {filepath}')
            start = time()
            result = await func(self, filepath, distro_repo)
            end = time()
            print(f'Finished parsing summary for {filepath} ({(end-start):.4f}s)')
            return result
        return wrap_func

    @__read_sum_file_timer
    async def _read_sum_file(self, filepath, distro_repo):
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

    async def _read_contents_file(self, filepath, repo_packages):
        file = gzip.open(filepath, "rb")

        current_package = None

        for line in file.read().decode().split("\n"):
            # EOF
            if line == "":
                break

            split_line = line.replace("\t", "").split(" ")
            package_file, package_loc = split_line[0], split_line[-1]

            split_loc = package_loc.split("/")

            # optimisation : check if the package is the same as last time because searching it again
            if current_package == None or split_loc[-1] != current_package.name:
                # search new package
                current_package = None
                subrepo = None

                # # optimisation : sometimes the location tells us the subrepo
                # if len(split_loc)==3:
                #     subrepo = split_loc[0]
                
                for subrepo_i, subrepo_packages in repo_packages.items():
                    if subrepo == None or subrepo == subrepo_i:
                        for package in subrepo_packages:
                            if package.name == split_loc[-1]:
                                current_package = package
                                break
                    
                    if current_package != None:
                        break
                
                if current_package == None:
                    raise RuntimeError(f"Package {split_loc[-1]} found nowhere")

                
                
            package.files.append(database.PackageFile(filepath=package_file))

        file.close()

    async def parse_async(self, dir):

        print(f"Starting to parse all summaries for {dir}")
        start = time()
        visited_dirs, packages = await self.parse_sums(dir)
        end = time()
        print(f"Finished parsing all summaries for {dir} ({(end-start):.4f}s)")
        # await self.parse_files(dir, visited_dirs, packages)

        packages_flat = []
        for repo_packages in packages.values():
            for subrepo_packages in repo_packages.values():
                packages_flat = packages_flat + subrepo_packages # should already be resolved
        
        return packages_flat

    async def parse_sums(self, dir):
        # get summaries
        packages = {}
        tasks = {}
        visited_dirs = []

        for subdir in os.listdir(dir):
            distro_codename, repo = (*subdir.split("-"),"")[0:2]

            visited_dirs.append(subdir)

            for subrepo in ("main", "universe", "restricted", "multiverse"):

                task = self._read_sum_file(os.path.join(dir, subdir, subrepo, "Packages.gz"), repo+"-"+subrepo)
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
                self._read_contents_file(os.path.join(dir, subdir, "Contents-amd64.gz"), repo_packages)
            )

        await asyncio.gather(*tasks)
