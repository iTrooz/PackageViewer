import os
import gzip
from time import time

from tqdm import tqdm

from packageviewer import utils

class AptParser:

    def __init__(self, distro_name: str, distro_version: str, dir_path: str) -> None:
        self.distro_name = distro_name
        self.distro_version = distro_version
        self.dir_path = dir_path
    
    def _parse_sum_file_(self, filepath, repo, area):
        def gen_row():
            return {
                "distro_name":self.distro_name,
                "distro_version":self.distro_version,
                "repo": repo,
            }

        row = gen_row()

        file = gzip.open(filepath, "rb")
        for line in file:
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
                case "Depends":
                    row["depends"] = value

        file.close()

    def _parse_files_file_(self, filepath, repo):
        if not os.path.exists(filepath):
            print(f"Warning: file {filepath} Doesn't exist. Skipping")
            return
        
        file = gzip.open(filepath, "rb")

        for line in tqdm(file):
            line = line.decode()

            # EOF
            if line == "":
                break

            split_line = line.split("\t")
            filepath, package_loc = split_line[0].strip(), split_line[-1].strip()

            package_name = package_loc.split("/")[-1]
            filepath_split = filepath.split(" ")
            dirname, filename = os.path.dirname(filepath), os.path.basename(filepath_split[-1])

            yield {"repo": repo, "package": package_name, "dirname": dirname, "filename": filename}

        file.close()

    def parse_sums(self):
        for repo, full_repo in utils.loop_dirs(self.dir_path):
            for area, full_area in utils.loop_dirs(full_repo):
                yield self._parse_sum_file_(os.path.join(full_area, "Packages.gz"), repo, area)

    def parse_files(self):
        for repo, full_repo in utils.loop_dirs(self.dir_path):
            yield self._parse_files_file_(os.path.join(full_repo, "Contents-amd64.gz"), repo)
