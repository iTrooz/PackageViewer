import os
import gzip
from time import time

from tqdm import tqdm

from packageviewer.distro_data import DistroData

def loop_dirs(base_dir):
    for subdir in os.listdir(base_dir):
        full_subdir = os.path.join(base_dir, subdir)
        if os.path.isdir(full_subdir):
            yield subdir, full_subdir
    

class AptParser:

    def __init__(self, distro_name: str, distro_version: str, dir_path: str) -> None:
        self.distro_name = distro_name
        self.distro_version = distro_version
        self.dir_path = dir_path
    
    def __parse_sum_file_timer__(func):
        def wrap_func(self, filepath, *args):
            print(f'Starting parsing summary for {filepath}')
            start = time()
            result = func(self, filepath, *args)
            end = time()
            print(f'Finished parsing summary for {filepath} ({(end-start):.4f}s)')
            return result
        return wrap_func

    @__parse_sum_file_timer__
    def _parse_sum_file_(self, filepath, repo, area):
        def gen_row():
            return {
                "distro_name":self.distro_name,
                "distro_version":self.distro_version,
                "distro_repo": repo,
                "others":{},
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
                case _:
                    row["others"][key] = value

        file.close()

    def _parse_files_filetimer__(func):
        def wrap_func(self, filepath, *args):
            print(f'Starting parsing files for {filepath}')
            start = time()
            result = func(self, filepath, *args)
            end = time()
            print(f'Finished parsing files for {filepath} ({(end-start):.4f}s)')
            return result
        return wrap_func

    @_parse_files_filetimer__
    def _parse_files_file_(self, filepath, repo):
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
        for repo, full_repo in loop_dirs(self.dir_path):
            for area, full_area in loop_dirs(full_repo):
                yield self._parse_sum_file_(os.path.join(full_area, "Packages.gz"), repo, area)

    def parse_files(self):
        for repo, full_repo in loop_dirs(self.dir_path):
            yield self._parse_files_file_(os.path.join(full_repo, "Contents-amd64.gz"), repo)