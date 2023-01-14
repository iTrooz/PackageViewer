import os
import gzip
from time import time

from tqdm import tqdm

from packageparse.distro_data import DistroData

class AptParser:

    def __init__(self, distro_data: DistroData, archives_dir: str, output) -> None:
        self.distro_data = distro_data
        self.repo_path = os.path.join(archives_dir, distro_data.name, distro_data.version, distro_data.repo)
        self.output = output

    
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
                "distro_name":self.distro_data.name,
                "distro_version":self.distro_data.version,
                "distro_repo":self.distro_data.repo+"/"+subrepo,
                "others":{},
            }

        row = gen_row()

        file = gzip.open(filepath, "rb")
        for line in file:
            line = line.decode().strip()

            if line == "":
                if "name" in row:
                    self.output.add_row(row)

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

            self.output.add_row({"package": package_name, "dirname": dirname, "filename": filename})

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
                self._parse_sum_file_(os.path.join(self.repo_path, subrepo, "Packages.gz"), subrepo)
                


    def parse_files(self):
        print(f"Inserting files for {self.repo_path}..")
        self._parse_files_file_(os.path.join(self.repo_path, "Contents-amd64.gz"))
        