import os
import tarfile
import tempfile
import itertools

from tqdm import tqdm

from packageviewer import utils

class PacmanParser:
    
    def __init__(self, distro_name: str, distro_version: str, dir_path: str) -> None:
        self.distro_name = distro_name
        self.distro_version = distro_version
        self.dir_path = dir_path

    def _translate_key_(self, key):
        if key in ("NAME", "VERSION"):
            return key.lower()
        else:
            return None


    def _parse_desc_file_(self, file, repo):
        data = {"repo": repo}
        key = None
        value = None
        for line in itertools.chain(file, [""]): # needed to flush last key/value pair
            line = line.strip()
            if len(line) == 0:
                key = self._translate_key_(key)
                if key != None:
                    data[key] = value
                key = None
                value = None
            elif key == None:
                if line[0] == "%" and line[-1] == "%":
                    key = line[1:-1]
                else:
                    raise ValueError(f"Expected %key%, got {line}")
            else:
                if value == None:
                    value = line
                elif type(value) == list:
                    value.append(line)
                else:
                    value = [value, line]
        return data

    def _parse_files_file_(self, file, package_name):
        line = next(file).strip()
        if line != "%FILES%":
            raise ValueError(f"Expected %FILES%, got {line}")

        for line in file:
            filepath = line.strip()
            dirname = os.path.dirname(filepath)
            filename = os.path.basename(filepath)
            if filename:
                yield {"package": package_name, "dirname": dirname, "filename": filename}


    def _parse_file_(self, filepath, repo):
        print("parse sums file "+filepath)

        expected_file = 0
        
        tmpdir_obj = tempfile.TemporaryDirectory()
        tmpdir = tmpdir_obj.name

        tar = tarfile.open(filepath)
        tar.extractall(tmpdir)
        for dir, fulldir in utils.loop_dirs(tmpdir):

            with open(os.path.join(fulldir, "desc"), "r") as f:
                sum = self._parse_desc_file_(f, repo)
            
            with open(os.path.join(fulldir, "files"), "r") as f:
                files = list(self._parse_files_file_(f, sum["name"]))
                
            yield {"sum": sum, "files": files}

    def parse(self):
        for repo, fullrepo in utils.loop_dirs(self.dir_path):
            yield self._parse_file_(os.path.join(fullrepo, repo+".files.tar.gz"), repo)
