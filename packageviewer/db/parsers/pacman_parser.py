import os
import tarfile
import tempfile
import itertools


from packageviewer.db import utils


class PacmanParser:

    def __init__(self, distro_name: str, distro_version: str, dir_path: str) -> None:
        self.distro_name = distro_name
        self.distro_version = distro_version
        self.dir_path = dir_path

    def _translate_sum_key_(self, key):
        if key in ("NAME", "VERSION"):
            return key.lower()
        else:
            return None

    def _parse_desc_file_(self, file, repo):
        sum = {"repo": repo}
        deps = []
        key = None
        value = None
        for line in itertools.chain(file, [""]):  # needed to flush last key/value pair
            line = line.strip()
            if len(line) == 0:
                if key == "DEPENDS":
                    deps = value if type(value) is list else (value,)
                else:
                    key = self._translate_sum_key_(key)
                    if key is not None:
                        sum[key] = value
                key = None
                value = None
            elif key is None:
                if line[0] == "%" and line[-1] == "%":
                    key = line[1:-1].upper()
                else:
                    raise ValueError(f"Expected %key%, got {line}")
            else:
                if value is None:
                    value = line
                elif type(value) is list:
                    value.append(line)
                else:
                    value = [value, line]
        return sum, deps

    def _parse_files_file_(self, file, package_name):
        key = None
        for line in file:
            line = line.strip()

            if len(line) == 0:
                key = None
            elif line[0] == "%" and line[-1] == "%":
                key = line[1:-1].upper()
            else:
                if key == "FILES":
                    dirname = os.path.dirname(line)
                    filename = os.path.basename(line)
                    if filename:
                        yield {
                            "package": package_name,
                            "dirname": dirname,
                            "filename": filename,
                        }

    def _parse_file_(self, filepath, repo):
        print("parse sums file " + filepath)

        tmpdir_obj = tempfile.TemporaryDirectory()
        tmpdir = tmpdir_obj.name

        tar = tarfile.open(filepath)
        tar.extractall(tmpdir)
        for dir, fulldir in utils.loop_dirs(tmpdir):

            with open(os.path.join(fulldir, "desc"), "r") as f:
                sum, deps = self._parse_desc_file_(f, repo)

            with open(os.path.join(fulldir, "files"), "r") as f:
                files = list(self._parse_files_file_(f, sum["name"]))

            yield {"sum": sum, "files": files, "deps": deps}

    def parse(self):
        for repo, fullrepo in utils.loop_dirs(self.dir_path):
            yield self._parse_file_(
                os.path.join(fullrepo, repo + ".files.tar.gz"), repo
            )
