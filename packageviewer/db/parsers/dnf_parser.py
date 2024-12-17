import gzip
import os
import sqlite3
import tempfile
import bz2
import lzma
import glob

from packageviewer.db import utils

class DnfParser:

    def __init__(self, distro_name: str, distro_version: str, dir_path: str) -> None:
        self.distro_name = distro_name
        self.distro_version = distro_version
        self.dir_path = dir_path


    def decompress(self, filepath):
        _, ext = os.path.splitext(filepath)
        tmp_file_obj = tempfile.NamedTemporaryFile(suffix=".sqlite")
        match ext:
            case ".xz":
                with lzma.open(filepath) as in_f, open(tmp_file_obj.name, "wb") as out_f:
                    out_f.write(in_f.read())
            case ".bz2":
                with bz2.open(filepath) as in_f, open(tmp_file_obj.name, "wb") as out_f:
                    out_f.write(in_f.read())
            case ".gz":
                with gzip.open(filepath) as in_f, open(tmp_file_obj.name, "wb") as out_f:
                    out_f.write(in_f.read())
            case _:
                raise ValueError(f"Invalid extension: {ext}")

        return tmp_file_obj

    def _parse_sum_file_(self, filepath, repo):
        tmp_file = self.decompress(filepath)
        conn = sqlite3.connect(tmp_file.name)

        cursor = conn.execute("SELECT pkgId, name, version, release, epoch FROM packages")

        for row in cursor:
            yield {"pkgId": row[0], "name": row[1], "version": row[2], "release": row[3], "epoch": row[4], "repo": repo}
        
        conn.close()


    def _parse_files_file_(self, filepath, repo):
        tmp_file = self.decompress(filepath)
        conn = sqlite3.connect(tmp_file.name)

        cursor = conn.execute('''
            SELECT pkgId, dirname, filenames, filetypes FROM filelist
            JOIN packages ON packages.pkgKey = filelist.pkgKey
        ''')

        for row in cursor:

            pkgId = row[0]
            dirname = row[1]
            if dirname[0] == "/":
                dirname = dirname[1:]
            else:
                raise ValueError(f"Directory should start with /, got {dirname}")
            
            for filename, filetype in zip(row[2].split("/"), row[3]):
                if filetype == 'f':
                    yield {"pkgId": pkgId, "dirname": dirname, "filename": filename, "repo": repo}
        
        conn.close()


    def _parse_deps_(self, filepath, repo):
        tmp_file = self.decompress(filepath)
        conn = sqlite3.connect(tmp_file.name)

        cursor = conn.execute('''
            SELECT packages.name, requires.name FROM requires
            JOIN packages ON packages.pkgKey = requires.pkgKey
        ''')
        for row in cursor.fetchall():
            yield {"parent_name": row[0], "dep_name": row[1]}

    
    def __join_file(self, full_repo, glob_):
        path = os.path.join(full_repo, glob_)
        glob_results = glob.glob(path)
        if len(glob_results) != 1:
            raise ValueError(f"Invalid glob results for {path}: {glob_results}")
        else:
            return glob_results[0]


    def parse_sums(self):
        for repo, full_repo in utils.loop_dirs(self.dir_path):
            path = self.__join_file(full_repo, "primary_db.sqlite*")
            yield self._parse_sum_file_(path, repo)

    def parse_files(self):
        for repo, full_repo in utils.loop_dirs(self.dir_path):
            path = self.__join_file(full_repo, "filelists_db.sqlite*")
            yield self._parse_files_file_(path, repo)

    def parse_deps(self):
        for repo, full_repo in utils.loop_dirs(self.dir_path):
            path = self.__join_file(full_repo, "primary_db.sqlite*")
            yield self._parse_deps_(path, repo)
