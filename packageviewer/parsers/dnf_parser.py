import os
import sqlite3

from packageviewer.distro_data import DistroData

class DnfParser:

    def __init__(self, distro_data: DistroData, archives_dir: str, output) -> None:
        self.distro_data = distro_data
        self.repo_path = os.path.join(archives_dir, distro_data.name, distro_data.version, distro_data.repo)
        self.output = output


    def _parse_summary_file_(self, filepath):
        conn = sqlite3.connect(filepath)

        cursor = conn.execute("SELECT pkgKey, dirname, filenames, filetypes FROM filelist")

        self.output.set_header(("pkgKey", "dirname", "filename"))

        for row in cursor:
            for filename, filetype in zip(row[2].split("/"), row[3]):
                if filetype == 'f':
                    self.output.add_row((row[0], row[1], filename))
        
        conn.close()


    def _parse_files_file_(self, filepath):
        conn = sqlite3.connect(filepath)

        cursor = conn.execute("SELECT pkgKey, dirname, filenames, filetypes FROM filelist")

        self.output.set_header(("pkgKey", "dirname", "filename"))

        for row in cursor:
            for filename, filetype in zip(row[2].split("/"), row[3]):
                if filetype == 'f':
                    self.output.add_row((row[0], row[1], filename))
        
        conn.close()


    def parse_sums(self):
        self._parse_sum_file_(os.path.join(self.repo_path, "primary.sqlite"))

    def parse_files(self):
        self._parse_files_file_(os.path.join(self.repo_path, "filelists.sqlite"))
