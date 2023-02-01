from packageviewer.parsers.pacman_parser import PacmanParser
from packageviewer.inserters.pacman_inserter import PacmanInserter
import timer

class PacmanProcessor:

    def __init__(self, distro_name, distro_version, dir_path, conn) -> None:
        self.distro_name = distro_name
        self.distro_version = distro_version
        self.parser = PacmanParser(distro_name, distro_version, dir_path)
        self.inserter = PacmanInserter(conn)

    @timer.dec
    def process(self):
        self.process_parser()

        self.inserter.normalize(self.distro_name, self.distro_version)

    @timer.dec
    def process_parser(self):
        all_list = self.parser.parse()
        sums_data = []

        for i in all_list:
            for j in i:
                sums_data.append(j["sum"])
                self.inserter.table_tmp_file.add_rows(j["files"])

        self.inserter.table_tmp_package.add_rows(sums_data)