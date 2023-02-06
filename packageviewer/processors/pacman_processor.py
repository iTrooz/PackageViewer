from packageviewer.parsers.pacman_parser import PacmanParser
from packageviewer.inserters.pacman_inserter import PacmanInserter
import timer

class PacmanProcessor:

    def __init__(self, distro_name, distro_version, dir_path, conn) -> None:
        self.distro_name = distro_name
        self.distro_version = distro_version
        self.parser = PacmanParser(distro_name, distro_version, dir_path)
        self.inserter = PacmanInserter(conn)

    # def _is_filedep_(self, depname):
    #     i = depname.find(".so")
    #     if i == -1:
    #         return False
    #     if len(depname) > i+3:
    #         return depname[i+3] in ('=', '<', '>')
    #     else:
    #         return True

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
                sum = j["sum"]
                sums_data.append(sum)
                self.inserter.table_tmp_file.add_rows(j["files"])
                for row in ({"parent_name": sum["name"], "dep_name": dep_name} for dep_name in j["deps"]):
                    self.inserter.table_tmp_dep.add_row(row)
                    # if not self._is_filedep_(row["dep_name"]):

        # TODO sort and filter by version

        self.inserter.table_tmp_package.add_rows(sums_data)