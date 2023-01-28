
from packageviewer.parsers.dnf_parser import DnfParser
from packageviewer.inserters.dnf_inserter import DnfInserter

import rpm_vercmp
class DnfProcessor:

    def __init__(self, distro_name, distro_version, dir_path, output_db_path) -> None:
        self.parser = DnfParser(distro_name, distro_version, dir_path)
        self.inserter = DnfInserter(output_db_path)


    def process_sums(self):
        sums_list = self.parser.parse_sums()

        sums_data = []
        for sums_loop in sums_list:
            sums_data.extend(sums_loop)

        sums_data = sorted(sums_data, key=lambda d: d["name"])

        # dedup packages by version
        dedup_sums_data = []
        current_sum = {"name": None}
        sums_data.append({"name": None})
        for loop_sum in sums_data:
            if loop_sum["name"] == current_sum["name"]:
                if rpm_vercmp.vercmp(loop_sum["version"], current_sum["version"]) == 1:
                    current_sum = loop_sum
            else:
                if current_sum["name"] != None:
                    del current_sum["release"]
                    del current_sum["epoch"]
                    
                    dedup_sums_data.append(current_sum)

                current_sum = loop_sum
        sums_data = dedup_sums_data

        self.inserter.table_tmp_package.add_rows(sums_data)

    def process(self):
        self.process_sums()
        # self.process_files()
        # self.inserter.normalize()

