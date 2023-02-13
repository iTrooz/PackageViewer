from pydpkg import Dpkg

from packageviewer.db.parsers.apt_parser import AptParser
from packageviewer.db.inserters.apt_inserter import AptInserter
import timer

def __first_group__(data, sep):
    index = data.find(sep)
    return data if index == -1 else data[:index]

class AptProcessor:

    def __init__(self, distro_name, distro_version, dir_path, conn) -> None:
        self.distro_name = distro_name
        self.distro_version = distro_version
        self.parser = AptParser(distro_name, distro_version, dir_path)
        self.inserter = AptInserter(conn)


    @timer.dec
    def process(self):
        self.process_sums()
        self.process_files()
        self.inserter.normalize(self.distro_name, self.distro_version)

    def gen_deps_rows(self, pkg_name, deps):
        separated_deps = set()
        for dep_group in deps.split(","):
            dep_name = __first_group__(dep_group, '|')
            dep_name = __first_group__(dep_name, '(')

            separated_deps.add(dep_name.strip()) # set so auto-dedup
        
        for dep_name in separated_deps:
            yield {"parent_name": pkg_name, "dep_name": dep_name}

    @timer.dec
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
                if Dpkg.compare_versions(loop_sum["version"], current_sum["version"]) == 1:
                    current_sum = loop_sum
            else:
                if current_sum["name"] != None:
                    dedup_sums_data.append(current_sum)
                current_sum = loop_sum
        sums_data = dedup_sums_data

        for row in sums_data:
            deps = row.get("depends")
            if deps:
                deps_rows = list(self.gen_deps_rows(row["name"], deps))
                self.inserter.table_tmp_dep.add_rows(deps_rows)
                del row["depends"]
                
            self.inserter.table_tmp_package.add_row(row)



    @timer.dec
    def process_files(self):
        files_list = self.parser.parse_files()

        for files_loop in files_list:
            self.inserter.table_tmp_file.add_rows(files_loop)
