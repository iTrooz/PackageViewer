from pydpkg import Dpkg

from packageviewer.parsers.apt_parser import AptParser
from packageviewer.inserters.apt_inserter import AptInserter

class AptProcessor:

    def __init__(self, distro_name, distro_version, dir_path, output_db_path) -> None:
        self.distro_name = distro_name
        self.distro_version = distro_version
        self.parser = AptParser(distro_name, distro_version, dir_path)
        self.inserter = AptInserter(output_db_path)


    def process(self):
        self.inserter.create_tables()

        self.inserter.insert_distro(self.distro_name, self.distro_version)

        sums_list = self.process_sums()
        repos_list = set(sum["distro_repo"] for sum in sums_list)
        
        self.inserter.insert_repos(repos_list)

        self.inserter.insert_packages(sums_list)
        
        files_list = self.process_files()

        dirnames_list = set(file["dirname"] for file in files_list)
        filenames_list = set(file["filename"] for file in files_list)
        
        self.inserter.insert_dirnames(dirnames_list)
        self.inserter.insert_filenames(filenames_list)
        self.inserter.insert_files(files_list)



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

        for sum in sums_data:
            sum["others"] = str(sum["others"])

        return sums_data

    def process_files(self):
        files_list = self.parser.parse_files()

        files_data = []
        for files_loop in files_list:
            files_data.extend(files_loop)

        return files_data