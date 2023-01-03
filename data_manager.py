import os

import database

from parsers.apt_parser import AptParser
from parsers.dnf_parser import DnfParser
from parsers.pacman_parser import PacmanParser


class DataManager:

    def __init__(self):
        self.ARCHIVES_DIR = "archives"
        self.db_session = database.get_session()

    def __get_parser_class__(self, distro_name):
        match distro_name:
            case "debian": return AptParser
            case "ubuntu": return AptParser
            case "fedora": return DnfParser
            case "archlinux": return PacmanParser
            case _: raise ValueError("Unknown distribution: "+distro_name)
    
    def recreate_db(self, magic_value):
        if magic_value == "Yes I am sure":
            print("Re-creating the database from the ORM model..")
            database.recreate_db()
            print("Re-recrated the database !")
        else:
            print("Invalid magic value, not re-creating database")
            exit(1)

    def clear_tables(self, content):
        tables = []
        match content:
            case "sums":
                tables = (database.Package,)
            case "files":
                tables = (database.PackageFile,)
            case "all":
                tables = (database.Package, database.PackageFile)
            case _:
                raise ValueError(f"SHOULD NOT HAPPEN: invalid content '{content}'")

        for table in tables:
            print(f"Clearing table '{table.__tablename__}'..")
            self.db_session.execute(f"DELETE FROM {table.__tablename__}")
            print(f"Cleared table '{table.__tablename__}'!")

            # do not use TRUNCATE or ALTER TABLE AUTO_INCREMENT because they are DDL operations that will auto-commit
            # we can't either reset the increment at commit time because we will already have the new ids

    def add_data(self, distro_name, distro_version, distro_repo, content):

        def value_or_loop_dir(value, *dir):
            if value:
                yield value
            else:
                path = os.path.join(self.ARCHIVES_DIR, *dir)
                for subdir in os.listdir(path):
                    if os.path.isdir(os.path.join(path, subdir)):
                        yield subdir
                
        for distro_name_loop in value_or_loop_dir(distro_name):
            for distro_version_loop in value_or_loop_dir(distro_version, distro_name_loop):
                for distro_repo_loop in value_or_loop_dir(distro_repo, distro_name_loop, distro_version_loop):
                    self.add_data_from_repo(distro_name_loop, distro_version_loop, distro_repo_loop, content or "all")


    def add_data_from_repo(self, distro_name, distro_version, distro_repo, content):
        ParserClass = self.__get_parser_class__(distro_name)
        print(f"Using parser class '{ParserClass.__name__}'")
        
        parser = ParserClass(self.db_session, distro_name, distro_version, distro_repo, self.ARCHIVES_DIR)
        
        match content:
            case "sums":
                parser.parse_sums()
            case "files":
                parser.parse_files()
            case "all":
                parser.parse_sums()
                parser.parse_files()
            case _:
                raise ValueError(f"SHOULD NOT HAPPEN: invalid content '{content}'")
        
    def commit(self):
        print("Committing transaction..")
        
        self.db_session.commit()
        
        print("Committed transaction !")