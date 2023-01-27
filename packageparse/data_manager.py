import os


from packageparse.processors.apt_processor import AptProcessor
from packageparse.processors.dnf_processor import DnfProcessor
from packageparse.processors.pacman_processor import PacmanProcessor

from packageparse.distro_data import DistroData
import packageparse.database


class DataManager:

    def __init__(self):
        pass
        
    def __get_processor_class__(self, distro_name):
        match distro_name:
            case "debian": return AptProcessor
            case "ubuntu": return AptProcessor
            case "fedora": return DnfProcessor
            case "archlinux": return PacmanProcessor
            case _: raise ValueError("Unknown distribution: "+distro_name)

    def process_data_point(self, distro_name, distro_version, dir_path, output_db_path):
        ProcessorClass = self.__get_processor_class__(distro_name)
        print(f"Using processor class '{ProcessorClass.__name__}' for {distro_name}:{distro_version}")
        
        parser = ProcessorClass(distro_name, distro_version, dir_path, output_db_path)
        parser.process()
