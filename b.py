import database
from tqdm import tqdm

class Obj:

    def __init__(self, package_name, filepath) -> None:
        self.package_name = package_name
        self.filepath = filepath

for i in tqdm(range(10**7)):
    # database.PackageFile(package_name="a", filepath="b")
    Obj(package_name="a", filepath="b")