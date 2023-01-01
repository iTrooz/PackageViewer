import os
import asyncio

from parsers.apt_parser import AptParser
from parsers.dnf_parser import DnfParser
from parsers.pacman_parser import PacmanParser
import database

distros = {
    "debian": AptParser,
    "ubuntu": AptParser,
    "fedora": DnfParser,
    "archlinux": PacmanParser,
}

db_engine, session = database.reset_db()

async def fill_db():
    tasks = []
    for dir in os.listdir("archives"):
        splitDir = dir.split("-")
        if len(splitDir) == 2:
            distro_name, distro_version = splitDir
            ParserClass = distros[distro_name]
            parserInst = ParserClass(db_engine, distro_name, distro_version)
            
            tasks.append(parserInst.parse_async("archives/"+dir))
        else:
            print(f"Found invalid directory : {dir}")

    all_results = await asyncio.gather(*tasks)
    
    for distro_packages, distro_package_files in all_results:
        session.add_all(distro_packages)
        session.add_all(distro_package_files)
    
    session.commit()

asyncio.run(fill_db())
