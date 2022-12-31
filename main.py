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
        distro_name, distro_version = dir.split("-")
        ParserClass = distros[distro_name]
        parserInst = ParserClass(db_engine, distro_name, distro_version)
        
        tasks.append(parserInst.parse_async("archives/"+dir))

    all_packages = await asyncio.gather(*tasks)

    print(all_packages[0][0])
    print(all_packages[0][1])

    # for distro_packages in all_packages:
    #     session.add_all(distro_packages)
    
    session.commit()

asyncio.run(fill_db())
