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

database.reset_db()
db_session = database.get_session()

async def fill_db():
    
    for dir in os.listdir("archives"):
        splitDir = dir.split("-")
        if len(splitDir) == 2:
            distro_name, distro_version = splitDir
            ParserClass = distros[distro_name]
            parserInst = ParserClass(db_session, distro_name, distro_version)
            
            parserInst.parse_sums("archives/"+dir)
        else:
            print(f"Found invalid directory : {dir}")

    db_session.commit()


async def test():
    parser = AptParser(db_session, "ubuntu", "22")
    await parser._parse_files_file("archives/ubuntu-22/jammy/Contents-amd64.gz")

asyncio.run(fill_db())
