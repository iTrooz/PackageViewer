from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session

from time import time
import itertools
from tqdm import tqdm

Base = declarative_base()

class Package(Base):
    __tablename__ = 'package'

    id = Column(Integer, primary_key=True)

    distro_name = Column(String(255), nullable=False)
    distro_version = Column(String(255), nullable=False)
    distro_repo = Column(String(255), nullable=False)
    
    name = Column(String(255), nullable=False)
    version = Column(String(255), nullable=False)
    arch = Column(String(255), nullable=False)
    others = Column(JSON)

    def add_other(self, key, value):
        if not self.others:
            self.others = {}
        self.others[key] = value

    def __str__(self) -> str:
        return f"Package(distro={self.distro_name}-{self.distro_version}-{self.distro_repo},name={self.name})"

    def __repr__(self) -> str:
        return "Package(..)"

class PackageFile(Base):
    __tablename__ = 'package_file'

    id = Column(Integer, primary_key=True)
    filepath = Column(String(4096), nullable=False)
    package_name = Column(String(255))

db_engine = create_engine('mariadb+pymysql://root:azerty123@localhost:3306/packageviewer', echo=False, future=True)

def reset_db():
    Base.metadata.drop_all(bind=db_engine)
    Base.metadata.create_all(bind=db_engine)

def get_session():
    return Session(bind=db_engine)


def get_conn():
    return db_engine.connect().execution_options(autocommit=False)

def bulk_insert_chunked(db_session, table, inserts):
    print("Inserting..")
    start = time()
    __bulk_insert_chunked(db_session, table, inserts)
    end = time()
    print(f"Inserted ! ({(end-start):.4f}s)")

def __bulk_insert_chunked(db_session, table, inserts):
    CHUNK_SIZE=10**6
    while True:
        print("Inserting new chunk..")
        slice = list(itertools.islice(inserts, CHUNK_SIZE))
        if len(slice) == 0:
            break
        db_session.bulk_insert_mappings(table, slice)